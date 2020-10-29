import argparse
import base64
import collections
import json
import logging
import os
import os.path
import struct
import typing
from datetime import datetime
import crcmod.predefined

# import google.cloud.exceptions
from google.cloud import bigquery, storage

# from googleapiclient import discovery
# from oauth2client.client import GoogleCredentials

SCHEMA_EXT = "json"
DATA_EXT = "jsonl"

# nolonger needed, we only need ONE project
# PROJECTS = os.path.join(os.getcwd(), "projects")
# get this from argparse
PROJECT = "table-loader-dev"

logger = logging.getLogger()
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO
)


class PreferredState(typing.TypedDict, total=False):
    schema: object
    schema_crc32c: str
    data_crc32c: str
    num_rows: int


class LastKnownAppliedState(typing.TypedDict, total=False):
    schema: object
    schema_crc32c: str
    data_crc32c: str
    modified: datetime


class CurrentState(typing.TypedDict, total=False):
    schema: object
    schema_crc32c: str
    num_rows: int
    modified: datetime


def make_crc32c(data: any):
    """
    This is a bit fiddly.
    crcmod: https://cloud.google.com/storage/docs/gsutil/addlhelp
    /CRC32CandInstallingcrcmod
    The Base64 encoded CRC32c is in big-endian byte order
    https://vsoch.github.io/2020/crc32c-validation-google-storage/
    :param data: schema or data
    :return:
    """
    crc32c = crcmod.predefined.Crc("crc-32c")
    crc32c.update(str(data).encode("utf-8"))
    return base64.b64encode(struct.pack(">I", crc32c.crcValue)).decode("utf-8")


def json_from_bigquery_schema(schema_fields: bigquery.schema.SchemaField):
    """
    :param schema_fields:
    :return:
    """
    json_output: typing.List[dict] = []
    for field in schema_fields:
        json_output.append(
            dict(sorted(field.to_api_repr().items(), key=lambda k: k[0]))
        )
    return json.dumps(json_output)


def get_preferred_state():
    """
    :return:
    """
    preferred_state: typing.Dict[str, PreferredState] = collections.defaultdict(
        lambda: PreferredState()
    )

    for root, dirs, files in os.walk(os.path.join(os.getcwd(), "projects", PROJECT)):
        if not files:
            continue

        dataset = os.path.basename(root)

        file: str
        for file in files:
            file_path = os.path.join(root, file)
            table_name = file.partition(".")[0]
            state_key = ".".join([PROJECT, dataset, table_name])
            state_value = preferred_state[state_key]

            with open(file_path, "r") as f:
                f_content = f.read()
                if file.endswith(SCHEMA_EXT):
                    state_value["schema"] = json.loads(f_content)
                    state_value["schema_crc32c"] = make_crc32c(state_value["schema"])
                elif file.endswith(DATA_EXT):
                    state_value["data_crc32c"] = make_crc32c(f_content)
                    state_value["num_rows"] = sum(
                        1 for line in open(file_path).read().splitlines()
                    )
                else:
                    logger.warning(f"File extension of {file} is not supported.")

        return preferred_state


def get_last_known_applied_state(bucket_prefix: str):
    """
    :param bucket_prefix: gs://bucket_name
    :return:
    """

    last_known_applied_state: typing.Dict[
        str, LastKnownAppliedState
    ] = collections.defaultdict(lambda: LastKnownAppliedState())

    storage_client = storage.Client()

    bucket: str = bucket_prefix.partition("//")[-1]
    for blob in storage_client.list_blobs(bucket_or_name=bucket):

        blob_parts = blob.name.split("/")
        if not blob_parts[1] or not blob_parts[2]:
            continue

        project_dir = blob_parts[0]
        dataset_dir = blob_parts[1]
        table_name = blob_parts[2].partition(".")[0]
        state_key = ".".join([project_dir, dataset_dir, table_name])
        state_value = last_known_applied_state[state_key]

        modified = str(blob.updated)
        if state_value.get("modified", "1970-01-01 00:00:00.000000+00:00") < modified:
            state_value["modified"] = modified

        if blob.name.endswith(SCHEMA_EXT):
            # do we need the schema if we have the crc?
            state_value["schema"] = json.loads(blob.download_as_string())
            state_value["schema_crc32c"] = make_crc32c(state_value["schema"])

            # Do we need some date conversion magic here?
            # maybe convert everything to UTC based on the location of the data?
            # or just simply leave it for the user to make their own decision

        if blob.name.endswith(DATA_EXT):
            state_value["data_crc32c"] = make_crc32c(
                "".join(map(chr, blob.download_as_string()))
            )

    return last_known_applied_state


# def get_current_state(known_tables: typing.List[str]):
def get_current_state():
    """
    :return:
    """
    current_state: typing.Dict[str, CurrentState] = collections.defaultdict(
        lambda: CurrentState()
    )

    bigquery_client = bigquery.Client(project=PROJECT)
    datasets = list(bigquery_client.list_datasets(project=PROJECT))

    if datasets:
        for dataset in datasets:
            tables = list(bigquery_client.list_tables(dataset=dataset.dataset_id))
            if tables:
                for table in tables:
                    state_key = ".".join([PROJECT, dataset.dataset_id, table.table_id])
                    state_value = current_state[state_key]
                    table_info = bigquery_client.get_table(state_key)
                    state_value["schema"] = json.loads(
                        json_from_bigquery_schema(table_info.schema)
                    )
                    state_value["schema_crc32c"] = make_crc32c(
                        json.loads(json_from_bigquery_schema(table_info.schema))
                    )
                    state_value["num_rows"] = table_info.num_rows
                    state_value["modified"] = str(table_info.modified)

    return current_state


def is_identical(
    preferred_state: PreferredState,
    last_known_applied_state: LastKnownAppliedState,
    current_state: CurrentState,
):
    """
    Checks if all states are identical
    :param preferred_state:
    :param last_known_applied_state:
    :param current_state:
    :return:
    """
    keys = distinct_keys(preferred_state, last_known_applied_state, current_state)

    for key in keys:
        logger.info(f"checking {key}")
        p_value = preferred_state.get(key, None)
        k_value = last_known_applied_state.get(key, None)
        c_value = current_state.get(key, None)
        if key in ["schema", "schema_crc32c"]:
            if not p_value == k_value == c_value:
                logger.info(
                    f"{key}\npreferred: {p_value}\n last known: {k_value}\ncurrent: {c_value}"
                )
                return False
        elif key == ["data_crc32c", "schema_crc32c"]:
            if not p_value == k_value:
                logger.info(f"{key}\npreferred: {p_value}\nlast known: {k_value}")
                return False
        elif key == ["num_rows", "schema"]:
            if not p_value == c_value:
                logger.info(f"{key}\npreferred: {p_value}\ncurrent: {c_value}")
                return False
        elif key == "modified":
            if not (k_value < c_value):
                logger.info(f"{key}\nlast known: {k_value}\ncurrent: {c_value}")
                return False

    return True


def decide_table_action(
    preferred_state: PreferredState,
    last_known_applied_state: LastKnownAppliedState,
    current_state: CurrentState,
):
    """

    :param preferred_state:
    :param last_known_applied_state:
    :param current_state:
    :return:
    """
    table_action: str = ""
    if preferred_state and last_known_applied_state and current_state:
        if is_identical(preferred_state, last_known_applied_state, current_state):
            table_action = "do nothing"
        else:
            table_action = "upload"
    elif not preferred_state and last_known_applied_state:
        table_action = "delete"
    elif preferred_state and (not last_known_applied_state or not current_state):
        table_action = "upload"
    else:
        table_action = "don't know what to do"

    return table_action


def decide_actions(
    preferred_state: typing.Dict[str, PreferredState],
    last_known_applied_state: typing.Dict[str, LastKnownAppliedState],
    current_state: typing.Dict[str, CurrentState],
) -> typing.List[typing.Callable]:
    """

    :param preferred_state:
    :param last_known_applied_state:
    :param current_state:
    :return:
    """
    actions = dict()
    for table in distinct_keys(
        preferred_state, last_known_applied_state, current_state
    ):
        logger.info(f"Checking {table}")
        action = decide_table_action(
            preferred_state.get(table, PreferredState()),
            last_known_applied_state.get(table, LastKnownAppliedState()),
            current_state.get(table, CurrentState()),
        )
        actions[table] = action
    return actions


def validate_state(table: typing.AnyStr, state: typing.Dict):
    """

    :param table:
    :param state:
    :return:
    """
    value = state.get(table, {})
    if not value:
        return {}
    return value


def distinct_keys(
    preferred_state,
    last_known_applied_state,
    current_state,
) -> typing.Iterable[str]:
    """

    :param preferred_state:
    :param last_known_applied_state:
    :param current_state:
    :return:
    """
    keys = set()
    for d in (preferred_state, last_known_applied_state, current_state):
        for k, _ in d.items():
            keys.add(k)
    return keys


# this must go to the cli.py
parser = argparse.ArgumentParser()
parser.add_argument(
    "--bucket-prefix",
    help="Something like: table_loader --bucket-prefix 'gs://bucket_name'",
)
args = parser.parse_args()


def main(bucket_prefix=args.bucket_prefix):

    print("\n-------------------------------------- preferred_state\n")
    preferred_state = get_preferred_state()
    logger.info(preferred_state)

    print("\n-------------------------------------- last_known_applied_state\n")
    last_known_applied_state = get_last_known_applied_state(bucket_prefix)
    logger.info(last_known_applied_state)

    print("\n-------------------------------------- current_state\n")
    current_state = get_current_state()
    logger.info(current_state)

    print("\n-------------------------------------- actions\n")
    actions = decide_actions(preferred_state, last_known_applied_state, current_state)
    logger.info(json.dumps(actions, indent=2))

    # this goes in the cli code
    # for action in actions:
    #     action()