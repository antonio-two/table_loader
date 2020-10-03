import argparse
import base64
import collections
import json
import logging
import os
import os.path
import struct
import typing

import crcmod.predefined

# import google.cloud.exceptions
from google.cloud import bigquery, storage
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials

SCHEMA_EXT = "json"
DATA_EXT = "jsonl"
INFO_EXT = "info"
PROJECTS = os.path.join(os.getcwd(), "projects")

logger = logging.getLogger()
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO
)


class StateValue(typing.TypedDict, total=False):
    """
    schema is local, remote and current
    schema_crc32c is local, remote and current
    data_crc32c is local and remote
    info_crc32c is local, remote and current
    """

    schema: object
    schema_crc32c: str
    data_crc32c: str
    info_crc32c: str


class InfoStateValue(typing.TypedDict, total=False):
    # TODO: think about CI. which of these can be created locally.
    num_bytes: int
    num_rows: int
    created: str
    modified: str
    table_type: str


def make_crc32c(data: str):
    """
    This is a bit fiddly.
    crcmod: https://cloud.google.com/storage/docs/gsutil/addlhelp/CRC32CandInstallingcrcmod
    The Base64 encoded CRC32c is in big-endian byte order
    https://vsoch.github.io/2020/crc32c-validation-google-storage/
    :param data: schema or data
    :return:
    """
    crc32c = crcmod.predefined.Crc("crc-32c")
    crc32c.update(data.encode("utf-8"))
    return base64.b64encode(struct.pack(">I", crc32c.crcValue)).decode("utf-8")


def get_preferred_state():
    """
    A checksum from each file is computed on the fly for both local schema and data files
    :return:
    """

    preferred_state: typing.Dict[str, StateValue] = collections.defaultdict(
        lambda: StateValue()
    )

    for root, dirs, files in os.walk(PROJECTS):
        if not files:
            continue

        project = os.path.basename(os.path.dirname(root))
        dataset = os.path.basename(root)

        file: str
        for file in files:

            file_path = os.path.join(root, file)
            table_name = file.partition(".")[0]
            state_key = ".".join([project, dataset, table_name])
            state_value = preferred_state[state_key]

            with open(file_path, "r") as f:
                f_content = f.read()
                if file.endswith(SCHEMA_EXT):
                    state_value["schema"] = json.loads(f_content)
                    state_value["schema_crc32c"] = make_crc32c(f_content)
                elif file.endswith(DATA_EXT):
                    state_value["data_crc32c"] = make_crc32c(f_content)
                #     TODO: may not be a viable option for local
                elif file.endswith(INFO_EXT):
                    state_value["info_crc32c"] = make_crc32c(f_content)
                else:
                    logger.warning(f"File extension of {file} is not supported.")

        return preferred_state


def get_last_known_applied_state(bucket_prefix: str):
    """
    A checksum for both schema and data blobs can be retrieved by the storage api
    :param bucket_prefix: gs://bucket_name
    :return:
    """

    last_known_applied_state: typing.Dict[str, StateValue] = collections.defaultdict(
        lambda: StateValue()
    )

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

        if blob.name.endswith(SCHEMA_EXT):
            state_value["schema_crc32c"] = blob.crc32c

        if blob.name.endswith(DATA_EXT):
            state_value["data_crc32c"] = blob.crc32c

    return last_known_applied_state


def get_current_state():

    current_state: typing.Dict[str, StateValue] = collections.defaultdict(
        lambda: StateValue()
    )

    credentials = GoogleCredentials.get_application_default()
    service = discovery.build("cloudresourcemanager", "v1", credentials=credentials)

    request = service.projects().list()
    response = request.execute()

    for project in response.get("projects", []):

        if project["lifecycleState"] == "ACTIVE":

            project_id = project["projectId"]
            bigquery_client = bigquery.Client(project=project_id)
            datasets = list(bigquery_client.list_datasets())

            if datasets:
                for dataset in datasets:
                    tables = list(
                        bigquery_client.list_tables(dataset=dataset.dataset_id)
                    )
                    if tables:
                        for table in tables:
                            state_key = ".".join(
                                [project_id, dataset.dataset_id, table.table_id]
                            )
                            state_value = current_state[state_key]
                            # TODO: figure out how to either get json from bigquery or convert the below to json
                            state_value["schema"] = None
                            state_value["schema_crc32c"] = make_crc32c("")

                            # state_key and fully qualified table name are the same
                            table_info = bigquery_client.get_table(state_key)
                            table_state_value = InfoStateValue()
                            table_state_value["num_rows"] = table_info.num_rows
                            # TODO: fix date format so it's comparable
                            table_state_value["modified"] = str(table_info.modified)

                            state_value["data_crc32c"] = make_crc32c(
                                json.dumps(table_state_value, indent=2)
                            )

    return current_state


def upload_table(table_key):
    pass


parser = argparse.ArgumentParser()
parser.add_argument(
    "--bucket_prefix",
    help="Something like: table_loader --bucket-prefix 'gs://bucket_name'",
)
args = parser.parse_args()


def main(bucket_prefix=args.bucket_prefix):

    preferred_state = get_preferred_state()
    last_known_applied_state = get_last_known_applied_state(bucket_prefix)
    # current_state = get_current_state()

    for table_key, state_value in preferred_state.items():
        logger.info(f"Checking {table_key}")

        # TODO: write some tests
        if last_known_applied_state.get(table_key):
            logger.info(f"{table_key} exists.")

            if (
                preferred_state["schema_crc32c"]
                != last_known_applied_state["schema_crc32c"]
                and preferred_state["modified"] > last_known_applied_state["modified"]
            ):

                upload_table(table_key)

        else:
            logger.info(f"{table_key} does not exist in the last known applied state")


"""
    if the states are not equal which is the right state?

    preferred overwrites last_known UNLESS last_known.modified > preferred.modified

    remote overwrites current UNLESS current.modified > remote.modified

    how about we change the language to:
    local = preferred
    remote = last_known_applied
    current = current

    preferred overwrites last_known_applied unless preferred.modified < last_known_applied.modified
"""
