import base64

# import collections
import logging
import os
import struct

# import simplejson as json
import typing
from crcmod import crcmod
from synchronisation.domain import model

# from synchronisation.adapters import repository

logger = logging.getLogger()
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO
)


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


def is_table(project: str, dataset: str, grid_name: str):
    expected_file_name = os.path.join(
        os.getcwd(), "projects", project, dataset, f"{grid_name}.jsonl"
    )
    return os.path.isfile(expected_file_name)


def is_view(project: str, dataset: str, grid_name: str):
    expected_file_name = os.path.join(
        os.getcwd(), "projects", project, dataset, f"{grid_name}.sql"
    )
    if os.path.isfile(expected_file_name):
        with open(expected_file_name, "r") as efn:
            # this is not enough
            return efn.read().startswith("create or replace view")
    return False


def is_materialised_view(project: str, dataset: str, grid_name: str):
    expected_file_name = os.path.join(
        os.getcwd(), "projects", project, dataset, f"{grid_name}.sql"
    )
    if os.path.isfile(expected_file_name):
        with open(expected_file_name, "r") as efn:
            # this is not enough
            return efn.read().startswith("create materialized view")
    return False


def get_grid_types(project: str, dataset: str) -> typing.Dict[str, str]:
    grid_types: typing.Dict[str, str] = {}

    for root, dirs, files in os.walk(os.path.join(os.getcwd(), "projects", project)):
        if not files:
            continue

        file: str
        for file in files:
            grid_name = file.partition(".")[0]

            if is_table(project, dataset, grid_name):
                grid_types[grid_name] = "table"

            if is_view(project, dataset, grid_name):
                grid_types[grid_name] = "view"

            if is_materialised_view(project, dataset, grid_name):
                grid_types[grid_name] = "materialised_view"

        return grid_types


def get_preferred_state(project: str, dataset: str, grid_types: typing.Dict[str, str]):

    grids: typing.Dict[
        str, typing.Union[model.Table, model.View, model.MaterialisedView]
    ] = {}

    for grid_name, grid_type in grid_types.items():

        fqfn = os.path.join(os.getcwd(), "projects", project, dataset, grid_name)
        grid_id = ".".join([project, dataset, grid_name])
        content_file: str
        schema_file: str
        sql_file: str

        if grid_type == "table":
            table = model.Table()
            table.grid_id = grid_id

            content_file = f"{fqfn}.jsonl"
            with open(content_file) as f:
                f_content = f.read()
                table.payload_hash = make_crc32c(f_content)
                table.rows = sum(1 for _ in f_content.splitlines())

            schema_file = f"{fqfn}.json"
            with open(schema_file, "r") as f:
                # TODO: normalise schema format
                table.schema = f.read()

            grids[grid_id] = table

        elif grid_type == "view":
            pass

        elif grid_type == "materialised_view":
            pass

    return grids


# def main():
#     p = "table-loader-dev"
#     d = "static_data"
#
#     print(
#         get_preferred_state(
#             project=p, dataset=d, grid_types=get_grid_types(project=p, dataset=d)
#         )
#     )
