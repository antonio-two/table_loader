import logging
import os
from uuid import uuid1

import pytest
from google.cloud import bigquery
from table_loader import cli

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def project_id():
    return os.getenv("GOOGLE_CLOUD_PROJECT")


@pytest.fixture(scope="session")
def dataset_id(project_id):
    # Create the empty test dataset
    dataset = str(uuid1()).partition("-")[0]
    dataset_id = f"{project_id}.{dataset}"
    client = bigquery.Client()
    client.create_dataset(dataset=dataset_id)
    logger.info(f"Created dataset `{dataset}` in project `{project_id}`")

    yield dataset_id

    # Tear down the emtpy test dataset
    client.delete_dataset(dataset=dataset_id, delete_contents=True)
    logger.info(f"Deleting dataset `{dataset}` from project `{project_id}`")


@pytest.fixture(scope="function")
def standard_table(tmpdir, dataset_id: str):

    project_id, _, dataset_name = dataset_id.partition(".")
    dataset_dir = tmpdir.join("projects", project_id, dataset_name)
    table_name = "standard_table"

    dataset_dir.join(f"{table_name}.json").write(
        """[{"description": "test description", "mode": "required", "name": "id", "type": "int64"}]""",
        ensure=True,
    )
    logger.info(f"Created table `{table_name}`")

    dataset_dir.join(f"{table_name}.jsonl").write("""{"id":10}\n""")
    logger.info(f"Inserted one row into table `{table_name}`")

    yield f"{dataset_id}.{table_name}"


def test_load_table(tmpdir, standard_table):
    os.chdir(tmpdir)
    cli.main()

    client = bigquery.Client()
    job = client.query(f"select sum(id) as id_count from `{standard_table}`")

    result = job.result()

    for r in result:
        logger.info(r)


def test_new_table():
    """
    preferred exists, last_known !exist, current !exist
        1. upload preferred -> last_known
        2. load -> current
        3. download current info -> last_known
        if it breaks at any stage, retry failed stage [x] times, before failing permanently
        then reset the last_known and current states
    :return:
    """
    pass


def test_existing_table_no_change():
    """
    preferred exists, last_known exists, current exists
        1. last_known modified = current modified
        2. preferred schema_crc = last_known schema_crc
        3. preferred data_crc = last_known data_crc
        if it breaks at any stage, retry failed stage [x] times, before failing permanently
        no need to do anything else as we're only executing comparison operations
    :return:
    """
    pass


def test_existing_table_data_change():
    """
    preferred exists, last_known exists, current exists
        1. last_known modified = current modified
        2. preferred schema_crc = last_known schema_crc
        3. preferred data_crc != last_known data_crc
            1. upload preferred -> last_known
            2. make current backup
            3. load truncate -> current
            4. preferred row_num = current row_num (optional?)
            5. download current info -> last_known
            6. remove backup
        if it breaks at any stage, retry failed stage [x] times, before failing permanently
        then reset the last_known and current states
    :return:
    """
    pass


def test_existing_table_schema_change():
    """
    preferred exists, last_known exists, current exists
        1. last_known modified = current modified
        2. preferred schema_crc != last_known schema_crc
            1. upload preferred -> last_known
            2. make current backup
            3. load truncate -> current
            4. preferred row_num = current row_num (optional?)
            5. download current info -> last_known
            6. remove backup
        if it breaks at any stage, retry failed stage [x] times, before failing permanently
        then reset the last_known and current states
    :return:
    """
    pass


def test_existing_dropped_current_no_change():
    """
    preferred exists, last_known exists, current !exist
    :return:
    """
    pass


def test_existing_dropped_current_schema_change():
    """

    :return:
    """
    pass


def test_existing_dropped_current_data_change():
    """

    :return:
    """
    pass


def test_existing_dropped_last_known_no_change():
    """

    :return:
    """
    pass


def test_existing_dropped_last_known_schema_change():
    """

    :return:
    """
    pass


def test_existing_dropped_last_known_data_change():
    """

    :return:
    """
    pass


def test_existing_dropped_preferred():
    """

    :return:
    """
    pass
