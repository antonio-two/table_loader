import copy
import json
import logging
import os
import typing
from uuid import uuid1

import py._path.local as local_path
import pytest
from google.cloud import bigquery
from table_loader.src.synchronisation.entrypoints import cli

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def project_id():
    return os.getenv("GOOGLE_CLOUD_PROJECT")


@pytest.fixture(scope="session")
def dataset_id(project_id: str):
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


@pytest.fixture(scope="session")
def table_id(dataset_id: str):
    return f"{dataset_id}.standard_table"


@pytest.fixture(scope="function")
def preferred_dir(tmpdir: local_path.LocalPath):
    return tmpdir / str(uuid1()).partition("-")[0]


SCHEMA_01 = [
    {"description": "S_ID", "mode": "REQUIRED", "name": "S_ID", "type": "INTEGER"}
]
SCHEMA_02 = [
    {"description": "S_ID", "mode": "REQUIRED", "name": "S_ID", "type": "INTEGER"},
    {"description": "S_NAME", "mode": "REQUIRED", "name": "S_NAME", "type": "STRING"},
]
SCHEMA_03 = [
    {"description": "S_NAME", "mode": "REQUIRED", "name": "S_NAME", "type": "STRING"}
]

DATA_01 = [{"S_ID": 1}]
DATA_02 = [{"S_ID": 1, "S_NAME": "ONE"}]
DATA_03 = [{"S_NAME": "ONE"}]
DATA_04 = [{"S_ID": 2}, {"S_ID": 3}]
DATA_05 = [{"S_ID": 2, "S_NAME": "TWO"}, {"S_ID": 3, "S_NAME": "THREE"}]
DATA_06 = [{"S_NAME": "TWO"}, {"S_NAME": "THREE"}]


def create_preferred_data(workdir, table_id, data):
    project_id, dataset_name, table_name = table_id.split(".")
    projects = workdir.join("projects")
    dataset_dir = projects.join(project_id, dataset_name)
    data_jsonl = "\n".join(json.dumps(f) for f in data)
    dataset_dir.join(f"{table_name}.jsonl").write(data_jsonl, ensure=True)


def create_preferred_schema(workdir, table_id, schema):
    project_id, dataset_name, table_name = table_id.split(".")
    projects = workdir.join("projects")
    dataset_dir = projects.join(project_id, dataset_name)
    schema_json = json.dumps(schema)
    dataset_dir.join(f"{table_name}.json").write(schema_json, ensure=True)


def create_preferred_empty(workdir):
    projects = workdir.join("projects")
    projects.ensure(dir=True)


def assert_current_data(
    schema: typing.List[typing.Dict[str, str]],
    table_id: str,
    data: typing.List[typing.Dict[str, typing.Any]],
):
    """
    Asserting preferred and current data match
    """
    client = bigquery.Client()

    preferred_table = bigquery.Table(table_id, schema=schema)
    current_table: bigquery.Table = client.get_table(table_id)

    assert current_table.schema == preferred_table.schema

    query_job = f"SELECT * FROM `{table_id}`"
    logger.info(f"Executing query: {query_job}")
    result = client.query(query_job)
    logger.info(f"Result: {result}")

    data = copy.copy(data)

    for row in result:
        row = dict(row)
        logger.info(f"Checking row: {row}")
        assert row in data
        data.remove(row)

    assert not data


def drop_table(table_id):
    client = bigquery.Client()
    client.delete_table(table_id, not_found_ok=True)


class TestExpectedUseCases:
    # TODO: logging -> caplog
    @staticmethod
    def test_new_table(preferred_dir, table_id):
        """
        A new table is added
        pytest tests/test_integration.py::TestExpectedUseCases
        ::test_new_table --log-cli-level=INFO
        """
        logger.info(f"Test local directory: {preferred_dir}")
        schema, data = SCHEMA_01, DATA_01
        create_preferred_schema(
            workdir=preferred_dir,
            schema=schema,
            table_id=table_id,
        )
        create_preferred_data(workdir=preferred_dir, table_id=table_id, data=data)
        os.chdir(preferred_dir)
        logger.info("Executing table_loader")
        cli.main()

        assert_current_data(schema=schema, table_id=table_id, data=data)

    @staticmethod
    def test_existing_table_no_change(preferred_dir, table_id):
        """
        Table exists. There is no change in schema/data
        pytest tests/test_integration.py::TestExpectedUseCases
        ::test_existing_table_no_change --log-cli-level=INFO
        """
        schema, data = SCHEMA_01, DATA_01
        create_preferred_schema(
            workdir=preferred_dir,
            schema=schema,
            table_id=table_id,
        )
        create_preferred_data(workdir=preferred_dir, table_id=table_id, data=data)
        os.chdir(preferred_dir)
        cli.main()

        client = bigquery.Client()
        table_properties = client.get_table(table=table_id)
        modified_before = table_properties.modified

        cli.main()
        table_properties = client.get_table(table=table_id)
        modified_after = table_properties.modified

        logger.info(
            f"modified_before={modified_before}, modified_after=" f"{modified_after}"
        )

        assert modified_before == modified_after

    # TODO: new rows are added to the jsonl
    # TODO: new columns are added to the jsonl - no columns added to json
    # TODO: new columns are added to the jsonl - columns added to json too

    @staticmethod
    def test_dropped_preferred_table(tmpdir: local_path.LocalPath, table_id):
        """
        Table exists -> local definitions are deleted -> table_loader executes
        pytest tests/test_integration.py::TestExpectedUseCases::test_dropped_preferred_table --log-cli-level=INFO
        """
        schema, data = SCHEMA_01, DATA_01
        wd_ = tmpdir / "wd1"
        create_preferred_data(wd_, table_id=table_id, data=data)
        create_preferred_schema(wd_, table_id=table_id, schema=schema)
        os.chdir(preferred_dir)
        cli.main()

        workdir = tmpdir / "wd2"
        create_preferred_empty(workdir=workdir)
        os.chdir(workdir)
        cli.main()

        client = bigquery.Client()

        with pytest.raises(Exception):
            client.get_table(table=table_id)

        # What do we assert here?


class TestEdgeUseCases:
    @staticmethod
    def test_disparate_schema_data(
        tmpdir: local_path.LocalPath, preferred_dir, table_id
    ):
        """
        Table exists -> table is dropped -> schema is changed ->
        table_loader executes
        No need to simulate a table drop, just a disparate schema/data
        Change the implementation to deal with partial schema/data matches
        or make it a valid failure
        Preferably the latter
        """
        schema, data = SCHEMA_02, DATA_01
        wd_ = tmpdir / "wd1"
        create_preferred_data(wd_, table_id=table_id, data=data)
        create_preferred_schema(wd_, table_id=table_id, schema=schema)
        os.chdir(preferred_dir)
        cli.main()

        assert_current_data(schema=schema, table_id=table_id, data=data)

    @staticmethod
    def test_existing_table_data_change(
        tmpdir: local_path.LocalPath, preferred_dir, table_id
    ):
        """
        Table exists -> data is added to the table directly -> table_loader
        executes
        pytest tests/test_integration.py::TestExpectedUseCases
        ::test_existing_table_data_change --log-cli-level=INFO
        """
        schema, data = SCHEMA_01, DATA_01
        wd_ = tmpdir / "wd1"
        create_preferred_data(wd_, table_id=table_id, data=data)
        create_preferred_schema(wd_, table_id=table_id, schema=schema)
        os.chdir(preferred_dir)
        cli.main()

        client = bigquery.Client()
        client.insert_rows_json(table=table_id, json_rows=DATA_04)

        assert_current_data(schema=schema, table_id=table_id, data=data)

    @staticmethod
    def test_existing_table_schema_change(
        tmpdir: local_path.LocalPath, preferred_dir, table_id
    ):
        """
        Table exists -> someone adds a column to the table directly ->
        table_loader executes
        pytest tests/test_integration.py::TestExpectedUseCases
        ::test_existing_table_schema_change --log-cli-level=INFO
        """
        schema, data = SCHEMA_01, DATA_01
        wd_ = tmpdir / "wd1"
        create_preferred_data(wd_, table_id=table_id, data=data)
        create_preferred_schema(wd_, table_id=table_id, schema=schema)
        os.chdir(preferred_dir)
        cli.main()

        client = bigquery.Client()
        table = client.get_table(table=table_id)
        original_schema = table.schema
        logger.info(f"original schema: {original_schema}")
        new_schema = original_schema[:]
        new_schema.append(bigquery.SchemaField("NEW_COLUMN", "STRING"))
        logger.info(f"new schema: {new_schema}")

        table.schema = new_schema
        table = client.update_table(table, ["schema"])

        if len(table.schema) == len(original_schema) + 1 == len(new_schema):
            logger.info("A new column has been added.")
        else:
            logger.info("The column has not been added.")

        cli.main()

        assert_current_data(schema=schema, table_id=table_id, data=data)

    @staticmethod
    def test_adding_rows_to_table_and_dropping_it(
        tmpdir: local_path.LocalPath, preferred_dir, table_id
    ):
        """
        Table exists -> extra rows are added -> table is dropped ->
        table_loader executes
        This makes no sense but coding it anyway for fun
        """
        schema, data = SCHEMA_01, DATA_01
        wd_ = tmpdir / "wd1"
        create_preferred_data(wd_, table_id=table_id, data=data)
        create_preferred_schema(wd_, table_id=table_id, schema=schema)
        os.chdir(preferred_dir)
        cli.main()

        client = bigquery.Client()
        client.insert_rows_json(table=table_id, json_rows=DATA_04)
        client.delete_table(table_id, not_found_ok=True)
        cli.main()

        assert_current_data(schema=schema, table_id=table_id, data=data)

    @staticmethod
    def test_dropped_table(tmpdir: local_path.LocalPath, preferred_dir, table_id):
        """
        Table exists -> table is dropped -> table_loader executes
        test_new_table
        """
        schema, data = SCHEMA_01, DATA_01
        wd_ = tmpdir / "wd1"
        create_preferred_data(wd_, table_id=table_id, data=data)
        create_preferred_schema(wd_, table_id=table_id, schema=schema)
        os.chdir(preferred_dir)
        cli.main()

        drop_table(table_id)
        cli.main()

        assert_current_data(schema=schema, table_id=table_id, data=data)
