import pytest
from py.path import local
from uuid import uuid1
from google.cloud import bigquery


def pytest_addoption(parser):
    parser.addoption("--bucket-name")
    parser.addoption("--project-id")


@pytest.fixture()
def bucket_name(pytestconfig) -> str:
    return pytestconfig.getoption("bucket_name")


@pytest.fixture(scope="session")
def project_id(pytestconfig) -> str:
    return pytestconfig.getoption("project_id")


@pytest.fixture(scope="function")
def preferred_root(tmpdir: local) -> local:
    return tmpdir


@pytest.fixture(scope="session")
def dataset_name():
    return str(uuid1()).partition("-")[0]


@pytest.fixture(scope="session")
def table_name():
    return "standard_table"


@pytest.fixture(scope="session")
def dataset_id(project_id: str, dataset_name):
    dataset_id = f"{project_id}.{dataset_name}"
    client = bigquery.Client()
    client.create_dataset(dataset=dataset_id)

    yield dataset_id

    client.delete_dataset(dataset=dataset_id, delete_contents=True)


@pytest.fixture(scope="session")
def table_id(dataset_id: str, table_name: str):
    return f"{dataset_id}.{table_name}"
