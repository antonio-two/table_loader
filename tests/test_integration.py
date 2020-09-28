import os
from uuid import uuid1

import pytest
from google.cloud import bigquery

PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT")
DATASET_ID = str(uuid1()).partition("-")[0]
DATASET = f"{PROJECT_ID}.{DATASET_ID}"
TIMEOUT = 30


@pytest.fixture(scope="session")
def session_fixture():
    # Create the empty test dataset
    client = bigquery.Client()
    datasets = list(client.list_datasets())
    for dataset in datasets:
        if dataset.dataset_id == DATASET_ID:
            # Should we do this or break the test?
            client.delete_dataset(dataset=DATASET_ID, timeout=TIMEOUT)
            break

    dataset = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
    client.create_dataset(dataset=dataset, timeout=TIMEOUT)
    print(f"Created dataset {DATASET}")
    yield dataset

    # Tear down the emtpy test dataset
    client.delete_dataset(dataset=dataset, timeout=TIMEOUT)
    print(f"Deleting dataset {DATASET}")


@pytest.fixture(scope="function")
def function_fixture():

    test_data_path = os.path.join("projects", PROJECT_ID, "test_data")
    if os.path.exists(test_data_path):
        os.rmdir(test_data_path)

    os.mkdir(test_data_path)

    yield test_data_path
    os.rmdir(test_data_path)


def test_load_table1(session_fixture, function_fixture):
    print("test_load_table1")


# def test_load_table2(session_fixture):
#     print('test_load_table2')
#     # raise ValueError()
