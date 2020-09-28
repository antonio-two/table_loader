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
