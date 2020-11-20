from synchronisation.adapters import repository

# import pytest
import os
import logging
from table_loader.tests.helpers import schema_object_from_json

logger = logging.getLogger(__name__)


# do we need a fake repository at all?
class FakeFilesystemGridRepository(repository.AbstractGridRepository):
    def __init__(self, root):
        self.root = root

    def add(self, target_path, content):
        if not os.path.exists(os.path.dirname(target_path)):
            os.mkdir(os.path.dirname(target_path))
        with open(target_path, "x") as tp:
            tp.write(content)

    def get(self, source_path):
        with open(source_path, "r") as f:
            return f.read()

    def list(self, dataset_path):
        return sorted(os.listdir(dataset_path))

    def remove(self, source_path):
        os.remove(source_path)


def test_adding_a_file(preferred_root, dataset_name, table_name):
    fake_fs_repository = FakeFilesystemGridRepository(root=preferred_root)
    target_path = f"{preferred_root}/{dataset_name}/{table_name}.jsonl"
    content = '{"STATIC_ID":1}'
    fake_fs_repository.add(target_path=target_path, content=content)

    assert os.path.basename(target_path) in fake_fs_repository.list(
        os.path.dirname(target_path)
    )


def test_getting_a_file(preferred_root, dataset_name, table_name):
    fake_fs_repository = FakeFilesystemGridRepository(root=preferred_root)
    target_path = f"{preferred_root}/{dataset_name}/{table_name}.jsonl"
    content = '{"STATIC_ID":1}'
    fake_fs_repository.add(target_path=target_path, content=content)
    assert fake_fs_repository.get(target_path) == content


def test_listing_files(preferred_root, dataset_name, table_name):
    fake_fs_repository = FakeFilesystemGridRepository(root=preferred_root)
    target_path = f"{preferred_root}/{dataset_name}/{table_name}.jsonl"
    content = '{"STATIC_ID":1}'
    fake_fs_repository.add(target_path=target_path, content=content)

    target_path = f"{preferred_root}/{dataset_name}/{table_name}.json"
    content = [
        {
            "description": "ID",
            "mode": "REQUIRED",
            "name": "STATIC_ID",
            "type": "INTEGER",
        }
    ]
    fake_fs_repository.add(target_path=target_path, content=str(content))

    assert fake_fs_repository.list(dataset_path=os.path.dirname(target_path)) == [
        "standard_table.json",
        "standard_table.jsonl",
    ]


def test_removing_a_file(preferred_root, dataset_name, table_name):
    fake_fs_repository = FakeFilesystemGridRepository(root=preferred_root)
    target_path = f"{preferred_root}/{dataset_name}/{table_name}.jsonl"
    content = '{"STATIC_ID":1}'

    fake_fs_repository.add(target_path=target_path, content=content)
    assert fake_fs_repository.list(dataset_path=os.path.dirname(target_path)) == [
        os.path.basename(target_path)
    ]

    fake_fs_repository.remove(target_path)
    assert fake_fs_repository.list(dataset_path=os.path.dirname(target_path)) == []


def test_uploading_a_file(preferred_root, dataset_name, table_name, bucket_name):
    # TODO: make all grid class related code generic, except from the ADD
    fake_fs_repository = FakeFilesystemGridRepository(root=preferred_root)
    gcs_repository = repository.GoogleCloudStorageGridRepository(
        bucket_name=bucket_name
    )

    file_path = f"{preferred_root}/{dataset_name}/{table_name}.jsonl"
    content = '{"STATIC_ID":1}'
    # add
    fake_fs_repository.add(target_path=file_path, content=content)

    target_postfix = f"{dataset_name}/{os.path.basename(file_path)}"
    gcs_repository.add(source_path=file_path, target_url=f"{target_postfix}")

    for blob in gcs_repository.list(prefix=dataset_name):
        assert target_postfix == blob.name

    gcs_repository.remove(target_postfix=f"{target_postfix}")


def test_getting_an_uploaded_file(
    preferred_root, dataset_name, table_name, bucket_name
):
    # TODO: make all grid class related code generic, except from the GET
    fake_fs_repository = FakeFilesystemGridRepository(root=preferred_root)
    gcs_repository = repository.GoogleCloudStorageGridRepository(
        bucket_name=bucket_name
    )

    file_path = f"{preferred_root}/{dataset_name}/{table_name}.jsonl"
    content = '{"STATIC_ID":1}'
    fake_fs_repository.add(target_path=file_path, content=content)

    target_postfix = f"{dataset_name}/{os.path.basename(file_path)}"
    gcs_repository.add(source_path=file_path, target_url=f"{target_postfix}")

    # get
    blob = gcs_repository.get(source_url=target_postfix)
    assert target_postfix == blob.name

    gcs_repository.remove(target_postfix=f"{target_postfix}")


def test_listing_uploaded_files(preferred_root, dataset_name, table_name, bucket_name):
    # TODO: make all grid class related code generic, except from the LIST
    fake_fs_repository = FakeFilesystemGridRepository(root=preferred_root)
    gcs_repository = repository.GoogleCloudStorageGridRepository(
        bucket_name=bucket_name
    )

    file_path = f"{preferred_root}/{dataset_name}/{table_name}.jsonl"
    content = '{"STATIC_ID":1}'
    fake_fs_repository.add(target_path=file_path, content=content)

    target_postfix = f"{dataset_name}/{os.path.basename(file_path)}"
    gcs_repository.add(source_path=file_path, target_url=f"{target_postfix}")

    # list
    for blob in gcs_repository.list(prefix=dataset_name):
        assert target_postfix == blob.name

    gcs_repository.remove(target_postfix=f"{target_postfix}")


def test_removing_an_uploaded_file(
    preferred_root, dataset_name, table_name, bucket_name
):
    # TODO: make all grid class related code generic, except from the REMOVE
    fake_fs_repository = FakeFilesystemGridRepository(root=preferred_root)
    gcs_repository = repository.GoogleCloudStorageGridRepository(
        bucket_name=bucket_name
    )

    file_path = f"{preferred_root}/{dataset_name}/{table_name}.jsonl"
    content = '{"STATIC_ID":1}'
    fake_fs_repository.add(target_path=file_path, content=content)

    target_postfix = f"{dataset_name}/{os.path.basename(file_path)}"
    gcs_repository.add(source_path=file_path, target_url=f"{target_postfix}")

    for blob in gcs_repository.list(prefix=dataset_name):
        assert target_postfix == blob.name

    # remove
    gcs_repository.remove(target_postfix=f"{target_postfix}")


def test_adding_table(
    preferred_root, project_id, dataset_id, bucket_name, dataset_name, table_name
):
    fake_fs_repository = FakeFilesystemGridRepository(root=preferred_root)
    gcs_repository = repository.GoogleCloudStorageGridRepository(
        bucket_name=bucket_name
    )
    bq_repository = repository.BigqueryGridRepository(billing_project=project_id)

    file_path = f"{preferred_root}/{dataset_name}/{table_name}.jsonl"
    data_content = '{"STATIC_ID":1, "LETTER": "A"}\n{"STATIC_ID":2, "LETTER": "B"}'
    fake_fs_repository.add(target_path=file_path, content=data_content)

    data_target_postfix = f"{dataset_name}/{os.path.basename(file_path)}"
    gcs_repository.add(source_path=file_path, target_url=f"{data_target_postfix}")

    schema_path = f"{preferred_root}/{dataset_name}/{table_name}.json"
    schema_content = [
        {
            "description": "ID",
            "mode": "REQUIRED",
            "name": "STATIC_ID",
            "type": "INTEGER",
        },
        {
            "description": "LETTER",
            "mode": "REQUIRED",
            "name": "LETTER",
            "type": "STRING",
        },
    ]
    fake_fs_repository.add(target_path=schema_path, content=str(schema_content))

    schema_target_postfix = f"{dataset_name}/{os.path.basename(schema_path)}"
    gcs_repository.add(source_path=file_path, target_url=f"{schema_target_postfix}")

    grid_id = f"{dataset_id}.{table_name}"
    data_uri = f"gs://{bucket_name}/{data_target_postfix}"
    bq_repository.add(
        grid_id=grid_id,
        schema=schema_object_from_json(schema_content),
        data_uri=data_uri,
        sql_query=None,
    )


def test_getting_table():
    pass


def test_listing_tables():
    pass


def test_removing_table():
    pass
