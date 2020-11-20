import abc
import os
import pathlib

# import typing
# from synchronisation.domain import model
from google.cloud import bigquery, storage


class AbstractGridRepository:
    @abc.abstractmethod
    def add(self, *args):
        raise NotImplementedError

    @abc.abstractmethod
    def get(self, *args):
        raise NotImplementedError

    @abc.abstractmethod
    def list(self, *args):
        raise NotImplementedError

    @abc.abstractmethod
    def remove(self, *args):
        raise NotImplementedError


class FilesystemGridRepository(AbstractGridRepository):
    def __init__(self, root: pathlib.Path):
        # do we even need root here?
        self.root = root

    def add(self, target_path: bytes, content: str):
        with open(target_path, "w") as tp:
            tp.write(content)

    def get(self, source_path: pathlib.Path):
        with open(source_path, "r") as f:
            return f.read()

    def list(self, dataset_path: pathlib.Path):
        return os.listdir(dataset_path)

    def remove(self, source_path: pathlib.Path):
        os.remove(source_path)


class GoogleCloudStorageGridRepository(AbstractGridRepository):
    def __init__(self, bucket_name: str):
        self.client = storage.Client()
        self.bucket_name = bucket_name
        self.bucket = self.client.bucket(bucket_name)

    def add(self, source_path: str, target_url: str):
        blob = self.bucket.blob(target_url)
        blob.upload_from_filename(source_path)

    def get(self, source_url: str):
        return self.bucket.blob(source_url)

    def list(self, prefix: str):
        return self.client.list_blobs(bucket_or_name=self.bucket_name, prefix=prefix)

    def remove(self, target_postfix: str):
        blob = self.bucket.blob(target_postfix)
        blob.delete()


class BigqueryGridRepository(AbstractGridRepository):
    def __init__(self, billing_project: str):
        self.billing_project = billing_project
        self.client = bigquery.Client()

    def add(self, grid_id, schema, sql_query, data_uri):
        """
        :param grid_id:
        :param schema:
        :param sql_query: if set, it's assumed the grid is a view
        :param data_uri: if set, it's assumed the grid is a table
        :return:
        """
        grid = bigquery.Table(table_ref=grid_id, schema=schema)

        if sql_query:
            grid.view_query = sql_query

        self.client.create_table(grid)

        if data_uri:
            job_config = bigquery.LoadJobConfig(
                schema=schema,
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            )
        load_job = self.client.load_table_from_uri(
            source_uris=data_uri, destination=grid_id, job_config=job_config
        )
        load_job.result()

    def get(self, grid_id: str):
        self.client.get_table(grid_id)

    def list(self, dataset_id: str):
        # TODO: might require a bit of formatting or
        #  maybe we do that somewhere else?
        return self.client.list_tables(dataset_id)

    def remove(self, grid_id: str):
        self.client.delete_table(table=grid_id, not_found_ok=True)
