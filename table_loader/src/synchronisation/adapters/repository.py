import abc
import os
import pathlib

# import typing
# from synchronisation.domain import model
from google.cloud import bigquery, storage


class AbstractGridRepository:
    @abc.abstractmethod
    def add(self, **kwargs):
        raise NotImplementedError

    @abc.abstractmethod
    def get(self, **kwargs):
        raise NotImplementedError

    @abc.abstractmethod
    def list(self, **kwargs):
        raise NotImplementedError

    @abc.abstractmethod
    def remove(self, **kwargs):
        raise NotImplementedError


class FilesystemGridRepository(AbstractGridRepository):
    def add(self):
        return NotImplementedError

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
        self.bucket = self.client.bucket(bucket_name)

    def add(self, source_path: pathlib.Path, target_url: str):
        blob = self.bucket.blob(target_url)
        blob.upload_from_filename(source_path)

    def get(self, source_url: str):
        return self.bucket.blob(source_url)

    def list(self, directory_url: str):
        return self.client.list_blobs(directory_url)

    def remove(self, target_url: str):
        blob = self.bucket(target_url)
        blob.delete()


class BigqueryGridRepository(AbstractGridRepository):
    def __init__(self):
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
