# import inspect
import os
import pathlib

# from synchronisation.domain import model
import typing
from google.cloud import bigquery, storage
from synchronisation.domain import model
import abc


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

    def grid_to_path_prefix(self, grid_id):
        return self.root.joinpath(grid_id.split("."))

    def add(self, grid: model.Grid):
        prefix = self.grid_to_path_prefix(grid.id)
        description_path = f"{prefix}.description"
        with open(description_path, "wt") as fd:
            fd.write(grid.description)
        if isinstance(grid, model.Content):
            content_path = f"{prefix}.jsonl"
            # Serialise content to json lines
            content_lines = grid.content  # TODO
            with open(content_path, "wt") as fd:
                fd.write(content_lines)
        if isinstance(grid, model.Sql):
            sql_path = f"{prefix}.sql"
            with open(sql_path, "wt") as fd:
                fd.write(grid.query)
        if isinstance(grid, model.Schema):
            schema_path = f"{prefix}.json"
            json_schema = grid.schema  # TODO
            with open(schema_path, "wt") as fd:
                # Serialise schema to json
                fd.write(json_schema)

    def matching_files_from_grid_id(self, grid_id):
        prefix = self.grid_to_path_prefix(grid_id)
        dirname = os.path.dirname(prefix)
        grid_name = os.path.basename(prefix)
        for _, _, files in os.walk(dirname):
            for file in files:
                if file.startswith(f"{grid_name}."):
                    yield f"{dirname}/{file}"

    def type_from_files(self, files: typing.Iterable[str]):
        required_mixins = set()
        for file in files:
            if file.endswith("jsonl"):
                required_mixins.add(model.Content)
            if file.endswith("json"):
                required_mixins.add(model.Schema)
        for subclass in model.Grid.__subclasses__():
            if set(subclass.__mro__).issuperset(required_mixins):
                return subclass
        return ValueError(f"Unable to extract type from files {files}")

    def get(self, grid_id: str) -> model.Grid:
        grid_files = self.matching_files_from_grid_id(grid_id)
        cls = self.type_from_files(grid_files)
        return cls()

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
