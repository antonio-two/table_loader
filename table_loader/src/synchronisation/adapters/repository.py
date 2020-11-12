import abc
import os
import pathlib
import typing
from synchronisation.domain import model
from google.cloud import storage


class AbstractGridRepository:
    @abc.abstractmethod
    def add(
        self,
        source_path: str,
        target_path: str,
    ):
        raise NotImplementedError

    @abc.abstractmethod
    def get(self, grid_id):
        raise NotImplementedError

    @abc.abstractmethod
    def list(self):
        raise NotImplementedError

    @abc.abstractmethod
    def remove(self, grid_id):
        raise NotImplementedError


class FilesystemGridRepository(AbstractGridRepository):
    def __init__(self):
        pass

    def add(self, source_path, target_path):
        return NotImplementedError

    def get(self, source_path: pathlib.Path):
        with open(source_path, "r") as f:
            return f.read()

    def list(self):
        return os.listdir(self.dataset_path)

    def remove(self, source_path: pathlib.Path):
        os.remove(source_path)


class GoogleCloudStorageGridRepository(AbstractGridRepository):
    def __init__(self, bucket_name: str):
        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket_name)

    def add(self, source_path: pathlib.Path, target_path: str):
        blob = self.bucket.blob(target_path)
        blob.upload_from_filename(source_path)

    def get(self, target_path: str):
        pass

    def list(self):
        pass

    def remove(self, target_path: str):
        pass


class BigqueryGridRepository(AbstractGridRepository):
    def __init__(self):
        pass

    def add(
        self,
        grid_id: str,
        grid: typing.Union[model.Table, model.View, model.MaterialisedView],
    ):
        pass

    def get(self, grid_id: str):
        pass

    def list(self):
        pass

    def remove(self, grid_id: str):
        pass
