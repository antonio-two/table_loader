import abc
import typing
from synchronisation.domain import model


class AbstractGridRepository:
    @abc.abstractmethod
    def add(
        self,
        grid_id: str,
        grid: typing.Union[model.Table, model.View, model.MaterialisedView],
    ):
        raise NotImplementedError

    @abc.abstractmethod
    def get(self, grid_id: str):
        raise NotImplementedError

    @abc.abstractmethod
    def list(self):
        raise NotImplementedError

    @abc.abstractmethod
    def remove(self, grid_id: str):
        raise NotImplementedError


class FakeGridRepository(AbstractGridRepository):
    def __init__(self):
        self.grids_in_memory: typing.Dict[
            str, typing.Union[model.Table, model.View, model.MaterialisedView]
        ] = {}

    def add(
        self,
        grid_id: str,
        grid: typing.Union[model.Table, model.View, model.MaterialisedView],
    ):
        self.grids_in_memory[grid_id] = grid

    def get(self, grid_id: str):
        return self.grids_in_memory[grid_id]

    def list(self):
        return self.grids_in_memory

    def remove(self, grid_id: str):
        self.grids_in_memory.pop(grid_id)


class FilesystemGridRepository(AbstractGridRepository):
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


class GoogleCloudStorageGridRepository(AbstractGridRepository):
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
