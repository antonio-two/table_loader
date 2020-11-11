"""
in cosmic python this is an abstraction over the idea of persistent (?) storage
could we use this as an abstraction over the idea of memory storage?
"""

import abc
import typing
from synchronisation.domain import model
from synchronisation.adapters import repository_loader


class AbstractGridRepository:
    @abc.abstractmethod
    def add(self, grid_id, grid):
        raise NotImplementedError

    @abc.abstractmethod
    def get(self, grid_id):
        raise NotImplementedError


class InMemoryGridRepository(AbstractGridRepository):
    def __init__(self, state_type: str):
        """
        :param state_type: preferred | last_known | current
        """
        self.state_type = state_type
        self.grids_in_memory: typing.Dict[
            str, typing.Union[model.Table, model.View, model.MaterialisedView]
        ] = {}

    def load_repository(self):
        return repository_loader.get_preferred_state(self.state_type)

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

    def create_or_replace(self):
        pass

    def drop(self):
        pass

    def update_property(self):
        pass
