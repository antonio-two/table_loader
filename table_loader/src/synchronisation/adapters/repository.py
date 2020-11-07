"""
in cosmic python this is an abstraction over the idea of persistent (?) storage
could we use this as an abstraction over the idea of memory storage?
"""

import abc
import typing
from table_loader.src.synchronisation.domain import model


class AbstractTableRepository:
    @abc.abstractmethod
    def add(self, table: model.Table):
        raise NotImplementedError

    @abc.abstractmethod
    def get(self, table_id: str):
        raise NotImplementedError


# this is the state abstraction
class InMemoryTableRepository(AbstractTableRepository):
    # TODO: how can this be broken out to a session fixture?
    # what does the session in class SqlAlchemyRepository(AbstractRepository) refer to
    # https://www.cosmicpython.com/book/chapter_02_repository.html
    def __init__(self):
        self.tables_in_memory: typing.Dict[
            str, typing.List[model.TableProperty, model.Schema, model.Content]
        ] = {}

    def add(self, table: model.Table):
        self.tables_in_memory[table.table_property.grid_id] = [
            table.table_property,
            table.schema.schema,
            table.content.payload_hash,
        ]

    def get(self, table_id: str):
        return self.tables_in_memory[table_id]

    def list(self):
        return self.tables_in_memory

    def create(self):
        pass

    def update_meta(self):
        pass

    def remove(self):
        pass


class FakeTableRepository(InMemoryTableRepository):
    pass
