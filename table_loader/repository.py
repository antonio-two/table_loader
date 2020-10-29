import abc
import typing
from table_loader import model


class AbstractTableRepository:
    @abc.abstractmethod
    def add(self, table: model.Table):
        raise NotImplementedError

    @abc.abstractmethod
    def get(self, table_id: str):
        raise NotImplementedError


class TableRepository(AbstractTableRepository):
    # TODO: how can this be broken out to a fixture?
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
