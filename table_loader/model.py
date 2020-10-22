import struct
import typing
import dataclasses
import crcmod.predefined
import base64
import json
import sqlparse


class TablePart:  # Abstract
    pass


# immutable class with no behaviour
@dataclasses.dataclass(frozen=True)
class FieldDefinition(TablePart):
    name: str
    type: str
    mode: str
    description: str
    # fields: object = field(default_factory=)
    # policy_tags: object = field(default_factory=)
    # policy_tags_names: typing.List[str] = field(default_factory=[])


class Schema(TablePart):
    def __init__(self, schema: typing.Iterable[FieldDefinition]):
        self.schema = schema

    def __str__(self):
        pretty_schema = []
        for field in self.schema:
            pretty_schema.append(
                {
                    "description": field.description,
                    "mode": field.mode,
                    "name": field.name,
                    "type": field.type,
                }
            )
        return json.dumps(pretty_schema, sort_keys=True, indent=1)

    def __repr__(self):
        # not sure
        # return json.loads(self.__str__())
        pass

    def __hash__(self):
        crc32c = crcmod.predefined.Crc("crc-32c")
        crc32c.update(self.schema.__str__().encode("utf-8"))
        return base64.b64encode(struct.pack(">I", crc32c.crcValue)).decode("utf-8")

    def __eq__(self, other):
        if not isinstance(other, Schema):
            return False
        return other.__hash__() == self.__hash__()


class Content(TablePart):
    def __init__(self, payload):
        payload: typing.Iterable[typing.Dict[str, typing.Any]]

    def __hash__(self):
        pass

    def __eq__(self, other):
        pass

    def num_rows(self):
        pass

    def num_columns(self):
        pass


class Sql(TablePart):
    def __init__(self, query: str):
        self.query = query

    def __str__(self):
        return sqlparse.format(
            self.query, reindent=True, keyword_case="upper", strip_comments=True
        )

    def __hash__(self):
        return hash(self.__str__())

    def __eq__(self, other):
        if isinstance(other, Sql):
            return False
        return self.__hash__() == other.__hash__()


class Meta(TablePart):
    # TODO: How do we represent modified date? Is it part of the domain model? or implementation detail?
    pass


class Grid:  # Abstract naming for table_name in project.dataset.table_name
    grid_id: str


class View(Grid):
    sql: Sql
    schema: Schema


class MaterializedView(Grid):
    sql: Sql
    schema: Schema


class Table(Grid):
    content: Content
    schema: Schema


def test_is_data_the_same():
    pass


def main():

    s = Sql(query="select * from x as x1 join z as z1 on x1.i = x1.i where x1.a = 1")
    print(s.__str__())
