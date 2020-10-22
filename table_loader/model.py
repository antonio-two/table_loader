import struct
import typing
import dataclasses
import crcmod.predefined
import base64
import json


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
                    "name": field.name,
                    "type": field.type,
                    "mode": field.mode,
                    "description": field.description,
                }
            )
        return json.dumps(pretty_schema, sort_keys=True, indent=1)

    def __repr__(self):
        return json.loads(self.__str__())

    def __hash__(self):
        crc32c = crcmod.predefined.Crc("crc-32c")
        crc32c.update(self.schema.__str__().encode("utf-8"))
        return base64.b64encode(struct.pack(">I", crc32c.crcValue)).decode("utf-8")

    def __eq__(self, other):
        if not isinstance(other, Schema):
            return False
        return other.__hash__() == self.__hash__()


class Content(TablePart):
    payload: typing.Iterable[typing.Dict[str, typing.Any]]


class Sql(TablePart):
    query: str


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

    identifier = FieldDefinition(
        description="test desc", mode="REQUIRED", name="IDENTIFIER", type="INTEGER"
    )
    name = FieldDefinition(
        description="test desc", mode="REQUIRED", name="NAME", type="STRING"
    )
    s1 = Schema([identifier, name])
    s2 = Schema([identifier, name])
    print(s1.__hash__(), s1.__str__())
    print(s1.__eq__(s2), s1.__hash__(), s2.__hash__())
    new_name = FieldDefinition(
        description="test desc blah", mode="REQUIRED", name="NAME", type="STRING"
    )
    s3 = Schema([identifier, new_name])
    print(s1.__eq__(s3), s1.__hash__(), s3.__hash__())
