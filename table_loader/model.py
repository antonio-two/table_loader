import typing
import dataclasses


class TableComponent:  # Abstract
    pass


# immutable class with no behaviour
@dataclasses.dataclass(frozen=True, eq=True, order=True)
class FieldDefinition:
    name: str
    type: str
    mode: str
    description: str
    # fields: object = field(default_factory=)
    # policy_tags: object = field(default_factory=)
    # policy_tags_names: typing.List[str] = field(default_factory=[])


class Schema(TableComponent):
    # formatting the schema is not part of the domain model
    def __init__(self, schema: typing.Iterable[FieldDefinition]):
        self.schema = tuple(schema)

    def __hash__(self):
        return hash(self.schema)

    def __eq__(self, other):
        if not isinstance(other, Schema):
            return False
        return other.schema == self.schema


class Content(TableComponent):
    # iterating through and hashing the content is not part of the domain model
    def __init__(self, payload_hash):
        self.payload_hash = payload_hash

    def __hash__(self):
        return hash(self.payload_hash)

    def __eq__(self, other):
        if not isinstance(other, Content):
            return False
        return other.payload_hash == self.payload_hash


class Sql(TableComponent):
    # formatting the query is not part of the domain model
    def __init__(self, query: str):
        self.query = query

    def __hash__(self):
        return hash(self.query)

    def __eq__(self, other):
        if isinstance(other, Sql):
            return False
        return self.query == other.query


# how much of that is part of the domain model?
# e.g what would we do if only the description has changed
# or if the labels are different?
# do we need a means to compare the properties to identify equality?
@dataclasses.dataclass(frozen=True, eq=True, order=True)
class TableProperty:
    description: str
    labels: typing.Dict[str, str]
    table_id: str
    # table_size: int
    rows: int
    # created: str
    # table_expiry: str
    last_modified: str
    # data_location: str


class Grid:  # Abstract naming for table_name in project.dataset.table_name
    def __init__(self, table_property: TableProperty):
        self.description = table_property.description
        self.labels = table_property.labels
        self.table_id = table_property.table_id
        # self.table_size = table_property.table_size
        self.rows = table_property.rows
        # self.created = table_property.created
        # self.table_expiry = table_property.table_expiry
        self.last_modified = table_property.last_modified
        # self.data_location = table_property.data_location


# these classes consist of TableComponent so why not add the grid_id as part of the top abstract TableComponent class?
class View(Grid):
    sql: Sql
    schema: Schema


class MaterializedView(Grid):
    sql: Sql
    schema: Schema


class Table(Grid):
    content: Content
    schema: Schema
