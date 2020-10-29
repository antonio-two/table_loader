import typing
import dataclasses


@dataclasses.dataclass(frozen=True, eq=True, order=True)
class FieldDefinition:
    name: str
    type: str
    mode: str
    description: str
    # TODO: maybe add these later as the complexity increases
    # fields: object = field(default_factory=)
    # policy_tags: object = field(default_factory=)
    # policy_tags_names: typing.List[str] = field(default_factory=[])


class GridComponent:
    pass


class Schema(GridComponent):
    def __init__(self, schema: typing.Iterable[FieldDefinition]):
        self.schema = tuple(schema)

    def __hash__(self):
        return hash(self.schema)

    def __eq__(self, other):
        if not isinstance(other, Schema):
            return False
        return other.schema == self.schema


class Content(GridComponent):
    def __init__(self, payload_hash):
        self.payload_hash = payload_hash

    def __hash__(self):
        return hash(self.payload_hash)

    def __eq__(self, other):
        if not isinstance(other, Content):
            return False
        return other.payload_hash == self.payload_hash


class Sql(GridComponent):
    def __init__(self, query: str):
        self.query = query

    def __hash__(self):
        return hash(self.query)

    def __eq__(self, other):
        if isinstance(other, Sql):
            return False
        return self.query == other.query


# how many of these properties are part of the domain model?
# e.g what would we do if only the description has changed or if the labels are different?
# do we need a means to compare the properties to identify equality?
# do we need 2 different types of updating (one for data and one for metadata?)
# also  would we invert this so the model does not know GCP but GCP knows about the model?
@dataclasses.dataclass(frozen=True, eq=True, order=True)
class GridProperty:
    description: str
    labels: typing.Dict[str, str]
    grid_id: str
    # created: str
    # last_modified: str
    # grid_expiry: str


@dataclasses.dataclass(frozen=True, eq=True, order=True)
class TableProperty(GridProperty):
    # table_size: int
    rows: int
    # data_location: str


@dataclasses.dataclass(frozen=True, eq=True, order=True)
class ViewProperty(GridProperty):
    pass


@dataclasses.dataclass(frozen=True, eq=True, order=True)
class MaterialisedViewProperty(GridProperty):
    last_refresh_time: str
    refresh_enabled: bool
    refresh_interval: int


# Originally an Abstract naming for table_name in project.dataset.table_name.
# Now just an abstract class for table
# No hashing or equations necessary because if a TableComponent is
class Grid:
    pass


class Table(Grid):
    table_property: TableProperty
    schema: Schema
    content: Content


class View(Grid):
    view_property: ViewProperty
    sql: Sql


class MaterialisedView(Grid):
    materialised_view_property: MaterialisedViewProperty
    sql: Sql
