import typing
import dataclasses


class Grid:
    def __init__(
        self,
        grid_id: str = None,
        description: str = "Not Set",
        labels: typing.Dict[str, str] = {},
    ):
        self.grid_id = grid_id
        self.description = description
        self.labels = labels


class GridComponent:
    pass


@dataclasses.dataclass(frozen=True, eq=True, order=True)
class FieldDefinition:
    name: str
    type: str
    mode: str
    description: str
    # TODO: maybe add fields later as the complexity increases
    # fields: object = field(default_factory=)
    # the below SHOULD NOT be part of this model but just adding it for completion for now
    # policy_tags: object
    # policy_tags_names: typing.List[str]


class Schema(GridComponent):
    def __init__(self, schema: typing.Iterable[FieldDefinition] = None):
        self.schema = tuple(schema)


class Content(GridComponent):
    def __init__(self, payload_hash: str = None):
        self.payload_hash = payload_hash


class Sql(GridComponent):
    def __init__(self, query: str = None):
        self.query = query


# @dataclasses.dataclass(frozen=True, eq=True, order=True)
class Table(Grid, Schema, Content):
    def __init__(self, rows: int = None):
        super().__init__()
        self.rows = rows

    def __repr__(self):
        return (
            f"{self.grid_id=}, {self.description=}, {self.labels=}, "
            f"{self.rows=}, {self.schema=}, {self.payload_hash=}"
        )


class View(Grid, Schema, Sql):
    def __init__(self):
        super().__init__()


class MaterialisedView(Grid, Schema, Sql):
    def __init__(self, refresh_enabled: bool = True, refresh_interval: int = 1800000):
        super().__init__()
        self.refresh_enabled = refresh_enabled
        self.refresh_interval = refresh_interval
