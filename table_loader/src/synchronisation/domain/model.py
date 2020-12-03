import typing
import dataclasses
import simplejson as json


class Grid:
    grid_id: str
    description: str = ""
    labels: typing.Dict[str, str] = {}
    modification_date: str = ""

    def is_metadata_unchanged(self, other):
        if not isinstance(other, Grid):
            return False
        return (
            self.description == other.description
            and self.labels == other.labels
            and self.modification_date == other.modification_date
        )


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


class SchemaMixin(GridComponent):
    schema: typing.List[typing.Dict] = []

    def set_schema_from_json(self, json_schema: typing.List[typing.Dict]):
        self.schema.clear()
        self.schema = json_schema

    def get_schema_json(self):
        return json.dumps(self.schema)

    def get_field_definitions(self):
        # TODO: is there a way to abstract the field_definitions population
        field_definitions: typing.List[FieldDefinition] = []
        for field in self.schema:
            field_definitions.append(
                FieldDefinition(
                    name=field["name"],
                    type=field["type"],
                    mode=field["mode"],
                    description=field["description"],
                )
            )
        return field_definitions


class ContentMixin(GridComponent):
    # TODO: should we move the hashing into the model or is it an implementation detail?
    payload_hash: typing.Optional[str] = ""
    payload: typing.Optional[typing.Iterable[dict]] = []


class SqlMixin(GridComponent):
    query: typing.Optional[str]


class Table(Grid, SchemaMixin, ContentMixin):
    rows: typing.Optional[int] = 0

    def __eq__(self, other):
        if not isinstance(other, Table):
            return False
        return (
            self.grid_id == other.grid_id
            and self.schema == other.schema
            and self.payload == other.payload
            and self.payload_hash == other.payload_hash
            and self.rows == other.rows
        )


class View(Grid, SchemaMixin, SqlMixin):
    pass


class MaterialisedView(Grid, SchemaMixin, SqlMixin):
    refresh_enabled: bool = True
    refresh_interval: int = 1800000
