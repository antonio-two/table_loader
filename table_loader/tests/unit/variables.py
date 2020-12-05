from synchronisation.domain import model

JSON_SCHEMA_1 = [
    {
        "name": "ID",
        "type": "INT",
        "mode": "REQUIRED",
        "description": "this is the identifier of a row",
    },
]

JSON_SCHEMA_2 = [
    {
        "name": "ID",
        "type": "INT",
        "mode": "REQUIRED",
        "description": "this is the identifier of a row",
    },
    {
        "name": "NAME",
        "type": "STRING",
        "mode": "REQUIRED",
        "description": "this is the name of a row",
    },
]

PAYLOAD_1 = [{"ID": 2}, {"ID": 3}]

PAYLOAD_2 = [{"ID": 2, "NAME": "TWO"}, {"ID": 3, "NAME": "THREE"}]

PAYLOAD_1_HASH = hash(str(PAYLOAD_1))

PAYLOAD_2_HASH = hash(str(PAYLOAD_2))

FIELD_DEFINITIONS_1 = [
    model.FieldDefinition(
        name="ID",
        type="INT",
        mode="REQUIRED",
        description="this is the identifier of a row",
    ),
]

FIELD_DEFINITIONS_2 = [
    model.FieldDefinition(
        name="ID",
        type="INT",
        mode="REQUIRED",
        description="this is the identifier of a row",
    ),
    model.FieldDefinition(
        name="NAME",
        type="STRING",
        mode="REQUIRED",
        description="this is the name of a row",
    ),
]

DESCRIPTION = "This is a description"
LABELS = {"label 1": "label 1 content"}
MODIFICATION_DATE_1 = "2019-12-31 00:00:00"
MODIFICATION_DATE_2 = "2020-12-31 00:00:00"
