from table_loader import model


def test_two_schemas_are_the_same():
    field_definition_1 = model.FieldDefinition(
        name="ID", type="INT", mode="REQUIRED", description="Nothing"
    )
    field_definition_2 = model.FieldDefinition(
        description="Nothing", name="ID", type="INT", mode="REQUIRED"
    )
    schema_1 = model.Schema([field_definition_1])
    schema_2 = model.Schema([field_definition_2])

    assert schema_1.schema == schema_2.schema
