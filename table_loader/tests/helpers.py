import typing
from google.cloud import bigquery


def schema_object_from_json(schema: typing.List[typing.Dict]) -> typing.List:
    schema_output: typing.List = []
    for field in schema:
        schema_output.append(bigquery.SchemaField(field["name"], field["type"]))

    return schema_output
