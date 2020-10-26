import pytest
import typing
import model


class TableComponents:
    schema: model.Schema
    content: model.Content
    sql: model.Sql


@pytest.fixture
def tables_in_memory():
    return typing.Dict[str, TableComponents]


@pytest.fixture
def session(tables_in_memory):
    # this is where we populate the grids dictionary with:
    # grid_id, schema, content, sql
    pass
