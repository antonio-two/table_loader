from synchronisation.domain import model
import logging
import json
import table_loader.tests.unit.variables as tuv

logger = logging.getLogger(__name__)


def test_schema_json_is_set():

    table = model.Table()
    table.set_schema_from_json(tuv.JSON_SCHEMA_2)
    assert table.get_schema_json() == json.dumps(tuv.JSON_SCHEMA_2)


def test_field_definitions():

    table = model.Table()
    table.set_schema_from_json(tuv.JSON_SCHEMA_2)
    assert table.get_field_definitions() == tuv.FIELD_DEFINITIONS_2


def test_table_comparison(grid_id):

    local_file = model.Table()
    local_file.grid_id = grid_id
    local_file.description = tuv.DESCRIPTION
    local_file.labels = tuv.LABELS
    local_file.modification_date = tuv.MODIFICATION_DATE
    local_file.schema = tuv.JSON_SCHEMA_2
    local_file.payload = tuv.PAYLOAD_2
    local_file.payload_hash = tuv.PAYLOAD_2_HASH
    local_file.rows = 2

    remote_file = model.Table()
    remote_file.grid_id = grid_id
    remote_file.description = tuv.DESCRIPTION
    remote_file.labels = tuv.LABELS
    remote_file.modification_date = tuv.MODIFICATION_DATE
    remote_file.schema = tuv.JSON_SCHEMA_2
    remote_file.payload = tuv.PAYLOAD_2
    remote_file.payload_hash = tuv.PAYLOAD_2_HASH
    remote_file.rows = 2

    # content is identical
    # metadata is identical
    assert local_file == remote_file
    assert local_file.is_metadata_unchanged(remote_file) is True

    # metadata has changed
    local_file.description = f"{tuv.DESCRIPTION} + added text"
    assert local_file.is_metadata_unchanged(remote_file) is False

    # content has changed
    local_file.schema = tuv.JSON_SCHEMA_1
    local_file.payload = tuv.PAYLOAD_1
    assert local_file != remote_file
