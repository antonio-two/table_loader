import os
import os.path
from google.cloud import bigquery
import json
import typing


# TODO: Move custom exceptions  to another file OR do something else instead of validating before loading
class TableIdFormatError(Exception):
    def __init__(self, value: str):
        self.value = value

    def __str__(self):
        return f'{self.value} is not in the expected format of project.dataset.table'


class FileCountError(Exception):
    def __init__(self, value: str):
        self.value = value

    def __str__(self):
        return f'Expecting 2 files in {self.value} but found {len(self.value)}'


class ExtensionError(Exception):
    def __init__(self, value: str):
        self.value = value
        self.allowed_extensions = ['.json', '.jsonl']

    def __str__(self):
        return f'Found extension {self.value} while expecting one of {self.allowed_extensions}'


class EmptyFileError(Exception):
    def __init__(self, value: str):
        self.value = value

    def __str__(self):
        return f'{self.value} is empty'


class FileFormatError(Exception):
    def __init__(self, value: str):
        self.value = value

    def __str__(self):
        return f'{self.value} is not well formed'


def get_tables() -> typing.Dict[str, typing.List[str]]:
    projects = os.path.join(os.getcwd(), "projects")
    tables = dict()
    for root, dirs, files in os.walk(projects):
        if not files:
            continue

        project = os.path.basename(os.path.dirname(root))
        dataset = os.path.basename(root)

        files.sort()
        for file in files:
            file_name = file.partition('.')[0]
            table_key = f'{project}.{dataset}.{file_name}'
            file_path = os.path.join(root, file)
            tables.setdefault(table_key, []).append(file_path)

    return tables


def validate_table(table_id, files):
    # TODO read about google exceptions
    # TODO finish the data file validation
    # TODO decide whether to front-load the validation or use google exceptions
    def is_json(j: str):
        try:
            json.load(j)
        except ValueError as e:
            return False
        return True

    # Check the table_id is in the expected format: project.dataset.table
    if len(table_id.split('.')) != 3:
        raise TableIdFormatError(table_id)

    # Check there are 2 files (assuming one is a schema and one is a data file)
    if len(files) != 2:
        raise FileCountError(files)

    for file in files:
        _, extension = os.path.splitext(file)

        # Check the 2 files have the expected extensions
        if extension not in ['.json', '.jsonl']:
            raise ExtensionError(extension)

        # Check the 2 files are not empty
        with open(file, 'rt') as f:
            if not f.read():
                raise EmptyFileError(file)

        # Validate the json is well formed
        with open(file, 'rt') as f:
            if extension == '.json' and not is_json(f):
                raise FileFormatError(file)
            # elif extension == '.jsonl':
            #     with open(file) as f:

    return 0


def load_table(table_id: str, schema_path: str, data_path: str):
    # TODO: are we keeping the pdb/exeption here?

    client = bigquery.Client(project=table_id.partition('.')[0])

    with open(schema_path, 'rt') as source_schema:
        schema = json.load(source_schema)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=False,
        schema=schema
    )

    with open(data_path, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)

    try:
        job.result()
    except:
        import pdb
        pdb.set_trace()


def main():
    """
    https://docs.python.org/3/library/os.html
    :return:
    """
    tables = get_tables()

    for table_id, files in tables.items():
        vt = validate_table(table_id, files)
        print(vt)
        # load_table(table_id=table_id, schema_path=schema_path, data_path=data_path)
