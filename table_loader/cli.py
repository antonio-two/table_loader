import argparse

parser = argparse.ArgumentParser()
parser.add_argument(
    "--project",
    help="table_loader --project=your-project --bucket-prefix 'gs://bucket_name'"
    # f"or leave empty to recurse through every project in your 'projects' directory",
)
parser.add_argument(
    "--bucket-prefix",
    help="Something like: table_loader --bucket-prefix 'gs://bucket_name'",
)
args = parser.parse_args()

SCHEMA_EXT = "json"
DATA_EXT = "jsonl"


def get_tables():
    pass


def get_action_for_table(table: str):
    return


def exec_action_for_table(action, table):
    return


def main():

    pass
