import argparse
from synchronisation.service_layer import actions, uow

parser = argparse.ArgumentParser()
parser.add_argument(
    "--project",
    help="--project=your-project --bucket-prefix"
    # f"or leave empty to recurse through every project in your 'projects' directory",
)
parser.add_argument(
    "--bucket-name",
    help="--bucket-name 'gs://bucket_name'",
)
args = parser.parse_args()


def main():
    uow.RealLifeUnitOfWork(...)
    actions.synchronise(uow)
