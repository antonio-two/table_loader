import argparse
from synchronisation.service_layer import actions  # , uow

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


# services.synchronise(
#     # pass the unit of work
# )


def main():
    actions.synchronise(project=args.project, bucket_prefix=args.bucket_prefix)
