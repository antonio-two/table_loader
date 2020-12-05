import abc

from table_loader.src.synchronisation.adapters import repository


class AbstractUnitOfWork(abc.ABC):
    preferred: repository.AbstractGridRepository
    current: repository.AbstractGridRepository
    last_known: repository.AbstractGridRepository

    def __exit__(self, *args):
        pass


class StateUnitOfWork(AbstractUnitOfWork):
    def __init__(self, bucket_prefix: str, billing_project: str, root: str):
        self.preferred = repository.FilesystemGridRepository(root=root)
        self.last_known = repository.GoogleCloudStorageGridRepository(
            bucket_name=bucket_prefix
        )
        self.current = repository.BigqueryGridRepository(
            billing_project=billing_project
        )

    def __enter__(self):
        self.preferred.get()

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
