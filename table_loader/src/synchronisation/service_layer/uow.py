import abc
from table_loader.src.synchronisation.adapters import repository


class AbstractUnitOfWork(abc.ABC):
    tables: repository.AbstractTableRepository
