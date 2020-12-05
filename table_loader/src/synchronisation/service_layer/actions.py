import typing

# from synchronisation.domain import model
from synchronisation.adapters import repository
from synchronisation.service_layer import uow


def decide_table_action():
    return "create|replace|update|delete"


def decide_actions(preferred_state, last_known, current):
    actions: typing.Dict[str, typing.Callable] = {}
    return actions


def create():
    pass


def replace():
    pass


def update():
    pass


def delete():
    pass


def synchronise(project: str, bucket_prefix: str):
    with uow:
        preferred_state = repository.FilesystemGridRepository.list()
        last_known = repository.GoogleCloudStorageGridRepository.list()
        current = repository.BigqueryGridRepository.list()

        actions: typing.Dict
        actions = decide_actions(
            preferred_state=preferred_state, last_known=last_known, current=current
        )

        for grid_id, action in actions.items():
            action()
            # call the message bus instead?
