# import pytest
from synchronisation.adapters import repository

# import typing


class InMemoryFakeGridRepository(repository.AbstractGridRepository):
    def __init__(
        self,
    ):
        self.grids_in_memory = {
            "project.dataset.grid": {
                "grid_id": "project.dataset.grid",
                "description": "description text",
                "labels": {},
                "rows": 1,
                "schema": '[{"description": "ID","mode": "REQUIRED","name": "GRID_ID","type": "INTEGER"}]',
                "payload_hash": "PE+oAA==",
            }
        }

    def get(self):
        pass

    def add(self):
        pass
