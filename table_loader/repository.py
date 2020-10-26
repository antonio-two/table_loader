import abc
import model


class AbstractRepository:
    @abc.abstractmethod
    def add(
        self,
        grid_id: model.Grid,
        schema: model.Schema,
        content: model.Content,
        sql: model.Sql,
    ):
        raise NotImplementedError

    @abc.abstractmethod
    def get(self, grid: model.Grid):
        raise NotImplementedError


class GridRepository(AbstractRepository):
    # this is like a memory object containing all the
    def __init__(self, session):
        self.session = session

    def add(self, grid_id, schema, content, sql):
        self.grids.add(grid_id=grid_id, schema=schema, content=content, sql=sql)

    def get(self, grid):
        return self.grids.get(model.Grid.grid_id)

    def list(self):
        # lists all the actions?
        pass
