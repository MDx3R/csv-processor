from engine.execution.executors.executor_factory import ExecutorFactory
from engine.execution.parser import Parser
from engine.execution.planner import QueryPlanner
from storage.table import Table
from storage.tuple import Tuple


class Engine:
    def __init__(
        self,
        table_registry: dict[str, Table],
        parser: Parser,
        query_planner: QueryPlanner,
        executor_factory: ExecutorFactory,
    ):
        self.table_registry = table_registry
        self.parser = parser
        self.query_planner = query_planner
        self.executor_factory = executor_factory

    def run(self, args: list[str]) -> list[Tuple]:
        select_stmt = self.parser.parse(args)

        table_name = select_stmt.from_table
        if table_name not in self.table_registry:
            raise ValueError(f"Table '{table_name}' not found")

        logical_plan = self.query_planner.create_plan(select_stmt)

        executor = self.executor_factory.create_executor(logical_plan)

        executor.init()

        result: list[Tuple] = []
        while (tup := executor.next()) is not None:
            result.append(tup)

        return result
