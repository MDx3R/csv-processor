from engine.execution.executors.executor_factory import ExecutorFactory
from engine.execution.plan.execution_plan import ExecutionPlan
from storage.tuple import Tuple


class Engine:
    def __init__(
        self,
        plan: ExecutionPlan,
        executor_factory: ExecutorFactory,
    ):
        self.plan = plan
        self.executor_factory = executor_factory

    def run(self) -> list[Tuple]:
        executor = self.executor_factory.create_executor(self.plan)

        executor.init()

        result: list[Tuple] = []
        while (tup := executor.next()) is not None:
            result.append(tup)

        return result
