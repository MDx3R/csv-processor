from engine.execution.executors.executor import Executor
from engine.execution.plan.projection_plan import ProjectionPlan
from storage.tuple import Tuple


class ProjectionExecutor(Executor):
    def __init__(self, plan: ProjectionPlan, child: Executor) -> None:
        self._plan = plan
        self._child = child
        self._expressions = plan.get_expressions()
        self._output_schema = plan.get_output_schema()

    def init(self) -> None:
        self._child.init()

    def next(self) -> Tuple | None:
        try:
            input_tuple = self._child.next()
        except StopIteration:
            return None
        if input_tuple is None:
            return None

        values = [expr.evaluate(input_tuple) for expr in self._expressions]
        return Tuple(values, self._output_schema)
