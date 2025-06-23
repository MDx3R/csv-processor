from engine.execution.executors.executor import Executor
from engine.execution.plan.filter_plan import FilterPlan
from storage.tuple import Tuple


class FilterExecutor(Executor):
    def __init__(self, plan: FilterPlan, child: Executor) -> None:
        super().__init__()
        self._plan = plan
        self._child = child

    def init(self) -> None:
        self._child.init()

    def next(self) -> Tuple | None:
        while (tup := self._child.next()) is not None:
            result = self._plan.get_predicate().evaluate(tup)
            if result.is_null():
                continue
            if result.to_boolean().get_value():
                return tup
        return None
