from engine.execution.executors.executor import Executor
from engine.execution.plan.sort_plan import SortPlan
from storage.tuple import Tuple


class SortExecutor(Executor):
    def __init__(self, plan: SortPlan, child: Executor) -> None:
        self._plan = plan
        self._order_by = plan.get_order_by()
        self._child = child
        self._sorted: list[Tuple] | None = None
        self._idx = 0

    def init(self) -> None:
        self._child.init()
        rows: list[Tuple] = []
        while (tup := self._child.next()) is not None:
            rows.append(tup)
        rows.sort(
            key=lambda t: [
                expr.evaluate(t).get_value() for expr in self._order_by
            ]
        )
        self._sorted = rows
        self._idx = 0

    def next(self) -> Tuple | None:
        if self._sorted is None:
            raise RuntimeError("Executor not initialized. Call init() first.")
        if self._idx >= len(self._sorted):
            return None
        tup = self._sorted[self._idx]
        self._idx += 1
        return tup
