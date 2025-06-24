from engine.execution.executors.executor import Executor
from engine.execution.plan.limit_plan import LimitPlan
from storage.tuple import Tuple


class LimitExecutor(Executor):
    def __init__(self, plan: LimitPlan, child: Executor) -> None:
        self._plan = plan
        self._limit = plan.get_limit()
        self._child = child
        self._count = 0

    def init(self) -> None:
        if self._limit == 0:
            return

        self._count = 0
        self._child.init()

    def next(self) -> Tuple | None:
        if self._limit == 0:
            return None

        tup = self._child.next()
        if tup is None:
            return None
        if self._count >= self._limit:
            return None
        self._count += 1
        return tup
