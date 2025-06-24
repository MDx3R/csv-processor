from engine.execution.executors.executor import Executor
from engine.execution.plan.offset_plan import OffsetPlan
from storage.tuple import Tuple


class OffsetExecutor(Executor):
    def __init__(self, plan: OffsetPlan, child: Executor) -> None:
        self._plan = plan
        self._offset = plan.get_offset()
        self._child = child
        self._count = 0

    def init(self) -> None:
        self._count = 0
        self._child.init()

    def next(self) -> Tuple | None:
        while (tup := self._child.next()) is not None:
            if self._count < self._offset:
                self._count += 1
                continue
            return tup
        return None
