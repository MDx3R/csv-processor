from collections.abc import Iterator

from engine.execution.executors.executor import Executor
from engine.execution.plan.scan_plan import ScanPlan
from storage.reader import TableReader
from storage.tuple import Tuple


class ScanExecutor(Executor):
    def __init__(self, plan: ScanPlan, reader: TableReader) -> None:
        super().__init__()
        self._plan = plan
        self._reader = reader
        self._iterator = None

    def init(self) -> None:
        self._iterator = self._reader.read()

    def next(self) -> Tuple | None:
        if self._iterator is None:
            raise RuntimeError("Executor not initialized. Call init() first.")
        # TODO: move tuple construction from TableReader here
        try:
            return next(self._iterator)
        except StopIteration:
            return None

    def get_iterator(self) -> Iterator[Tuple]:
        if self._iterator is None:
            raise RuntimeError("Executor not initialized. Call init() first.")

        return self._iterator
