from storage.schema import Schema

from .execution_plan import ExecutionPlan


class LimitPlan(ExecutionPlan):
    def __init__(
        self,
        limit: int,
        output_schema: Schema,
        child: ExecutionPlan,
    ) -> None:
        super().__init__(output_schema, [child])
        self._limit = limit

    def get_limit(self) -> int:
        return self._limit

    def get_child(self) -> ExecutionPlan:
        return self._children[0]
