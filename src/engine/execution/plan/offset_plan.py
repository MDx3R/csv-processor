from storage.schema import Schema

from .execution_plan import ExecutionPlan


class OffsetPlan(ExecutionPlan):
    def __init__(
        self,
        offset: int,
        output_schema: Schema,
        child: ExecutionPlan,
    ) -> None:
        super().__init__(output_schema, [child])
        self._offset = offset

    def get_offset(self) -> int:
        return self._offset

    def get_child(self) -> ExecutionPlan:
        return self._children[0]
