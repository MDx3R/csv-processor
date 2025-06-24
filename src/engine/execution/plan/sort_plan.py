from engine.execution.expressions.expression import Expression
from engine.execution.plan.execution_plan import ExecutionPlan
from storage.schema import Schema


class SortPlan(ExecutionPlan):
    def __init__(
        self,
        order_by: list[Expression],
        output_schema: Schema,
        child: ExecutionPlan,
    ) -> None:
        super().__init__(output_schema, [child])
        self._order_by = order_by

    def get_order_by(self) -> list[Expression]:
        return self._order_by

    def get_child(self) -> ExecutionPlan:
        return self._children[0]
