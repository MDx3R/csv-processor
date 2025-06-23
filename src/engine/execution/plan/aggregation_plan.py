from engine.execution.aggregate import Aggregate
from engine.execution.expressions.expression import Expression
from storage.schema import Schema

from .execution_plan import ExecutionPlan


class AggregationPlan(ExecutionPlan):
    def __init__(
        self,
        group_bys: list[Expression],
        aggregates: list[Aggregate],
        output_schema: Schema,
        child: ExecutionPlan,
    ):
        super().__init__(output_schema, [child])
        self._group_bys = group_bys
        self._aggregates = aggregates

    def get_group_bys(self) -> list[Expression]:
        return self._group_bys.copy()

    def get_aggregates(self) -> list[Aggregate]:
        return self._aggregates.copy()

    def get_child(self) -> ExecutionPlan:
        return self._children[0]
