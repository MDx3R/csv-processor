from engine.execution.expressions.expression import Expression
from storage.schema import Schema

from .execution_plan import ExecutionPlan


class FilterPlan(ExecutionPlan):
    def __init__(
        self,
        predicate: Expression,
        output_schema: Schema,
        child: ExecutionPlan,
    ) -> None:
        super().__init__(output_schema, [child])
        self._predicate = predicate

    def get_predicate(self) -> Expression:
        return self._predicate

    def get_child(self) -> ExecutionPlan:
        return self._children[0]
