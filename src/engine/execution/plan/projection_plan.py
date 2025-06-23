from engine.execution.expressions.expression import Expression
from storage.schema import Schema

from .execution_plan import ExecutionPlan


class ProjectionPlan(ExecutionPlan):
    def __init__(
        self,
        expressions: list[Expression],
        output_schema: Schema,
        child: ExecutionPlan,
    ) -> None:
        super().__init__(output_schema, [child])
        self._expressions = expressions.copy()

    def get_expressions(self) -> list[Expression]:
        return self._expressions

    def get_child(self) -> ExecutionPlan:
        return self._children[0]
