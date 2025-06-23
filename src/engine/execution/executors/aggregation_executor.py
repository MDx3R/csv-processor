from collections.abc import Iterator
from copy import deepcopy

from engine.execution.aggregate import AggregationType
from engine.execution.executors.executor import Executor
from engine.execution.expressions.aggregate_expression import (
    AggregateExpression,
    AvgExpression,
    CountExpression,
    MaxExpression,
    MinExpression,
    SumExpression,
)
from engine.execution.plan.aggregation_plan import AggregationPlan
from storage.tuple import Tuple
from type.value import Value


GROUP_KEY = tuple[Value, ...]
GROUP_VALUE = list[AggregateExpression]


class AggregationExecutor(Executor):
    def __init__(self, plan: AggregationPlan, child: Executor) -> None:
        self._plan = plan
        self._child = child
        self._hash_table: dict[GROUP_KEY, GROUP_VALUE] = {}
        self._group_bys = plan.get_group_bys()
        self._aggregates = plan.get_aggregates()
        self._output_schema = plan.get_output_schema()

        self._result_iterator: (
            Iterator[tuple[GROUP_KEY, GROUP_VALUE]] | None
        ) = None

    def init(self) -> None:
        self._child.init()

        while (tup := self._child.next()) is not None:
            group_key = tuple(expr.evaluate(tup) for expr in self._group_bys)
            if group_key not in self._hash_table:
                self._hash_table[group_key] = self.get_initital_values()

            states = self._hash_table[group_key]
            for agg, state in zip(self._aggregates, states, strict=False):
                state.update(agg.expr.evaluate(tup))

        self._result_iterator = iter(self._hash_table.items())

    def next(self) -> Tuple | None:
        if self._result_iterator is None:
            raise RuntimeError("Executor not initialized. Call init() first.")

        try:
            group_key, agg_values = next(self._result_iterator)
        except StopIteration:
            return None

        values: list[Value] = []
        values.extend(group_key)

        for agg in agg_values:
            values.append(agg.finalize())

        return Tuple(values, self._plan.get_output_schema())

    def get_initital_values(self) -> list[AggregateExpression]:
        values: list[AggregateExpression] = []
        for aggr in self._aggregates:
            expr = None
            match aggr.type:
                case AggregationType.SUM:
                    expr = SumExpression()
                case AggregationType.COUNT:
                    expr = CountExpression()
                case AggregationType.AVG:
                    expr = AvgExpression()
                case AggregationType.MIN:
                    expr = MinExpression()
                case AggregationType.MAX:
                    expr = MaxExpression()
                case _:
                    pass
            if expr is None:
                raise TypeError(
                    f"Unsupported aggregation operation {aggr.type}"
                )
            values.append(expr)
        return values

    def get_hash_table(self) -> dict[GROUP_KEY, GROUP_VALUE]:
        return deepcopy(self._hash_table)
