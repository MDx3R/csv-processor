from unittest.mock import Mock

import pytest

from engine.execution.aggregate import Aggregate, AggregationType
from engine.execution.executors.aggregation_executor import AggregationExecutor
from engine.execution.executors.executor import Executor
from engine.execution.expressions.aggregate_expression import (
    AvgExpression,
    CountExpression,
    MaxExpression,
    MinExpression,
    SumExpression,
)
from engine.execution.expressions.expression import Expression
from engine.execution.plan.aggregation_plan import AggregationPlan
from storage.schema import Column, Schema
from storage.tuple import Tuple
from type.type_enum import TypeEnum
from type.value import Value


class TestAggregationExecutorInit:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.schema = Schema(
            [Column("group", TypeEnum.STRING), Column("count", TypeEnum.INT)]
        )
        self.group_by_expr = Mock(spec=Expression)
        self.group_by_expr.evaluate.return_value = Value(
            TypeEnum.STRING, "group1"
        )
        self.aggregate_expr = Mock(spec=Expression)
        self.aggregate_expr.evaluate.return_value = Value(TypeEnum.INT, 10)
        self.aggregate = Aggregate(
            AggregationType.COUNT, self.aggregate_expr, "count"
        )
        self.child_executor = Mock(spec=Executor)
        self.tuple = Tuple(
            [Value(TypeEnum.STRING, "group1"), Value(TypeEnum.INT, 10)],
            self.schema,
        )
        self.child_executor.next.return_value = self.tuple
        self.plan = AggregationPlan(
            [self.group_by_expr], [self.aggregate], self.schema, Mock()
        )
        self.executor = AggregationExecutor(self.plan, self.child_executor)

    def test_init_populates_hash_table(self):
        self.child_executor.next.side_effect = [self.tuple, None]
        self.executor.init()
        hash_table = self.executor.get_hash_table()

        assert len(hash_table) == 1

        group_key = (Value(TypeEnum.STRING, "group1"),)

        assert group_key in hash_table
        assert len(hash_table[group_key]) == 1
        assert isinstance(hash_table[group_key][0], CountExpression)

        self.child_executor.init.assert_called_once()
        self.group_by_expr.evaluate.assert_called_once_with(self.tuple)
        self.aggregate_expr.evaluate.assert_called_once_with(self.tuple)

    def test_next_without_init_raises(self):
        with pytest.raises(RuntimeError):
            self.executor.next()

    def test_next_yields_tuples(self):
        self.child_executor.next.side_effect = [self.tuple, None]
        self.executor.init()
        result = self.executor.next()

        assert isinstance(result, Tuple)

        assert result.schema == self.schema
        assert len(result.values) == 2

        assert result.values[0].compare_equals(
            Value(TypeEnum.STRING, "group1")
        )

        assert result.values[1].compare_equals(Value(TypeEnum.INT, 1))
        assert self.executor.next() is None

    def test_next_empty_input(self):
        self.child_executor.next.return_value = None
        self.executor.init()
        assert self.executor.next() is None
        self.group_by_expr.evaluate.assert_not_called()
        self.aggregate_expr.evaluate.assert_not_called()

    def test_get_initial_values_count(self):
        self.executor._aggregates = [  # type: ignore
            Aggregate(AggregationType.COUNT, Mock(), "count")
        ]

        values = self.executor.get_initital_values()

        assert len(values) == 1
        assert isinstance(values[0], CountExpression)

    def test_get_initial_values_avg(self):
        self.executor._aggregates = [  # type: ignore
            Aggregate(AggregationType.AVG, Mock(), "avg")
        ]
        values = self.executor.get_initital_values()
        assert len(values) == 1
        assert isinstance(values[0], AvgExpression)

    def test_get_initial_values_sum(self):
        self.executor._aggregates = [  # type: ignore
            Aggregate(AggregationType.SUM, Mock(), "sum")
        ]
        values = self.executor.get_initital_values()
        assert len(values) == 1
        assert isinstance(values[0], SumExpression)

    def test_get_initial_values_min(self):
        self.executor._aggregates = [  # type: ignore
            Aggregate(AggregationType.MIN, Mock(), "min")
        ]
        values = self.executor.get_initital_values()
        assert len(values) == 1
        assert isinstance(values[0], MinExpression)

    def test_get_initial_values_max(self):
        self.executor._aggregates = [  # type: ignore
            Aggregate(AggregationType.MAX, Mock(), "max")
        ]
        values = self.executor.get_initital_values()
        assert len(values) == 1
        assert isinstance(values[0], MaxExpression)

    def test_get_initial_values_unsupported_type(self):
        self.executor._aggregates = [Aggregate("INVALID", Mock(), "invalid")]  # type: ignore
        with pytest.raises(TypeError):
            self.executor.get_initital_values()


class TestAggregationExecutorWork:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.schema = Schema(
            [Column("group", TypeEnum.STRING), Column("count", TypeEnum.INT)]
        )
        self.group_by_expr = Mock(spec=Expression)
        self.group_by_expr.evaluate.return_value = Value(
            TypeEnum.STRING, "group1"
        )
        self.aggregate_expr = Mock(spec=Expression)
        self.aggregate_expr.evaluate.return_value = Value(TypeEnum.INT, 10)
        self.aggregate = Aggregate(
            AggregationType.COUNT, self.aggregate_expr, "count"
        )
        self.child_executor = Mock(spec=Executor)
        self.tuple = Tuple(
            [Value(TypeEnum.STRING, "group1"), Value(TypeEnum.INT, 10)],
            self.schema,
        )
        self.child_executor.next.return_value = self.tuple
        self.plan = AggregationPlan(
            [self.group_by_expr], [self.aggregate], self.schema, Mock()
        )
        self.executor = AggregationExecutor(self.plan, self.child_executor)

    def test_count_aggregation(self):
        schema = Schema(
            [Column("group", TypeEnum.STRING), Column("count", TypeEnum.INT)]
        )
        expr = Mock(spec=Expression)
        expr.evaluate.side_effect = [
            Value(TypeEnum.INT, 10),
            Value(TypeEnum.INT, None),
        ]
        aggregate = Aggregate(AggregationType.COUNT, expr, "count")
        plan = AggregationPlan(
            [self.group_by_expr], [aggregate], schema, Mock()
        )
        executor = AggregationExecutor(plan, self.child_executor)
        self.child_executor.next.side_effect = [
            Tuple(
                [Value(TypeEnum.STRING, "group1"), Value(TypeEnum.INT, 10)],
                schema,
            ),
            Tuple(
                [Value(TypeEnum.STRING, "group1"), Value(TypeEnum.INT, None)],
                schema,
            ),
            None,
        ]
        executor.init()
        result = executor.next()
        assert result is not None
        assert result.values[0].compare_equals(
            Value(TypeEnum.STRING, "group1")
        )
        assert result.values[1].compare_equals(
            Value(TypeEnum.INT, 1)
        )  # Null is ignored

    def test_sum_aggregation(self):
        schema = Schema(
            [Column("group", TypeEnum.STRING), Column("sum", TypeEnum.DECIMAL)]
        )
        expr = Mock(spec=Expression)
        expr.evaluate.side_effect = [
            Value(TypeEnum.DECIMAL, 10.5),
            Value(TypeEnum.DECIMAL, 20.5),
        ]
        aggregate = Aggregate(AggregationType.SUM, expr, "sum")
        plan = AggregationPlan(
            [self.group_by_expr], [aggregate], schema, Mock()
        )
        executor = AggregationExecutor(plan, self.child_executor)
        self.child_executor.next.side_effect = [
            Tuple(
                [
                    Value(TypeEnum.STRING, "group1"),
                    Value(TypeEnum.DECIMAL, 10.5),
                ],
                schema,
            ),
            Tuple(
                [
                    Value(TypeEnum.STRING, "group1"),
                    Value(TypeEnum.DECIMAL, 20.5),
                ],
                schema,
            ),
            None,
        ]
        executor.init()
        result = executor.next()
        assert result is not None
        assert result.values[0].compare_equals(
            Value(TypeEnum.STRING, "group1")
        )
        assert result.values[1].compare_equals(Value(TypeEnum.DECIMAL, 31.0))

    def test_avg_aggregation(self):
        schema = Schema(
            [Column("group", TypeEnum.STRING), Column("avg", TypeEnum.DECIMAL)]
        )
        expr = Mock(spec=Expression)
        expr.evaluate.side_effect = [
            Value(TypeEnum.DECIMAL, 10.0),
            Value(TypeEnum.DECIMAL, 20.0),
        ]
        aggregate = Aggregate(AggregationType.AVG, expr, "avg")
        plan = AggregationPlan(
            [self.group_by_expr], [aggregate], schema, Mock()
        )
        executor = AggregationExecutor(plan, self.child_executor)
        self.child_executor.next.side_effect = [
            Tuple(
                [
                    Value(TypeEnum.STRING, "group1"),
                    Value(TypeEnum.DECIMAL, 10.0),
                ],
                schema,
            ),
            Tuple(
                [
                    Value(TypeEnum.STRING, "group1"),
                    Value(TypeEnum.DECIMAL, 20.0),
                ],
                schema,
            ),
            None,
        ]
        executor.init()
        result = executor.next()
        assert result is not None
        assert result.values[0].compare_equals(
            Value(TypeEnum.STRING, "group1")
        )
        assert result.values[1].compare_equals(Value(TypeEnum.DECIMAL, 15.0))

    def test_min_aggregation(self):
        schema = Schema(
            [Column("group", TypeEnum.STRING), Column("min", TypeEnum.DECIMAL)]
        )
        expr = Mock(spec=Expression)
        expr.evaluate.side_effect = [
            Value(TypeEnum.DECIMAL, 10.0),
            Value(TypeEnum.DECIMAL, 5.0),
        ]
        aggregate = Aggregate(AggregationType.MIN, expr, "min")
        plan = AggregationPlan(
            [self.group_by_expr], [aggregate], schema, Mock()
        )
        executor = AggregationExecutor(plan, self.child_executor)
        self.child_executor.next.side_effect = [
            Tuple(
                [
                    Value(TypeEnum.STRING, "group1"),
                    Value(TypeEnum.DECIMAL, 10.0),
                ],
                schema,
            ),
            Tuple(
                [
                    Value(TypeEnum.STRING, "group1"),
                    Value(TypeEnum.DECIMAL, 5.0),
                ],
                schema,
            ),
            None,
        ]
        executor.init()
        result = executor.next()
        assert result is not None
        assert result.values[0].compare_equals(
            Value(TypeEnum.STRING, "group1")
        )
        assert result.values[1].compare_equals(Value(TypeEnum.DECIMAL, 5.0))

    def test_max_aggregation(self):
        schema = Schema(
            [Column("group", TypeEnum.STRING), Column("max", TypeEnum.DECIMAL)]
        )
        expr = Mock(spec=Expression)
        expr.evaluate.side_effect = [
            Value(TypeEnum.DECIMAL, 10.0),
            Value(TypeEnum.DECIMAL, 20.0),
        ]
        aggregate = Aggregate(AggregationType.MAX, expr, "max")
        plan = AggregationPlan(
            [self.group_by_expr], [aggregate], schema, Mock()
        )
        executor = AggregationExecutor(plan, self.child_executor)
        self.child_executor.next.side_effect = [
            Tuple(
                [
                    Value(TypeEnum.STRING, "group1"),
                    Value(TypeEnum.DECIMAL, 10.0),
                ],
                self.schema,
            ),
            Tuple(
                [
                    Value(TypeEnum.STRING, "group1"),
                    Value(TypeEnum.DECIMAL, 20.0),
                ],
                schema,
            ),
            None,
        ]
        executor.init()
        result = executor.next()
        assert result is not None
        assert result.values[0].compare_equals(
            Value(TypeEnum.STRING, "group1")
        )
        assert result.values[1].compare_equals(Value(TypeEnum.DECIMAL, 20.0))

    def test_multiple_aggregations(self):
        schema = Schema(
            [
                Column("group", TypeEnum.STRING),
                Column("count", TypeEnum.INT),
                Column("sum", TypeEnum.DECIMAL),
                Column("avg", TypeEnum.DECIMAL),
            ]
        )
        count_expr = Mock(spec=Expression)
        count_expr.evaluate.side_effect = [
            Value(TypeEnum.INT, 1),
            Value(TypeEnum.INT, 1),
        ]
        sum_expr = Mock(spec=Expression)
        sum_expr.evaluate.side_effect = [
            Value(TypeEnum.DECIMAL, 10.0),
            Value(TypeEnum.DECIMAL, 20.0),
        ]
        avg_expr = Mock(spec=Expression)
        avg_expr.evaluate.side_effect = [
            Value(TypeEnum.DECIMAL, 10.0),
            Value(TypeEnum.DECIMAL, 20.0),
        ]
        count_agg = Aggregate(AggregationType.COUNT, count_expr, "count")
        sum_agg = Aggregate(AggregationType.SUM, sum_expr, "sum")
        avg_agg = Aggregate(AggregationType.AVG, avg_expr, "avg")
        plan = AggregationPlan(
            [self.group_by_expr], [count_agg, sum_agg, avg_agg], schema, Mock()
        )
        executor = AggregationExecutor(plan, self.child_executor)
        self.child_executor.next.side_effect = [
            Tuple(
                [
                    Value(TypeEnum.STRING, "group1"),
                    Value(TypeEnum.DECIMAL, 10.0),
                ],
                self.schema,
            ),
            Tuple(
                [
                    Value(TypeEnum.STRING, "group1"),
                    Value(TypeEnum.DECIMAL, 20.0),
                ],
                self.schema,
            ),
            None,
        ]
        executor.init()
        result = executor.next()
        assert result is not None
        assert result.values[0].compare_equals(
            Value(TypeEnum.STRING, "group1")
        )
        assert result.values[1].compare_equals(Value(TypeEnum.INT, 2))  # COUNT
        assert result.values[2].compare_equals(
            Value(TypeEnum.DECIMAL, 30.0)
        )  # SUM
        assert result.values[3].compare_equals(
            Value(TypeEnum.DECIMAL, 15.0)
        )  # AVG

    def test_multiple_groups(self):
        schema = Schema(
            [Column("group", TypeEnum.STRING), Column("sum", TypeEnum.DECIMAL)]
        )
        expr = Mock(spec=Expression)
        expr.evaluate.side_effect = [
            Value(TypeEnum.DECIMAL, 10.0),
            Value(TypeEnum.DECIMAL, 20.0),
        ]
        aggregate = Aggregate(AggregationType.SUM, expr, "sum")
        self.group_by_expr.evaluate.side_effect = [
            Value(TypeEnum.STRING, "group1"),
            Value(TypeEnum.STRING, "group2"),
        ]
        plan = AggregationPlan(
            [self.group_by_expr], [aggregate], schema, Mock()
        )
        executor = AggregationExecutor(plan, self.child_executor)
        self.child_executor.next.side_effect = [
            Tuple(
                [
                    Value(TypeEnum.STRING, "group1"),
                    Value(TypeEnum.DECIMAL, 10.0),
                ],
                self.schema,
            ),
            Tuple(
                [
                    Value(TypeEnum.STRING, "group2"),
                    Value(TypeEnum.DECIMAL, 20.0),
                ],
                self.schema,
            ),
            None,
        ]
        executor.init()
        result1 = executor.next()
        assert result1 is not None
        result2 = executor.next()
        assert result2 is not None
        assert result1.values[0].compare_equals(
            Value(TypeEnum.STRING, "group1")
        )
        assert result1.values[1].compare_equals(Value(TypeEnum.DECIMAL, 10.0))
        assert result2.values[0].compare_equals(
            Value(TypeEnum.STRING, "group2")
        )
        assert result2.values[1].compare_equals(Value(TypeEnum.DECIMAL, 20.0))

    def test_null_values_all_aggregations(self):
        schema = Schema(
            [
                Column("group", TypeEnum.STRING),
                Column("count", TypeEnum.INT),
                Column("sum", TypeEnum.DECIMAL),
                Column("avg", TypeEnum.DECIMAL),
                Column("min", TypeEnum.DECIMAL),
                Column("max", TypeEnum.DECIMAL),
            ]
        )
        count_expr = Mock(spec=Expression)
        count_expr.evaluate.return_value = Value.create_null_from_type_id(
            TypeEnum.INT
        )
        sum_expr = Mock(spec=Expression)
        sum_expr.evaluate.return_value = Value.create_null_from_type_id(
            TypeEnum.DECIMAL
        )
        avg_expr = Mock(spec=Expression)
        avg_expr.evaluate.return_value = Value.create_null_from_type_id(
            TypeEnum.DECIMAL
        )
        min_expr = Mock(spec=Expression)
        min_expr.evaluate.return_value = Value.create_null_from_type_id(
            TypeEnum.DECIMAL
        )
        max_expr = Mock(spec=Expression)
        max_expr.evaluate.return_value = Value.create_null_from_type_id(
            TypeEnum.DECIMAL
        )
        count_agg = Aggregate(AggregationType.COUNT, count_expr, "count")
        sum_agg = Aggregate(AggregationType.SUM, sum_expr, "sum")
        avg_agg = Aggregate(AggregationType.AVG, avg_expr, "avg")
        min_agg = Aggregate(AggregationType.MIN, min_expr, "min")
        max_agg = Aggregate(AggregationType.MAX, max_expr, "max")
        plan = AggregationPlan(
            [self.group_by_expr],
            [count_agg, sum_agg, avg_agg, min_agg, max_agg],
            schema,
            Mock(),
        )
        executor = AggregationExecutor(plan, self.child_executor)
        self.child_executor.next.side_effect = [
            Tuple(
                [
                    Value(TypeEnum.STRING, "group1"),
                    Value.create_null_from_type_id(TypeEnum.DECIMAL),
                ],
                self.schema,
            ),
            None,
        ]
        executor.init()
        result = executor.next()
        assert result is not None
        assert result.values[0].compare_equals(
            Value(TypeEnum.STRING, "group1")
        )
        assert result.values[1].compare_equals(Value(TypeEnum.INT, 0))  # COUNT
        assert result.values[2].compare_equals(
            Value.create_null_from_type_id(TypeEnum.DECIMAL)
        )  # SUM
        assert result.values[3].compare_equals(
            Value.create_null_from_type_id(TypeEnum.DECIMAL)
        )  # AVG
        assert result.values[4].compare_equals(
            Value.create_null_from_type_id(TypeEnum.DECIMAL)
        )  # MIN
        assert result.values[5].compare_equals(
            Value.create_null_from_type_id(TypeEnum.DECIMAL)
        )  # MAX
