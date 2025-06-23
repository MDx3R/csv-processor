from unittest.mock import Mock

import pytest

from engine.execution.executors.executor import Executor
from engine.execution.executors.projection_executor import ProjectionExecutor
from engine.execution.expressions.expression import Expression
from engine.execution.plan.projection_plan import ProjectionPlan
from storage.schema import Column, Schema
from storage.tuple import Tuple
from type.type_enum import TypeEnum
from type.value import Value


class TestProjectionExecutor:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.output_schema = Schema(
            [Column("out1", TypeEnum.STRING), Column("out2", TypeEnum.INT)]
        )
        self.expr1 = Mock(spec=Expression)
        self.expr2 = Mock(spec=Expression)
        self.expr1.evaluate.return_value = Value(
            TypeEnum.STRING, "projected_value"
        )
        self.expr2.evaluate.return_value = Value(TypeEnum.INT, 42)
        self.child_schema = Schema(
            [Column("out1", TypeEnum.STRING), Column("out2", TypeEnum.INT)]
        )
        self.input_tuple = Tuple(
            [Value(TypeEnum.STRING, "123"), Value(TypeEnum.INT, 100)],
            self.child_schema,
        )
        self.child_executor = Mock(spec=Executor)
        self.child_executor.next.return_value = self.input_tuple
        self.plan = ProjectionPlan(
            [self.expr1, self.expr2], self.output_schema, Mock()
        )
        self.executor = ProjectionExecutor(self.plan, self.child_executor)

    def test_init_calls_child_init(self):
        self.executor.init()
        self.child_executor.init.assert_called_once()

    def test_next_projects_tuple(self):
        result = self.executor.next()
        assert isinstance(result, Tuple)
        assert result.schema == self.output_schema
        assert len(result.values) == 2
        assert result.values[0].compare_equals(
            Value(TypeEnum.STRING, "projected_value")
        )
        assert result.values[1].compare_equals(Value(TypeEnum.INT, 42))

        self.expr1.evaluate.assert_called_once_with(self.input_tuple)
        self.expr2.evaluate.assert_called_once_with(self.input_tuple)

    def test_next_handles_null_input(self):
        self.child_executor.next.return_value = None
        self.executor.init()
        result = self.executor.next()
        assert result is None
        self.expr1.evaluate.assert_not_called()
        self.expr2.evaluate.assert_not_called()

    def test_next_handles_exhausted_child(self):
        self.child_executor.next.side_effect = StopIteration
        self.executor.init()
        result = self.executor.next()
        assert result is None
        self.expr1.evaluate.assert_not_called()
        self.expr2.evaluate.assert_not_called()

    def test_next_multiple_tuples(self):
        tuple2 = Tuple(
            [Value(TypeEnum.STRING, "123"), Value(TypeEnum.INT, 100)],
            self.child_schema,
        )
        self.child_executor.next.side_effect = [self.input_tuple, tuple2, None]
        self.expr1.evaluate.side_effect = [
            Value(TypeEnum.STRING, "value1"),
            Value(TypeEnum.STRING, "value2"),
        ]
        self.expr2.evaluate.side_effect = [
            Value(TypeEnum.INT, 42),
            Value(TypeEnum.INT, 84),
        ]
        self.executor.init()
        result1 = self.executor.next()
        result2 = self.executor.next()
        result3 = self.executor.next()
        assert result1
        assert result1.values[0].compare_equals(
            Value(TypeEnum.STRING, "value1")
        )
        assert result1.values[1].compare_equals(Value(TypeEnum.INT, 42))
        assert result2
        assert result2.values[0].compare_equals(
            Value(TypeEnum.STRING, "value2")
        )
        assert result2.values[1].compare_equals(Value(TypeEnum.INT, 84))
        assert result3 is None
