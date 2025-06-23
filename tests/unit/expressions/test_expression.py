from unittest.mock import Mock

import pytest

from engine.execution.expressions.column_expression import ColumnExpression
from engine.execution.expressions.comparison_expression import (
    ComparisonExpression,
)
from engine.execution.expressions.constant_expression import ConstantExpression
from engine.execution.expressions.expression import Expression
from storage.schema import Column
from storage.tuple import Tuple
from type.enums import ComparisonOperandEnum
from type.type_enum import TypeEnum
from type.value import Value


class TestExpression:
    class MockExpression(Expression):
        def evaluate(self, tup: Tuple) -> Value:
            return Value.create_null_from_type_id(TypeEnum.BOOLEAN)

        def get_return_type(self) -> TypeEnum:
            return TypeEnum.BOOLEAN

        def __eq__(self, other: object) -> bool:
            return True

        def __hash__(self) -> int:
            return 1

        def to_string(self) -> str:
            return "Mock"

    @pytest.fixture(autouse=True)
    def setup(self):
        self.child1 = Mock(spec=Expression)
        self.child2 = Mock(spec=Expression)
        self.expression = TestExpression.MockExpression(
            [self.child1, self.child2]
        )

    def test_get_children_copy(self):
        children = self.expression.get_children()
        assert children == [self.child1, self.child2]
        assert children is not self.expression._children  # type: ignore


class TestColumnExpression:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.column = Column("test_col", TypeEnum.STRING)
        self.tuple = Mock(spec=Tuple)
        self.tuple.get_value_by_name.return_value = Value(
            TypeEnum.STRING, "test_value"
        )
        self.expression = ColumnExpression(self.column)

    def test_evaluate(self):
        result = self.expression.evaluate(self.tuple)
        assert result.compare_equals(Value(TypeEnum.STRING, "test_value"))
        self.tuple.get_value_by_name.assert_called_once_with("test_col")

    def test_get_return_type(self):
        assert self.expression.get_return_type() == TypeEnum.STRING


class TestComparisonExpression:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.left = Mock(spec=Expression)
        self.right = Mock(spec=Expression)

        self.op = ComparisonOperandEnum.EQ
        self.tuple = Mock(spec=Tuple)

        self.left.evaluate.return_value = Value(TypeEnum.INT, 42)
        self.right.evaluate.return_value = Value(TypeEnum.INT, 42)
        self.expression = ComparisonExpression(
            self.left, self.right, ComparisonOperandEnum.EQ
        )

    def test_evaluate_equal(self):
        result = self.expression.evaluate(self.tuple)

        assert result.compare_equals(Value(TypeEnum.BOOLEAN, True))
        self.left.evaluate.assert_called_once_with(self.tuple)
        self.right.evaluate.assert_called_once_with(self.tuple)

    def test_evaluate_not_equal(self):
        self.right.evaluate.return_value = Value(TypeEnum.INT, 43)
        self.expression = ComparisonExpression(
            self.left, self.right, ComparisonOperandEnum.EQ
        )
        result = self.expression.evaluate(self.tuple)

        assert result.compare_equals(Value(TypeEnum.BOOLEAN, False))

    def test_get_return_type(self):
        assert self.expression.get_return_type() == TypeEnum.BOOLEAN


class TestConstantExpression:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.value = Value(TypeEnum.INT, 100)
        self.tuple = Mock(spec=Tuple)
        self.expression = ConstantExpression(self.value)

    def test_evaluate(self):
        result = self.expression.evaluate(self.tuple)
        assert result.compare_equals(Value(TypeEnum.INT, 100))
        self.tuple.assert_not_called()

    def test_get_return_type(self):
        assert self.expression.get_return_type() == TypeEnum.INT
