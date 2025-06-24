from unittest.mock import Mock

import pytest

from engine.execution.executors.executor import Executor
from engine.execution.executors.sort_executor import SortExecutor
from engine.execution.expressions.column_expression import ColumnExpression
from engine.execution.plan.sort_plan import SortPlan
from storage.schema import Column, Schema
from storage.tuple import Tuple
from type.type_enum import TypeEnum
from type.value import Value


class TestSortExecutor:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.column1 = Column("col1", TypeEnum.INT)
        self.schema = Schema([self.column1])
        self.expr = ColumnExpression(self.column1)
        self.tuple1 = Tuple([Value(TypeEnum.INT, 2)], self.schema)
        self.tuple2 = Tuple([Value(TypeEnum.INT, 1)], self.schema)
        self.tuple3 = Tuple([Value(TypeEnum.INT, 3)], self.schema)
        self.child_executor = Mock(spec=Executor)
        self.child_executor.next.side_effect = [
            self.tuple1,
            self.tuple2,
            self.tuple3,
            None,
        ]
        self.plan = SortPlan([self.expr], self.schema, Mock())
        self.executor = SortExecutor(self.plan, self.child_executor)

    def test_init_collects_tuples(self):
        self.executor.init()
        self.child_executor.init.assert_called_once()
        result = self.executor._sorted  # type: ignore
        assert result
        assert len(result) == 3
        assert result[0] == self.tuple2
        assert result[1] == self.tuple1
        assert result[2] == self.tuple3
        assert self.executor._idx == 0  # type: ignore

    def test_next_without_init_raises(self):
        with pytest.raises(RuntimeError):
            self.executor.next()

    def test_next_yields_sorted_tuples(self):
        self.executor.init()
        result1 = self.executor.next()
        result2 = self.executor.next()
        result3 = self.executor.next()
        result4 = self.executor.next()
        assert result1 == self.tuple2  # Value 1
        assert result2 == self.tuple1  # Value 2
        assert result3 == self.tuple3  # Value 3
        assert result4 is None

    def test_next_empty_input(self):
        self.child_executor.next.side_effect = [None]
        self.child_executor.next.return_value = None
        self.executor.init()
        assert self.executor.next() is None
