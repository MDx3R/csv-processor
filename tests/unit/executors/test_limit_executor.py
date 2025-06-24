from unittest.mock import Mock

import pytest

from engine.execution.executors.executor import Executor
from engine.execution.executors.limit_executor import LimitExecutor
from engine.execution.plan.limit_plan import LimitPlan
from storage.schema import Column, Schema
from storage.tuple import Tuple
from type.type_enum import TypeEnum
from type.value import Value


class TestLimitExecutor:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.schema = Schema([Column("col1", TypeEnum.INT)])
        self.tuple1 = Tuple([Value(TypeEnum.INT, 1)], self.schema)
        self.tuple2 = Tuple([Value(TypeEnum.INT, 2)], self.schema)
        self.child_executor = Mock(spec=Executor)
        self.child_executor.next.side_effect = [self.tuple1, self.tuple2, None]
        self.plan = LimitPlan(2, self.schema, Mock())
        self.executor = LimitExecutor(self.plan, self.child_executor)
        yield

    def test_init_resets_count(self):
        self.executor._count = 5  # type: ignore
        self.executor.init()
        self.child_executor.init.assert_called_once()
        assert self.executor._count == 0  # type: ignore

    def test_next_respects_limit(self):
        self.executor.init()
        result1 = self.executor.next()
        result2 = self.executor.next()
        result3 = self.executor.next()
        assert result1 == self.tuple1
        assert result2 == self.tuple2
        assert result3 is None
        assert self.executor._count == 2  # type: ignore

    def test_next_zero_limit(self):
        self.plan = LimitPlan(0, self.schema, Mock())
        self.executor = LimitExecutor(self.plan, self.child_executor)
        self.executor.init()

        assert self.executor.next() is None
        self.child_executor.next.assert_not_called()

    def test_next_empty_input(self):
        self.child_executor.next.side_effect = [None]
        self.child_executor.next.return_value = None
        self.executor.init()
        assert self.executor.next() is None
