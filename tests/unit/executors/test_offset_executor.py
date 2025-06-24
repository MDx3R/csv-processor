from unittest.mock import Mock

import pytest

from engine.execution.executors.executor import Executor
from engine.execution.executors.offset_executor import OffsetExecutor
from engine.execution.plan.offset_plan import OffsetPlan
from storage.schema import Column, Schema
from storage.tuple import Tuple
from type.type_enum import TypeEnum
from type.value import Value


class TestOffsetExecutor:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.schema = Schema([Column("col1", TypeEnum.INT)])
        self.tuple1 = Tuple([Value(TypeEnum.INT, 1)], self.schema)
        self.tuple2 = Tuple([Value(TypeEnum.INT, 2)], self.schema)
        self.tuple3 = Tuple([Value(TypeEnum.INT, 3)], self.schema)
        self.child_executor = Mock(spec=Executor)
        self.child_executor.next.side_effect = [
            self.tuple1,
            self.tuple2,
            self.tuple3,
            None,
        ]
        self.plan = OffsetPlan(1, self.schema, Mock())
        self.executor = OffsetExecutor(self.plan, self.child_executor)
        yield

    def test_init_resets_count(self):
        self.executor._count = 5  # type: ignore
        self.executor.init()
        self.child_executor.init.assert_called_once()
        assert self.executor._count == 0  # type: ignore

    def test_next_skips_offset(self):
        self.executor.init()
        result1 = self.executor.next()
        result2 = self.executor.next()
        result3 = self.executor.next()
        assert result1 == self.tuple2  # Skips tuple1
        assert result2 == self.tuple3
        assert result3 is None
        assert self.executor._count == 1  # type: ignore

    def test_next_offset_exceeds_input(self):
        self.plan = OffsetPlan(5, self.schema, Mock())
        self.executor = OffsetExecutor(self.plan, self.child_executor)
        self.executor.init()
        assert self.executor.next() is None
        assert self.executor._count == 3  # type: ignore

    def test_next_empty_input(self):
        self.child_executor.next.side_effect = [None]
        self.child_executor.next.return_value = None
        self.executor.init()
        assert self.executor.next() is None
