from unittest.mock import Mock

import pytest

from engine.execution.executors.executor import Executor
from engine.execution.executors.filter_executor import FilterExecutor
from engine.execution.expressions.expression import Expression
from engine.execution.plan.filter_plan import FilterPlan
from storage.schema import Schema
from storage.tuple import Tuple
from type.type_enum import TypeEnum
from type.value import Value


class TestFilterExecutor:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.schema = Schema([])
        self.predicate = Mock(spec=Expression)
        self.child_executor = Mock(spec=Executor)
        self.plan = FilterPlan(self.predicate, self.schema, Mock())
        self.executor = FilterExecutor(self.plan, self.child_executor)

    def test_init_calls_child_init(self):
        self.executor.init()
        self.child_executor.init.assert_called_once()

    def test_next_filters_tuples(self):
        tuple1, tuple2, tuple3 = (
            Tuple([], self.schema),
            Tuple([], self.schema),
            Tuple([], self.schema),
        )
        self.child_executor.next.side_effect = [tuple1, tuple2, tuple3, None]
        self.predicate.evaluate.side_effect = [
            Value(TypeEnum.BOOLEAN, False),
            Value(TypeEnum.BOOLEAN, True),
            Value(TypeEnum.BOOLEAN, None),
        ]

        self.executor.init()
        result = self.executor.next()

        assert result == tuple2
        assert self.predicate.evaluate.call_count == 2
        assert self.executor.next() is None
        assert self.predicate.evaluate.call_count == 3

    def test_next_handles_all_false_predicates(self):
        tuple1, tuple2 = Tuple([], self.schema), Tuple([], self.schema)
        self.child_executor.next.side_effect = [tuple1, tuple2, None]
        self.predicate.evaluate.side_effect = [
            Value(TypeEnum.BOOLEAN, False),
            Value(TypeEnum.BOOLEAN, False),
        ]

        self.executor.init()

        assert self.executor.next() is None
        assert self.predicate.evaluate.call_count == 2

    def test_next_handles_null_predicate(self):
        tuple1, tuple2 = Tuple([], self.schema), Tuple([], self.schema)
        self.child_executor.next.side_effect = [tuple1, tuple2, None]
        self.predicate.evaluate.side_effect = [
            Value(TypeEnum.BOOLEAN, None),
            Value(TypeEnum.BOOLEAN, True),
        ]

        self.executor.init()

        assert self.executor.next() == tuple2
        assert self.predicate.evaluate.call_count == 2

    def test_next_empty_child(self):
        self.child_executor.next.return_value = None

        self.executor.init()

        assert self.executor.next() is None
        self.predicate.evaluate.assert_not_called()
