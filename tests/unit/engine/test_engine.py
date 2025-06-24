from unittest.mock import Mock

import pytest

from engine.engine import Engine
from engine.execution.executors.executor import Executor
from engine.execution.executors.executor_factory import ExecutorFactory
from engine.execution.expressions.column_expression import ColumnExpression
from engine.execution.plan.execution_plan import ExecutionPlan
from engine.execution.plan.projection_plan import ProjectionPlan
from engine.execution.plan.scan_plan import ScanPlan
from engine.execution.plan.sort_plan import SortPlan
from storage.schema import Column, Schema
from storage.table import Table
from storage.tuple import Tuple
from type.type_enum import TypeEnum
from type.value import Value


class TestEngine:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.column1 = Column("col1", TypeEnum.INT)
        self.schema = Schema([self.column1])
        self.tuple1 = Tuple([Value(TypeEnum.INT, 1)], self.schema)
        self.tuple2 = Tuple([Value(TypeEnum.INT, 2)], self.schema)
        self.executor = Mock(spec=Executor)
        self.executor.next.side_effect = [self.tuple1, self.tuple2, None]
        self.executor_factory = Mock(spec=ExecutorFactory)
        self.executor_factory.create_executor.return_value = self.executor
        self.plan = Mock(spec=ExecutionPlan)
        self.engine = Engine(self.plan, self.executor_factory)

    def test_run_collects_tuples(self):
        result = self.engine.run()
        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0] == self.tuple1
        assert result[1] == self.tuple2
        self.executor.init.assert_called_once()
        assert self.executor.next.call_count == 3

    def test_run_empty_result(self):
        self.executor.next.side_effect = [None]
        self.executor.next.return_value = None
        result = self.engine.run()
        assert isinstance(result, list)
        assert len(result) == 0

        self.executor.init.assert_called_once()
        self.executor.next.assert_called_once()

    def test_run_with_complex_plan(self):
        table = Mock(spec=Table)
        table.get_schema.return_value = self.schema
        scan_plan = ScanPlan(table, self.schema)
        sort_expr = ColumnExpression(self.column1)
        sort_plan = SortPlan([sort_expr], self.schema, scan_plan)
        proj_expr = ColumnExpression(self.column1)
        output_schema = Schema([Column("out1", TypeEnum.INT)])
        proj_plan = ProjectionPlan([proj_expr], output_schema, sort_plan)

        # Set up executor chain
        scan_executor = Mock(spec=Executor)
        scan_executor.next.side_effect = [
            self.tuple2,
            self.tuple1,
            None,
        ]  # Unsorted order
        sort_executor = Mock(spec=Executor)
        sort_executor.next.side_effect = [
            self.tuple1,
            self.tuple2,
            None,
        ]  # Sorted order
        proj_executor = Mock(spec=Executor)
        proj_executor.next.side_effect = [
            Tuple([Value(TypeEnum.INT, 1)], output_schema),
            Tuple([Value(TypeEnum.INT, 2)], output_schema),
            None,
        ]

        def create_executor(plan: ExecutionPlan):
            if plan == proj_plan:
                return proj_executor
            if plan == sort_plan:
                return sort_executor
            if plan == scan_plan:
                return scan_executor
            return Mock(spec=Executor)

        self.executor_factory.create_executor = create_executor
        self.engine = Engine(proj_plan, self.executor_factory)

        result = self.engine.run()
        assert len(result) == 2
        assert result[0].values[0].compare_equals(Value(TypeEnum.INT, 1))
        assert result[1].values[0].compare_equals(Value(TypeEnum.INT, 2))
        assert result[0].schema == output_schema
        assert result[1].schema == output_schema
        proj_executor.init.assert_called_once()
        assert proj_executor.next.call_count == 3
