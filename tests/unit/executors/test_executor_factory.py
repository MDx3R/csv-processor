from unittest.mock import Mock

import pytest

from engine.execution.executors.aggregation_executor import AggregationExecutor
from engine.execution.executors.executor_factory import ExecutorFactory
from engine.execution.executors.filter_executor import FilterExecutor
from engine.execution.executors.projection_executor import ProjectionExecutor
from engine.execution.executors.scan_executor import ScanExecutor
from engine.execution.plan.aggregation_plan import AggregationPlan
from engine.execution.plan.execution_plan import ExecutionPlan
from engine.execution.plan.filter_plan import FilterPlan
from engine.execution.plan.projection_plan import ProjectionPlan
from engine.execution.plan.scan_plan import ScanPlan
from storage.reader import TableReader, TableReaderFactory
from storage.schema import Schema
from storage.table import StringTable, Table


class TestExecutorFactory:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.schema = Schema([])
        self.table = StringTable("", self.schema)
        self.catalog: dict[str, Table] = {"table1": self.table}
        self.factory = ExecutorFactory(self.catalog)
        self.reader = Mock(spec=TableReader)
        self.child_plan = ScanPlan(self.table, self.schema)

    def test_create_scan_executor(self):
        TableReaderFactory.create_reader = Mock(return_value=self.reader)
        scan_plan = ScanPlan(self.table, self.schema)

        executor = self.factory.create_executor(scan_plan)

        assert isinstance(executor, ScanExecutor)
        assert executor._plan == scan_plan  # type: ignore
        assert executor._reader == self.reader  # type: ignore
        TableReaderFactory.create_reader.assert_called_once_with(self.table)

    def test_create_filter_executor(self):
        filter_plan = FilterPlan(Mock(), self.schema, self.child_plan)
        executor = self.factory.create_executor(filter_plan)
        assert isinstance(executor, FilterExecutor)
        assert executor._plan == filter_plan  # type: ignore

    def test_create_aggregation_executor(self):
        aggregation_plan = AggregationPlan(
            [], [], self.schema, self.child_plan
        )
        executor = self.factory.create_executor(aggregation_plan)
        assert isinstance(executor, AggregationExecutor)
        assert executor._plan == aggregation_plan  # type: ignore

    def test_create_projection_executor(self):
        projection_plan = ProjectionPlan([], self.schema, self.child_plan)
        executor = self.factory.create_executor(projection_plan)
        assert isinstance(executor, ProjectionExecutor)
        assert executor._plan == projection_plan  # type: ignore

    def test_create_executor_unsupported_plan(self):
        unsupported_plan = Mock(spec=ExecutionPlan)
        with pytest.raises(
            NotImplementedError,
            match=f"Executor for {type(unsupported_plan)} not implemented",
        ):
            self.factory.create_executor(unsupported_plan)

    def test_recursive_executor_creation(self):
        child_plan = ScanPlan(self.table, self.schema)
        filter_plan = FilterPlan(Mock(), self.schema, child_plan)
        TableReaderFactory.create_reader = Mock(return_value=self.reader)

        executor = self.factory.create_executor(filter_plan)

        assert isinstance(executor, FilterExecutor)
        assert isinstance(executor._child, ScanExecutor)  # type: ignore
        assert executor._child._plan == child_plan  # type: ignore
        assert executor._child._reader == self.reader  # type: ignore
        TableReaderFactory.create_reader.assert_called_once_with(self.table)
