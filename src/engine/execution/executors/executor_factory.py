from engine.execution.executors.aggregation_executor import AggregationExecutor
from engine.execution.executors.executor import Executor
from engine.execution.executors.filter_executor import FilterExecutor
from engine.execution.executors.projection_executor import ProjectionExecutor
from engine.execution.executors.scan_executor import ScanExecutor
from engine.execution.plan.aggregation_plan import AggregationPlan
from engine.execution.plan.execution_plan import ExecutionPlan
from engine.execution.plan.filter_plan import FilterPlan
from engine.execution.plan.projection_plan import ProjectionPlan
from engine.execution.plan.scan_plan import ScanPlan
from storage.reader import TableReaderFactory
from storage.table import Table


class ExecutorFactory:
    def __init__(self, catalog: dict[str, Table]):
        self._catalog = catalog

    def create_executor(self, plan: ExecutionPlan) -> Executor:
        if isinstance(plan, ScanPlan):
            return self._create_scan_executor(plan)
        elif isinstance(plan, FilterPlan):
            child_executor = self.create_executor(plan.get_child())
            return FilterExecutor(plan, child_executor)
        elif isinstance(plan, AggregationPlan):
            child_executor = self.create_executor(plan.get_child())
            return AggregationExecutor(
                plan=plan,
                child=child_executor,
            )
        elif isinstance(plan, ProjectionPlan):
            child_executor = self.create_executor(plan.get_child())
            return ProjectionExecutor(
                plan=plan,
                child=child_executor,
            )
        else:
            raise NotImplementedError(
                f"Executor for {type(plan)} not implemented"
            )

    def _create_scan_executor(self, plan: ScanPlan) -> ScanExecutor:
        return ScanExecutor(
            plan, TableReaderFactory.create_reader(plan.get_table())
        )
