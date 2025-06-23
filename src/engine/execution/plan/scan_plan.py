from storage.schema import Schema
from storage.table import Table

from .execution_plan import ExecutionPlan


class ScanPlan(ExecutionPlan):
    def __init__(self, table: Table, output_schema: Schema) -> None:
        super().__init__(output_schema, [])
        self._table = table

    def get_table(self) -> Table:
        return self._table
