from dataclasses import dataclass

from engine.execution.aggregate import (
    Aggregate,
    AggregationType,
    get_start_count_expr,
)
from engine.execution.expressions.column_expression import ColumnExpression
from engine.execution.expressions.expression import Expression
from engine.execution.plan.aggregation_plan import AggregationPlan
from engine.execution.plan.execution_plan import ExecutionPlan
from engine.execution.plan.filter_plan import FilterPlan
from engine.execution.plan.projection_plan import ProjectionPlan
from engine.execution.plan.scan_plan import ScanPlan
from storage.schema import Column, Schema
from storage.table import Table


@dataclass(frozen=True)
class AggregateDef:
    type: AggregationType
    column: Expression | None  # None stands for COUNT(*)
    output_name: str


@dataclass(frozen=True)
class SelectStatement:
    select_expressions: list[Expression]
    from_table: str
    group_bys: list[Expression]
    aggregates: list[AggregateDef]
    where_clause: Expression | None = None


class SelectStatementValidator:
    def validate(self, stmt: SelectStatement) -> None:
        if not stmt.from_table:
            raise ValueError("FROM clause is required")

        has_group_by = bool(stmt.group_bys)
        has_aggregates = bool(stmt.aggregates)

        if has_group_by:
            self._validate_grouped_select(
                stmt.select_expressions, stmt.group_bys, stmt.aggregates
            )

        elif has_aggregates:
            if not self._is_full_table_aggregate(
                stmt.select_expressions, stmt.aggregates
            ):
                raise ValueError(
                    "With aggregates and no GROUP BY, SELECT must only contain aggregate expressions"
                )

    def _validate_grouped_select(
        self,
        select_exprs: list[Expression],
        group_bys: list[Expression],
        aggregates: list[AggregateDef],
    ) -> None:
        allowed_exprs: set[Expression] = set()
        if group_bys:
            allowed_exprs.update(e for e in group_bys)
        if aggregates:
            for agg in aggregates:
                if agg.column:
                    allowed_exprs.add(agg.column)
                else:
                    allowed_exprs.add(get_start_count_expr())

        for expr in select_exprs:
            if expr not in allowed_exprs:
                raise ValueError(
                    f"Expression {expr} in SELECT is not a group key or aggregate"
                )

    def _is_full_table_aggregate(
        self,
        select_exprs: list[Expression],
        aggregates: list[AggregateDef],
    ) -> bool:
        allowed_exprs: list[Expression] = []
        for agg in aggregates:
            if agg.column:
                allowed_exprs.append(agg.column)
            else:
                allowed_exprs.append(get_start_count_expr())

        return all(expr in allowed_exprs for expr in select_exprs)


class QueryPlanner:
    def __init__(
        self,
        catalog: dict[str, Table],
        validator: SelectStatementValidator | None = None,
    ):
        self._catalog = catalog
        self._validator = validator or SelectStatementValidator()

    def create_plan(self, statement: SelectStatement) -> ExecutionPlan:
        self._validator.validate(statement)

        table = self._catalog.get(statement.from_table)
        assert table
        table_schema = table.get_schema()

        plan = ScanPlan(table, table_schema)

        if statement.where_clause:
            plan = FilterPlan(statement.where_clause, table_schema, plan)

        if statement.group_bys or statement.aggregates:
            plan = self._build_aggregation_plan(statement, plan, table_schema)

        output_schema = self._construct_output_schema(statement, table_schema)

        return self._build_projection_plan(output_schema, plan)

    def _build_aggregation_plan(
        self,
        statement: SelectStatement,
        child_plan: ExecutionPlan,
        table_schema: Schema,
    ) -> ExecutionPlan:
        output_schema = self._construct_output_schema(statement, table_schema)
        group_bys = statement.group_bys
        aggregates = self._construct_aggregates(statement.aggregates)
        return AggregationPlan(
            group_bys=group_bys,
            aggregates=aggregates,
            output_schema=output_schema,
            child=child_plan,
        )

    def _construct_aggregates(
        self, aggregates: list[AggregateDef]
    ) -> list[Aggregate]:
        return [
            Aggregate(
                i.type,
                (i.column if i.column else get_start_count_expr()),
                i.output_name,
            )
            for i in aggregates
        ]

    def _build_projection_plan(
        self, output_schema: Schema, child_plan: ExecutionPlan
    ) -> ExecutionPlan:
        projection_expressions = self._build_projection_expressions(
            output_schema
        )
        return ProjectionPlan(
            projection_expressions, output_schema, child_plan
        )

    def _build_projection_expressions(
        self, output_schema: Schema
    ) -> list[Expression]:
        result: list[Expression] = []
        for col in output_schema.get_columns():
            expr = ColumnExpression(col)
            result.append(expr)
        return result

    def _construct_output_schema(
        self, statement: SelectStatement, table_schema: Schema
    ) -> Schema:
        columns: list[Column] = []
        for expr in statement.select_expressions:
            name: str | None = None
            type_id = expr.get_return_type()

            # Is expr an aggregate
            for agg in statement.aggregates:
                if agg.column == expr or agg.column is None:
                    name = agg.output_name
                    break

            # Is else (for example, group by)
            if name is None:
                name = expr.to_string()

            columns.append(Column(name, type_id))

        return Schema(columns) if columns else table_schema
