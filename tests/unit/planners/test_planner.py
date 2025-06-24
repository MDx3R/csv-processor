from unittest.mock import Mock

import pytest

from engine.execution.aggregate import AggregationType
from engine.execution.expressions.column_expression import ColumnExpression
from engine.execution.expressions.constant_expression import ConstantExpression
from engine.execution.expressions.expression import Expression
from engine.execution.plan.aggregation_plan import AggregationPlan
from engine.execution.plan.filter_plan import FilterPlan
from engine.execution.plan.limit_plan import LimitPlan
from engine.execution.plan.offset_plan import OffsetPlan
from engine.execution.plan.projection_plan import ProjectionPlan
from engine.execution.plan.scan_plan import ScanPlan
from engine.execution.plan.sort_plan import SortPlan
from engine.execution.planner import (
    AggregateDef,
    QueryPlanner,
    SelectStatement,
)
from storage.schema import Column, Schema
from storage.table import Table
from type.type_enum import TypeEnum
from type.value import Value


class TestQueryPlanner:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.table = Mock(spec=Table)
        self.table_schema = Schema(
            [Column("col1", TypeEnum.STRING), Column("col2", TypeEnum.INT)]
        )
        self.table.get_schema.return_value = self.table_schema
        self.catalog: dict[str, Table] = {"table1": self.table}
        self.planner = QueryPlanner(self.catalog)
        self.planner._validator.validate = Mock()  # type: ignore
        self.select_expr1 = Mock(spec=Expression)
        self.select_expr1.to_string.return_value = "col1"
        self.select_expr1.get_return_type.return_value = TypeEnum.STRING
        self.select_expr2 = Mock(spec=Expression)
        self.select_expr2.to_string.return_value = "col2"
        self.select_expr2.get_return_type.return_value = TypeEnum.INT
        self.where_clause = Mock(spec=Expression)
        self.group_by_expr = Mock(spec=Expression)
        self.order_by_expr = Mock(spec=Expression)
        self.aggregate_expr = Mock(spec=Expression)
        self.aggregate_def = AggregateDef(
            AggregationType.COUNT, self.aggregate_expr, "count"
        )
        self.statement = SelectStatement(
            select_expressions=[self.select_expr1, self.select_expr2],
            from_table="table1",
            group_bys=[],
            aggregates=[],
        )

    def test_create_plan_basic_select(self):
        plan = self.planner.create_plan(self.statement)

        assert isinstance(plan, ProjectionPlan)

        child = plan.get_child()
        assert isinstance(child, ScanPlan)
        assert child.get_table() == self.table
        assert child.get_output_schema() == self.table_schema

        assert len(plan.get_expressions()) == 2
        assert isinstance(plan.get_expressions()[0], ColumnExpression)
        assert isinstance(plan.get_expressions()[1], ColumnExpression)

        output_schema = plan.get_output_schema()
        assert len(output_schema.get_columns()) == 2
        assert output_schema.get_column(0).name == "col1"
        assert output_schema.get_column(0).get_type_id() == TypeEnum.STRING
        assert output_schema.get_column(1).name == "col2"
        assert output_schema.get_column(1).get_type_id() == TypeEnum.INT

    def test_create_plan_with_where_clause(self):
        statement = SelectStatement(
            select_expressions=[self.select_expr1],
            from_table="table1",
            group_bys=[],
            aggregates=[],
            where_clause=self.where_clause,
        )
        plan = self.planner.create_plan(statement)
        assert isinstance(plan, ProjectionPlan)

        child = plan.get_child()
        assert isinstance(child, FilterPlan)
        predicate = child.get_predicate()
        assert predicate == self.where_clause

        grandchild = child.get_child()
        assert isinstance(grandchild, ScanPlan)
        table = grandchild.get_table()
        schema = grandchild.get_output_schema()
        assert table == self.table
        assert schema == self.table_schema

    def test_create_plan_with_aggregation(self):
        statement = SelectStatement(
            select_expressions=[self.select_expr1],
            from_table="table1",
            group_bys=[],
            aggregates=[self.aggregate_def],
        )
        plan = self.planner.create_plan(statement)
        assert isinstance(plan, ProjectionPlan)

        child = plan.get_child()
        assert isinstance(child, AggregationPlan)

        aggregates = child.get_aggregates()
        assert aggregates[0].type == AggregationType.COUNT
        assert aggregates[0].expr == self.aggregate_expr
        assert aggregates[0].output_name == "count"

        group_bys = child.get_group_bys()
        assert group_bys == []

        scan = child.get_child()
        assert isinstance(scan, ScanPlan)
        assert scan.get_table() == self.table

    def test_create_plan_with_group_by(self):
        statement = SelectStatement(
            select_expressions=[self.select_expr1],
            from_table="table1",
            group_bys=[self.group_by_expr],
            aggregates=[],
        )

        plan = self.planner.create_plan(statement)
        assert isinstance(plan, ProjectionPlan)

        child = plan.get_child()
        assert isinstance(child, AggregationPlan)

        group_bys = child.get_group_bys()
        assert group_bys == [self.group_by_expr]

        aggregates = child.get_aggregates()
        assert aggregates == []

        scan = child.get_child()
        assert isinstance(scan, ScanPlan)
        assert scan.get_table() == self.table

    def test_create_plan_with_count_star(self):
        aggregate_def = AggregateDef(AggregationType.COUNT, None, "count")
        statement = SelectStatement(
            select_expressions=[self.select_expr1],
            from_table="table1",
            group_bys=[],
            aggregates=[aggregate_def],
        )

        plan = self.planner.create_plan(statement)
        assert isinstance(plan, ProjectionPlan)

        child = plan.get_child()
        assert isinstance(child, AggregationPlan)

        aggregates = child.get_aggregates()
        aggregate = aggregates[0]

        assert aggregate.type == AggregationType.COUNT
        assert isinstance(aggregate.expr, ConstantExpression)

        value = aggregate.expr.evaluate(Mock())
        assert value.compare_equals(Value(TypeEnum.INT, 1))
        assert aggregate.output_name == "count"

    def test_create_plan_with_sort(self):
        statement = SelectStatement(
            select_expressions=[self.select_expr1],
            from_table="table1",
            group_bys=[],
            aggregates=[],
            order_by=[self.order_by_expr],
        )
        plan = self.planner.create_plan(statement)
        assert isinstance(plan, SortPlan)
        assert plan.get_order_by() == [self.order_by_expr]

        child = plan.get_child()
        assert isinstance(child, ProjectionPlan)

        grandchild = child.get_child()
        assert isinstance(grandchild, ScanPlan)
        assert grandchild.get_table() == self.table

    def test_create_plan_with_limit(self):
        statement = SelectStatement(
            select_expressions=[self.select_expr1],
            from_table="table1",
            group_bys=[],
            aggregates=[],
            limit=10,
        )
        plan = self.planner.create_plan(statement)
        assert isinstance(plan, LimitPlan)
        assert plan.get_limit() == 10

        child = plan.get_child()
        assert isinstance(child, ProjectionPlan)

        grandchild = child.get_child()
        assert isinstance(grandchild, ScanPlan)
        assert grandchild.get_table() == self.table

    def test_create_plan_with_offset(self):
        statement = SelectStatement(
            select_expressions=[self.select_expr1],
            from_table="table1",
            group_bys=[],
            aggregates=[],
            offset=5,
        )
        plan = self.planner.create_plan(statement)
        assert isinstance(plan, OffsetPlan)
        assert plan.get_offset() == 5

        child = plan.get_child()
        assert isinstance(child, ProjectionPlan)

        grandchild = child.get_child()
        assert isinstance(grandchild, ScanPlan)
        assert grandchild.get_table() == self.table

    def test_create_plan_missing_table(self):
        statement = SelectStatement(
            select_expressions=[self.select_expr1],
            from_table="missing_table",
            group_bys=[],
            aggregates=[],
        )
        with pytest.raises(AssertionError):
            self.planner.create_plan(statement)

    def test_construct_aggregates_count_star(self):
        aggregate_def = AggregateDef(AggregationType.COUNT, None, "count")
        aggregates = self.planner._construct_aggregates([aggregate_def])  # type: ignore
        assert len(aggregates) == 1
        aggregate = aggregates[0]
        assert aggregate.type == AggregationType.COUNT
        assert isinstance(aggregate.expr, ConstantExpression)
        value = aggregate.expr.evaluate(Mock())
        assert value.compare_equals(Value(TypeEnum.INT, 1))
        assert aggregate.output_name == "count"

    def test_construct_aggregates_empty(self):
        aggregates = self.planner._construct_aggregates([])  # type: ignore
        assert aggregates == []
