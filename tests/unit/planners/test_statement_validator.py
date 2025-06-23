from unittest.mock import Mock

import pytest

from engine.execution.aggregate import AggregationType
from engine.execution.expressions.constant_expression import ConstantExpression
from engine.execution.expressions.expression import Expression
from engine.execution.planner import (
    AggregateDef,
    SelectStatement,
    SelectStatementValidator,
)
from type.value import Value


class TestSelectStatementValidator:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.expr1 = Mock(spec=Expression)
        self.expr2 = Mock(spec=Expression)
        self.where_expr = Mock(spec=Expression)
        self.group_by_expr = Mock(spec=Expression)
        self.aggregate_expr = Mock(spec=Expression)
        self.count_star_expr = ConstantExpression(Value.create_int(1))
        self.aggregate_def = AggregateDef(
            AggregationType.COUNT, self.aggregate_expr, "count"
        )
        self.count_star_aggregate_def = AggregateDef(
            AggregationType.COUNT, None, "count"
        )
        self.statement = SelectStatement(
            select_expressions=[self.expr1],
            from_table="test_table",
            group_bys=[],
            aggregates=[],
            where_clause=None,
        )
        self.validator = SelectStatementValidator()

    def test_validate_empty_from_table(self):
        statement = SelectStatement(
            select_expressions=[self.expr1],
            from_table="",
            group_bys=[],
            aggregates=[],
        )
        with pytest.raises(ValueError, match="FROM clause is required"):
            self.validator.validate(statement)

    def test_validate_basic_select(self):
        self.validator.validate(self.statement)  # Should pass

    def test_validate_with_where(self):
        statement = SelectStatement(
            select_expressions=[self.expr1],
            from_table="test_table",
            where_clause=self.where_expr,
            group_bys=[],
            aggregates=[],
        )
        self.validator.validate(statement)  # Should pass

    def test_validate_with_group_by_valid(self):
        statement = SelectStatement(
            select_expressions=[self.group_by_expr],
            from_table="test_table",
            group_bys=[self.group_by_expr],
            aggregates=[],
        )
        self.validator.validate(statement)  # Should pass

    def test_validate_with_group_by_invalid(self):
        statement = SelectStatement(
            select_expressions=[self.expr1],  # Not in group_bys or aggregates
            from_table="test_table",
            group_bys=[self.group_by_expr],
            aggregates=[],
        )
        with pytest.raises(
            ValueError, match="Expression.*is not a group key or aggregate"
        ):
            self.validator.validate(statement)

    def test_validate_with_aggregates_no_group_by_valid(self):
        statement = SelectStatement(
            select_expressions=[self.aggregate_expr],
            from_table="test_table",
            group_bys=[],
            aggregates=[self.aggregate_def],
        )
        self.validator.validate(statement)  # Should pass

    def test_validate_with_aggregates_no_group_by_invalid(self):
        statement = SelectStatement(
            select_expressions=[self.expr1],  # Not an aggregate expression
            from_table="test_table",
            group_bys=[],
            aggregates=[self.aggregate_def],
        )
        with pytest.raises(
            ValueError,
            match="With aggregates and no GROUP BY, SELECT must only contain aggregate expressions",
        ):
            self.validator.validate(statement)

    def test_validate_with_count_star(self):
        statement = SelectStatement(
            select_expressions=[self.count_star_expr],
            from_table="test_table",
            group_bys=[],
            aggregates=[self.count_star_aggregate_def],
        )
        self.validator.validate(statement)  # Should pass

    def test_validate_group_by_and_aggregates(self):
        statement = SelectStatement(
            select_expressions=[self.group_by_expr, self.aggregate_expr],
            from_table="test_table",
            group_bys=[self.group_by_expr],
            aggregates=[self.aggregate_def],
        )
        self.validator.validate(statement)
