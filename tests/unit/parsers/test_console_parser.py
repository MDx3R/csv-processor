import pytest

from engine.execution.aggregate import AggregationType
from engine.execution.expressions.column_expression import ColumnExpression
from engine.execution.expressions.comparison_expression import (
    ComparisonExpression,
)
from engine.execution.expressions.constant_expression import ConstantExpression
from engine.execution.expressions.expression import Expression
from engine.execution.parser import (
    ConsoleSelectParser,
    ExpressionResolver,
    QueryConfig,
)
from engine.execution.planner import AggregateDef, SelectStatement
from storage.schema import Column, Schema
from storage.table import StringTable, Table
from type.type_enum import TypeEnum
from type.value import Value


class TestConsoleSelectParser:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.schema = Schema(
            [Column("col1", TypeEnum.STRING), Column("col2", TypeEnum.INT)]
        )
        self.table_registry: dict[str, Table] = {
            "table1": StringTable("", self.schema)
        }
        self.resolver = ExpressionResolver(self.table_registry)
        self.column1 = Column("col1", TypeEnum.STRING)
        self.column2 = Column("col2", TypeEnum.INT)
        self.col_expr1 = ColumnExpression(self.column1)
        self.col_expr2 = ColumnExpression(self.column2)
        self.parser = ConsoleSelectParser(self.resolver)
        self.count_star_expr = ConstantExpression(Value.create_int(1))

        self.args = [
            "--table=table1",
            "--where=col1>100",
            "--aggregate=col2=SUM",
            "--group-by=col1",
        ]
        yield

    def test_parse_config(self):
        config = self.parser.parse_config(self.args)
        assert isinstance(config, QueryConfig)
        assert config.table == "table1"
        assert config.where == "col1>100"
        assert config.aggregates == ["col2=SUM"]
        assert config.group_bys == ["col1"]

    def test_parse_config_missing_table(self):
        args = ["--where=col1>100"]  # no table
        with pytest.raises(SystemExit):  # argparse raises SystemExit on error
            self.parser.parse_config(args)

    def test_parse_basic(self):
        statement = self.parser.parse(self.args)

        assert isinstance(statement, SelectStatement)
        assert statement.from_table == "table1"
        assert isinstance(statement.where_clause, ComparisonExpression)
        assert statement.group_bys == [self.col_expr1]
        assert len(statement.aggregates) == 1

        aggregate = statement.aggregates[0]
        assert aggregate.type == AggregationType.SUM
        assert aggregate.column == self.col_expr2
        assert aggregate.output_name == "sum(col2)"
        assert statement.select_expressions == [self.col_expr1, self.col_expr2]

    def test_parse_no_where(self):
        args = ["--table=table1", "--aggregate=col2=COUNT", "--group-by=col1"]
        statement = self.parser.parse(args)
        assert statement.from_table == "table1"
        assert statement.where_clause is None
        assert statement.group_bys == [self.col_expr1]
        assert len(statement.aggregates) == 1

        aggregate = statement.aggregates[0]
        assert aggregate.type == AggregationType.COUNT
        assert aggregate.column == self.col_expr2
        assert aggregate.output_name == "count(col2)"
        assert statement.select_expressions == [self.col_expr1, self.col_expr2]

    def test_parse_with_sort_limit_offset(self):
        args = [
            "--table=table1",
            "--sort=col2",
            "--limit=10",
            "--offset=5",
        ]
        statement = self.parser.parse(args)
        assert statement.from_table == "table1"
        assert statement.where_clause is None
        assert statement.group_bys == []
        assert statement.aggregates == []
        assert statement.select_expressions == []
        assert statement.order_by == [self.col_expr2]
        assert statement.limit == 10
        assert statement.offset == 5

    def test_parse_config_with_sort_limit_offset(self):
        args = ["--table=table1", "--sort=col2"]
        config1 = self.parser.parse_config(args)
        args = ["--table=table1", "--order-by=col2"]
        config2 = self.parser.parse_config(args)
        assert config1.sort == config2.sort

    def test_parse_count_star(self):
        args = ["--table=table1", "--aggregate=*=COUNT"]
        statement = self.parser.parse(args)

        assert len(statement.aggregates) == 1

        aggregate = statement.aggregates[0]
        assert aggregate.type == AggregationType.COUNT
        assert aggregate.column is None
        assert aggregate.output_name == "count(*)"
        assert statement.select_expressions == [self.count_star_expr]

    def test_parse_no_group_by_no_aggregates(self):
        args = ["--table=table1"]
        statement = self.parser.parse(args)
        assert statement.from_table == "table1"
        assert statement.where_clause is None
        assert statement.group_bys == []
        assert statement.aggregates == []
        assert statement.select_expressions == []

    def test_parse_invalid_aggregate_format(self):
        args = ["--table=table1", "--aggregate=invalid_agg"]
        with pytest.raises(ValueError, match="Invalid aggregate format"):
            self.parser.parse(args)

    def test_parse_invalid_aggregate_type(self):
        args = ["--table=table1", "--aggregate=col2=INVALID"]
        with pytest.raises(
            ValueError
        ):  # Enums raise KeyError for invalid value
            self.parser.parse(args)

    def test_parse_aggregate(self):
        aggregate = self.parser._parse_aggregate("col2=SUM", "table1")  # type: ignore
        assert isinstance(aggregate, AggregateDef)
        assert aggregate.type == AggregationType.SUM
        assert aggregate.column == self.col_expr2
        assert aggregate.output_name == "sum(col2)"

    def test_parse_count_star_aggregate(self):
        aggregate = self.parser._parse_aggregate("*=COUNT", "table1")  # type: ignore
        assert aggregate.type == AggregationType.COUNT
        assert aggregate.column is None
        assert aggregate.output_name == "count(*)"

    def test_infer_select_expressions_group_by_and_aggregates(self):
        group_by_exprs: list[Expression] = [self.col_expr1]
        aggregates = [
            AggregateDef(AggregationType.SUM, self.col_expr2, "sum_col2")
        ]
        exprs = self.parser._infer_select_expressions(  # type: ignore
            group_by_exprs, aggregates
        )
        assert exprs == [self.col_expr1, self.col_expr2]

    def test_infer_select_expressions_count_star(self):
        aggregates = [AggregateDef(AggregationType.COUNT, None, "count_*")]
        exprs = self.parser._infer_select_expressions([], aggregates)  # type: ignore
        assert exprs == [self.count_star_expr]

    def test_infer_select_expressions_empty(self):
        exprs = self.parser._infer_select_expressions([], [])  # type: ignore
        assert exprs == []
