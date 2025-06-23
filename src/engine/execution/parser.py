import argparse
from abc import ABC, abstractmethod
from dataclasses import dataclass

from engine.execution.aggregate import AggregationType, get_start_count_expr
from engine.execution.expressions.column_expression import ColumnExpression
from engine.execution.expressions.comparison_expression import (
    ComparisonExpression,
)
from engine.execution.expressions.constant_expression import ConstantExpression
from engine.execution.expressions.expression import Expression
from engine.execution.planner import (
    AggregateDef,
    SelectStatement,
)
from storage.schema import Column, Schema
from type.enums import ComparisonOperandEnum
from type.value import Value


class Parser(ABC):
    @abstractmethod
    def parse(self, args: list[str]) -> SelectStatement: ...


class ExpressionResolver:
    def __init__(self, schema: Schema) -> None:
        self.schema = schema

    def resolve_column(self, name: str) -> Column:
        for column in self.schema.columns:
            if column.name == name:
                return column
        raise ValueError(f"Column '{name}' not found in schema")

    def resolve_column_expression(self, name: str) -> ColumnExpression:
        return ColumnExpression(self.resolve_column(name))

    def resolve_comparison(self, condition: str) -> ComparisonExpression:
        operators = ["!=", ">=", "<=", "=", "<", ">"]
        for op in operators:
            if op in condition:
                left_str, right_str = condition.split(op, 1)
                left = self.resolve_column_expression(left_str.strip())
                right_val = self._parse_value(right_str.strip())
                right = ConstantExpression(right_val)

                return ComparisonExpression(
                    left,
                    right,
                    {
                        "=": ComparisonOperandEnum.EQ,
                        "!=": ComparisonOperandEnum.NEQ,
                        "<": ComparisonOperandEnum.LT,
                        "<=": ComparisonOperandEnum.LTE,
                        ">": ComparisonOperandEnum.GT,
                        ">=": ComparisonOperandEnum.GTE,
                    }[op],
                )
        raise ValueError(f"Invalid condition: {condition}")

    def _parse_value(self, raw: str) -> Value:
        if raw.lower() in ("true", "false"):
            return Value.create_boolean(raw.lower() == "true")
        if (raw.startswith("'") and raw.endswith("'")) or raw.isalpha():
            if raw.isalpha():
                return Value.create_string(raw)
            return Value.create_string(raw[1:-1])
        if "." in raw:
            return Value.create_decimal(float(raw))
        return Value.create_int(int(raw))


@dataclass(frozen=True)
class QueryConfig:
    table: str
    where: str | None
    aggregates: list[str]
    group_bys: list[str]


class ConsoleSelectParser(Parser):
    def __init__(self, resolver: ExpressionResolver) -> None:
        self.resolver = resolver

    def parse_config(self, args: list[str]) -> QueryConfig:
        parser = argparse.ArgumentParser()
        parser.add_argument("--table", required=True)
        parser.add_argument("--where")
        parser.add_argument("--aggregate", action="append", default=[])
        parser.add_argument("--group-by", action="append", default=[])

        parsed = parser.parse_args(args)

        return QueryConfig(
            table=parsed.table,
            where=parsed.where,
            aggregates=parsed.aggregate,
            group_bys=parsed.group_by,
        )

    def parse(self, args: list[str]) -> SelectStatement:
        config = self.parse_config(args)

        # WHERE
        where_expr = (
            self.resolver.resolve_comparison(config.where)
            if config.where
            else None
        )

        # GROUP BY
        group_by_exprs: list[Expression] | None = [
            self.resolver.resolve_column_expression(col)
            for col in config.group_bys
        ]

        # AGGREGATES
        aggregates = [self._parse_aggregate(s) for s in config.aggregates]

        # SELECT exprs
        select_exprs = self._infer_select_expressions(
            group_by_exprs, aggregates
        )

        return SelectStatement(
            select_expressions=select_exprs,
            from_table=config.table,
            where_clause=where_expr,
            group_bys=group_by_exprs,
            aggregates=aggregates,
        )

    def _parse_aggregate(self, s: str) -> AggregateDef:
        if "=" not in s:
            raise ValueError(
                f"Invalid aggregate format: '{s}' (expected column=FUNC)"
            )

        col_str, func_str = s.split("=", 1)
        agg_type = AggregationType(func_str.lower())

        column_expr = None
        if col_str != "*":
            column_expr = self.resolver.resolve_column_expression(
                col_str.strip()
            )

        output_name = f"{func_str.lower()}({col_str})"
        return AggregateDef(agg_type, column_expr, output_name)

    def _infer_select_expressions(
        self,
        group_by_exprs: list[Expression] | None,
        aggregates: list[AggregateDef] | None,
    ) -> list[Expression]:
        if aggregates or group_by_exprs:
            exprs: list[Expression] = []
            if group_by_exprs:
                exprs.extend(group_by_exprs)
            if aggregates:
                exprs.extend(
                    (agg.column if agg.column else get_start_count_expr())
                    for agg in aggregates
                )
            return exprs
        else:
            # select *
            return []
