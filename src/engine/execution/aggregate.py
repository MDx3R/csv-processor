from dataclasses import dataclass
from enum import Enum

from engine.execution.expressions.constant_expression import ConstantExpression
from engine.execution.expressions.expression import Expression
from type.value import Value


class AggregationType(str, Enum):
    COUNT = "count"
    SUM = "sum"
    AVG = "avg"
    MIN = "min"
    MAX = "max"


@dataclass(frozen=True)
class Aggregate:
    type: AggregationType
    expr: Expression
    output_name: str


def get_start_count_expr() -> Expression:
    return ConstantExpression(Value.create_int(1))
