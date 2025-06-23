from storage.tuple import Tuple
from type.enums import ComparisonOperandEnum
from type.type_enum import TypeEnum
from type.value import Value

from .expression import Expression


class ComparisonExpression(Expression):
    def __init__(
        self, left: Expression, right: Expression, op: ComparisonOperandEnum
    ):
        super().__init__([])
        self._left = left
        self._right = right
        self._op = op

    def evaluate(self, tup: Tuple) -> Value:
        lhs = self._left.evaluate(tup)
        rhs = self._right.evaluate(tup)
        return Value.create_boolean(lhs.compare(rhs, self._op))

    def get_left(self) -> Expression:
        return self._left

    def get_right(self) -> Expression:
        return self._right

    def get_return_type(self) -> TypeEnum:
        return TypeEnum.BOOLEAN

    def __eq__(self, other: object) -> bool:
        return hash(self) == hash(other)

    def __hash__(self) -> int:
        return hash((self._left, self._right, self._op))

    def to_string(self) -> str:
        return f"({self._left.to_string()} {self._op.value} {self._right.to_string()})"
