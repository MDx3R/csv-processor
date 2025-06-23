from storage.tuple import Tuple
from type.type_enum import TypeEnum
from type.value import Value

from .expression import Expression


class ConstantExpression(Expression):
    def __init__(self, value: Value):
        super().__init__([])
        self._value = value

    def evaluate(self, tup: Tuple) -> Value:
        return self._value

    def get_value(self) -> Value:
        return self._value

    def get_return_type(self) -> TypeEnum:
        return self._value.get_type_id()

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, ConstantExpression)
            and self._value == other.get_value()
        )

    def __hash__(self) -> int:
        return hash(self._value)

    def to_string(self) -> str:
        return self._value.to_string()
