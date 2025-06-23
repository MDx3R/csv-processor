from abc import ABC, abstractmethod
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Protocol,
    runtime_checkable,
)

from type.enums import (
    ComparisonOperandEnum,
    ComparisonValue,
    ModificationOperandEnum,
)
from type.type_enum import NUMERIC_TYPES, TypeEnum


if TYPE_CHECKING:
    from type.value import Value


@runtime_checkable
class Comparable(Protocol):
    def __eq__(self, other: object, /) -> bool: ...
    def __ne__(self, other: object, /) -> bool: ...
    def __lt__(self, other: Any, /) -> bool: ...
    def __le__(self, other: Any, /) -> bool: ...
    def __gt__(self, other: Any, /) -> bool: ...
    def __ge__(self, other: Any, /) -> bool: ...

    def __hash__(self) -> int: ...


class Type(ABC):
    _instances: ClassVar[dict[str, "Type"]] = {}

    def __init__(self, type_id: TypeEnum) -> None:
        assert type_id != TypeEnum.INVALID
        assert self.get_type_id() == type_id
        self._instances[type_id] = self

    @abstractmethod
    def get_type_id(self) -> TypeEnum: ...

    @abstractmethod
    def get_required_type(self) -> type: ...

    @abstractmethod
    def cast(self, value: "Value", type_id: TypeEnum) -> "Value": ...

    @abstractmethod
    def compare(
        self, left: "Value", right: "Value", op: ComparisonOperandEnum
    ) -> ComparisonValue: ...

    def serialize(self, val: "Value") -> bytes:
        return val.to_string().encode()

    @abstractmethod
    def deserialize(self, raw: bytes) -> "Value": ...

    def add(self, left: "Value", right: "Value") -> "Value":
        return self.modify(left, right, ModificationOperandEnum.ADD)

    def subtract(self, left: "Value", right: "Value") -> "Value":
        return self.modify(left, right, ModificationOperandEnum.SUB)

    def multiply(self, left: "Value", right: "Value") -> "Value":
        return self.modify(left, right, ModificationOperandEnum.MULT)

    def divide(self, left: "Value", right: "Value") -> "Value":
        return self.modify(left, right, ModificationOperandEnum.DIV)

    def min(self, left: "Value", right: "Value") -> "Value":
        if not self.check_comparable(left, right):
            raise TypeError("Values are not comparable")
        return (
            left
            if left.compare_less_than_equals(right) == ComparisonValue.TRUE
            else right
        )

    def max(self, left: "Value", right: "Value") -> "Value":
        if not self.check_comparable(left, right):
            raise TypeError("Values are not comparable")
        return (
            left
            if left.compare_greater_than_equals(right) == ComparisonValue.TRUE
            else right
        )

    def modify(
        self, left: "Value", right: "Value", op: ModificationOperandEnum
    ) -> "Value":
        if not (
            left.get_type_id() in NUMERIC_TYPES
            and right.get_type_id() in NUMERIC_TYPES
        ):
            raise TypeError("Values are not numeric")

        return self._calculate_modification(left, right, op)

    @abstractmethod
    def _calculate_modification(
        self, left: "Value", right: "Value", op: ModificationOperandEnum
    ) -> "Value": ...

    def _compare_with_op(
        self, lval: Comparable, rval: Comparable, op: ComparisonOperandEnum
    ) -> ComparisonValue:
        result = {
            ComparisonOperandEnum.EQ: lval == rval,
            ComparisonOperandEnum.NEQ: lval != rval,
            ComparisonOperandEnum.LT: lval < rval,
            ComparisonOperandEnum.LTE: lval <= rval,
            ComparisonOperandEnum.GT: lval > rval,
            ComparisonOperandEnum.GTE: lval >= rval,
        }.get(op)

        if result is None:
            raise ValueError(f"Unsupported comparison operator: {op}")

        return ComparisonValue.TRUE if result else ComparisonValue.FALSE

    @classmethod
    def check_comparable(cls, left: "Value", right: "Value") -> bool:
        left_type = left.get_type_id()
        right_type = right.get_type_id()

        if left_type == right_type:
            return True

        if left_type in NUMERIC_TYPES and right_type in NUMERIC_TYPES:
            return True

        return False

    def to_string(self, value: "Value") -> str:
        self.assert_type_match(value)
        return str(value.get_value())

    def assert_type_match(self, value: "Value"):
        assert self.get_type_id() == value.get_type_id()
        val = value.get_value()
        assert val is None or isinstance(val, self.get_required_type())

    @classmethod
    def get_instance(cls, type_id: TypeEnum) -> "Type":
        assert type_id in cls._instances
        return cls._instances[type_id]
