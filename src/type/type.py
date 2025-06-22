from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, ClassVar, Protocol, runtime_checkable

from type.enums import ComparisonValue, OperandEnum
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
        self, left: "Value", right: "Value", op: OperandEnum
    ) -> ComparisonValue: ...

    def _compare_with_op(
        self, lval: Comparable, rval: Comparable, op: OperandEnum
    ) -> ComparisonValue:
        result = {
            OperandEnum.EQ: lval == rval,
            OperandEnum.NEQ: lval != rval,
            OperandEnum.LT: lval < rval,
            OperandEnum.LTE: lval <= rval,
            OperandEnum.GT: lval > rval,
            OperandEnum.GTE: lval >= rval,
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
