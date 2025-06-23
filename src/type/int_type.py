from copy import copy
from typing import cast

from type.enums import (
    ComparisonOperandEnum,
    ComparisonValue,
    ModificationOperandEnum,
)
from type.type import Type
from type.type_enum import NUMERIC_TYPES, TypeEnum
from type.value import Value


class IntType(Type):
    _type_id = TypeEnum.INT
    _required_type = int

    def get_type_id(self) -> TypeEnum:
        return self._type_id

    def get_required_type(self) -> type:
        return self._required_type

    def cast(self, value: Value, type_id: TypeEnum) -> Value:
        self.assert_type_match(value)
        val = value.get_value()
        assert isinstance(val, self._required_type)

        match type_id:
            case TypeEnum.INT:
                return copy(value)
            case TypeEnum.DECIMAL:
                return Value.create_decimal(float(val))
            case TypeEnum.BOOLEAN:
                return Value.create_boolean(bool(val))
            case TypeEnum.STRING:
                return Value.create_string(value.to_string())
            case _:
                pass

        raise TypeError(f"Integer is not coercable to {type_id.value}")

    def compare(
        self, left: Value, right: Value, op: ComparisonOperandEnum
    ) -> ComparisonValue:
        self.assert_type_match(left)
        assert left.check_comparable(right)

        if left.is_null() or right.is_null():
            return ComparisonValue.NULL

        if (
            left.get_type_id() in NUMERIC_TYPES
            and right.get_type_id() in NUMERIC_TYPES
        ):
            lval = cast(float, left.cast(TypeEnum.DECIMAL).get_value())
            rval = cast(float, right.cast(TypeEnum.DECIMAL).get_value())
        else:
            lval = cast(int, left.get_value())
            rval = cast(int, right.cast(TypeEnum.INT).get_value())

        return self._compare_with_op(lval, rval, op)

    def deserialize(self, raw: bytes) -> "Value":
        return Value.create_string(raw.decode()).cast(self.get_type_id())

    def _calculate_modification(
        self, left: Value, right: Value, op: ModificationOperandEnum
    ) -> Value:
        if left.is_null() or right.is_null():
            return Value.create_null_from_type_id(TypeEnum.INT)

        lval = cast(float, left.get_value())
        rval = cast(float, right.cast(TypeEnum.DECIMAL).get_value())

        result = 0
        match op:
            case ModificationOperandEnum.ADD:
                result = lval + rval
            case ModificationOperandEnum.SUB:
                result = lval - rval
            case ModificationOperandEnum.MULT:
                result = lval * rval
            case ModificationOperandEnum.DIV:
                result = lval / rval if rval != 0 else float("nan")
            case _:
                raise ValueError(f"Unsupported modification operator: {op}")

        return Value.create_decimal(result)
