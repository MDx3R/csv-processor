from copy import copy
from typing import cast

from type.enums import ComparisonValue, OperandEnum
from type.type import Type
from type.type_enum import TypeEnum
from type.value import Value


class DecimalType(Type):
    _type_id = TypeEnum.DECIMAL
    _required_type = float

    def get_type_id(self) -> TypeEnum:
        return self._type_id

    def get_required_type(self) -> type:
        return self._required_type

    def cast(self, value: Value, type_id: TypeEnum) -> Value:
        self.assert_type_match(value)
        val = value.get_value()
        assert isinstance(val, self._required_type)

        match type_id:
            case TypeEnum.DECIMAL:
                return copy(value)
            case TypeEnum.INT:
                return Value.create_int(int(val))
            case TypeEnum.BOOLEAN:
                return Value.create_boolean(bool(val))
            case TypeEnum.STRING:
                return Value.create_string(value.to_string())
            case _:
                pass

        raise TypeError(f"Decimal is not coercable to {type_id.value}")

    def compare(
        self, left: Value, right: Value, op: OperandEnum
    ) -> ComparisonValue:
        self.assert_type_match(left)
        assert left.check_comparable(right)

        if left.is_null() or right.is_null():
            return ComparisonValue.NULL

        lval = cast(float, left.get_value())
        rval = cast(float, right.cast(TypeEnum.DECIMAL).get_value())

        return self._compare_with_op(lval, rval, op)
