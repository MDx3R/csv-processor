from copy import copy

from type.enums import (
    ComparisonOperandEnum,
    ComparisonValue,
    ModificationOperandEnum,
)
from type.type import Type
from type.type_enum import TypeEnum
from type.value import Value


class BooleanType(Type):
    _type_id = TypeEnum.BOOLEAN
    _required_type = bool

    def get_type_id(self) -> TypeEnum:
        return self._type_id

    def get_required_type(self) -> type:
        return self._required_type

    def cast(self, value: Value, type_id: TypeEnum) -> Value:
        self.assert_type_match(value)
        val = value.get_value()
        assert isinstance(val, self._required_type)

        match type_id:
            case TypeEnum.BOOLEAN:
                return copy(value)
            case TypeEnum.INT:
                return Value.create_int(int(val))
            case TypeEnum.DECIMAL:
                return Value.create_decimal(float(val))
            case TypeEnum.STRING:
                return Value.create_string(str(val))
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

        lval = bool(left.get_value())
        rval = bool(right.cast(TypeEnum.BOOLEAN).get_value())

        return self._compare_with_op(lval, rval, op)

    def deserialize(self, raw: bytes) -> "Value":
        return Value.create_string(raw.decode()).cast(self.get_type_id())

    def _calculate_modification(
        self, left: Value, right: Value, op: ModificationOperandEnum
    ) -> Value:
        raise NotImplementedError
