from copy import copy

from type.enums import ComparisonValue, OperandEnum
from type.type import Type
from type.type_enum import TypeEnum
from type.value import Value


class StringType(Type):
    _type_id = TypeEnum.STRING
    _required_type = str

    def get_type_id(self) -> TypeEnum:
        return self._type_id

    def get_required_type(self) -> type:
        return self._required_type

    def cast(self, value: Value, type_id: TypeEnum) -> Value:
        self.assert_type_match(value)
        val = value.get_value()
        assert isinstance(val, self._required_type)

        match type_id:
            case TypeEnum.STRING:
                return copy(value)
            case TypeEnum.INT:
                try:
                    return Value.create_int(int(val))
                except ValueError as exc:
                    raise ValueError(
                        f"Cannot convert string '{val}' to integer"
                    ) from exc
            case TypeEnum.DECIMAL:
                try:
                    return Value.create_decimal(float(val))
                except ValueError as exc:
                    raise ValueError(
                        f"Cannot convert string '{val}' to decimal"
                    ) from exc
            case TypeEnum.BOOLEAN:
                s = value.to_string().lower()
                if s in {"true", "1"}:
                    return Value.create_boolean(True)
                elif s in {"false", "0"}:
                    return Value.create_boolean(False)
                raise ValueError("Boolean value format error")
            case _:
                pass

        raise TypeError(f"String is not coercable to {type_id.value}")

    def compare(
        self, left: Value, right: Value, op: OperandEnum
    ) -> ComparisonValue:
        self.assert_type_match(left)
        assert left.check_comparable(right)

        if left.is_null() or right.is_null():
            return ComparisonValue.NULL

        lval = str(left.get_value())
        rval = str(right.cast(TypeEnum.STRING).get_value())

        return self._compare_with_op(lval, rval, op)
