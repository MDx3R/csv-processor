from typing import Self, overload

from type.enums import ComparisonOperandEnum, ComparisonValue
from type.type import Type
from type.type_enum import TypeEnum


class Value:
    def __init__(self, type_id: TypeEnum, value: object):
        assert value is None or isinstance(
            value, Type.get_instance(type_id).get_required_type()
        )
        self._type_id = type_id
        self._value = value

    def get_type_id(self) -> TypeEnum:
        return self._type_id

    def get_value(self) -> object:
        return self._value

    def is_null(self) -> bool:
        return self._value is None

    def cast(self, type_id: TypeEnum) -> "Value":
        return self.get_instance().cast(self, type_id)

    def check_comparable(self, val: "Value") -> bool:
        return self.get_instance().check_comparable(self, val)

    def compare_equals(self, val: "Value") -> ComparisonValue:
        return self.compare(val, ComparisonOperandEnum.EQ)

    def compare_not_equals(self, val: "Value") -> ComparisonValue:
        return self.compare(val, ComparisonOperandEnum.NEQ)

    def compare_less_than(self, val: "Value") -> ComparisonValue:
        return self.compare(val, ComparisonOperandEnum.LT)

    def compare_less_than_equals(self, val: "Value") -> ComparisonValue:
        return self.compare(val, ComparisonOperandEnum.LTE)

    def compare_greater_than(self, val: "Value") -> ComparisonValue:
        return self.compare(val, ComparisonOperandEnum.GT)

    def compare_greater_than_equals(self, val: "Value") -> ComparisonValue:
        return self.compare(val, ComparisonOperandEnum.GTE)

    def compare(
        self, val: "Value", op: ComparisonOperandEnum
    ) -> ComparisonValue:
        return self.get_instance().compare(self, val, op)

    def add(self, val: "Value") -> "Value":
        return self.get_instance().add(self, val)

    def subtract(self, val: "Value") -> "Value":
        return self.get_instance().subtract(self, val)

    def multiply(self, val: "Value") -> "Value":
        return self.get_instance().multiply(self, val)

    def divide(self, val: "Value") -> "Value":
        return self.get_instance().divide(self, val)

    def min(self, val: "Value") -> "Value":
        return self.get_instance().min(self, val)

    def max(self, val: "Value") -> "Value":
        return self.get_instance().max(self, val)

    def to_string(self) -> str:
        return self.get_instance().to_string(self)

    def to_boolean(self) -> "Value":
        return self.get_instance().cast(self, TypeEnum.BOOLEAN)

    def get_instance(self) -> Type:
        return Type.get_instance(self._type_id)

    @classmethod
    def create(cls, type_id: TypeEnum, value: object) -> Self:
        return cls(type_id, value)

    @classmethod
    def create_int(cls, value: int) -> Self:
        return cls.create(TypeEnum.INT, value)

    @classmethod
    def create_decimal(cls, value: float) -> Self:
        return cls.create(TypeEnum.DECIMAL, value)

    @classmethod
    @overload
    def create_boolean(cls, value: bool) -> Self: ...
    @classmethod
    @overload
    def create_boolean(cls, value: ComparisonValue) -> Self: ...

    @classmethod
    def create_boolean(cls, value: bool | ComparisonValue) -> Self:
        match value:
            case bool():
                return cls.create(TypeEnum.BOOLEAN, value)
            case ComparisonValue():
                val = None
                if value == ComparisonValue.TRUE:
                    val = True
                elif value == ComparisonValue.FALSE:
                    val = False

                return cls.create(TypeEnum.BOOLEAN, val)

        raise TypeError(f"Unsupported type {type(value)} for boolean value")

    @classmethod
    def create_string(cls, value: str) -> Self:
        return cls.create(TypeEnum.STRING, value)

    @classmethod
    def create_null_from_type_id(cls, type_id: TypeEnum) -> Self:
        return cls.create(type_id, None)
