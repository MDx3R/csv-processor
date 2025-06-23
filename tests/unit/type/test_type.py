import pytest

from type.boolean_type import BooleanType
from type.decimal_value import DecimalType
from type.enums import ModificationOperandEnum
from type.int_type import IntType
from type.string_value import StringType
from type.type import Type
from type.type_enum import TypeEnum
from type.value import Value


class TestType:
    def test_get_instance_invalid(self):
        with pytest.raises(AssertionError):
            Type.get_instance(TypeEnum.INVALID)

    def test_check_comparable_different_types(self):
        v1 = Value.create_int(5)
        v2 = Value.create_string("5")
        assert not Type.check_comparable(v1, v2)

    def test_serialize(self):
        v = Value.create_int(42)
        assert IntType(TypeEnum.INT).serialize(v) == b"42"
        v = Value.create_decimal(3.14)
        assert DecimalType(TypeEnum.DECIMAL).serialize(v) == b"3.14"
        v = Value.create_boolean(True)
        assert BooleanType(TypeEnum.BOOLEAN).serialize(v) == b"True"
        v = Value.create_string("hello")
        assert StringType(TypeEnum.STRING).serialize(v) == b"hello"

    def test_modify_non_numeric(self):
        v1 = Value.create_string("hello")
        v2 = Value.create_string("world")
        with pytest.raises(TypeError, match="Values are not numeric"):
            StringType(TypeEnum.STRING).modify(
                v1, v2, ModificationOperandEnum.ADD
            )

    def test_min_invalid_comparison(self):
        v1 = Value.create_int(5)
        v2 = Value.create_string("5")
        with pytest.raises(TypeError, match="Values are not comparable"):
            IntType(TypeEnum.INT).min(v1, v2)

    def test_max_invalid_comparison(self):
        v1 = Value.create_int(5)
        v2 = Value.create_string("5")
        with pytest.raises(TypeError, match="Values are not comparable"):
            IntType(TypeEnum.INT).max(v1, v2)
