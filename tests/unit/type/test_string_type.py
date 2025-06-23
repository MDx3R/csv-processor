import pytest

from type.enums import ComparisonValue
from type.type_enum import TypeEnum
from type.value import Value


class TestStringType:
    def test_cast_to_int(self):
        v = Value.create_string("123")
        v_int = v.cast(TypeEnum.INT)
        assert v_int.get_type_id() == TypeEnum.INT
        assert v_int.get_value() == 123
        with pytest.raises(
            ValueError,
        ):
            Value.create_string("abc").cast(TypeEnum.INT)

    def test_cast_to_decimal(self):
        v = Value.create_string("3.14")
        v_decimal = v.cast(TypeEnum.DECIMAL)
        assert v_decimal.get_type_id() == TypeEnum.DECIMAL
        assert v_decimal.get_value() == 3.14
        with pytest.raises(ValueError):
            Value.create_string("abc").cast(TypeEnum.DECIMAL)

    def test_cast_to_boolean(self):
        v = Value.create_string("true")
        v_bool = v.cast(TypeEnum.BOOLEAN)
        assert v_bool.get_type_id() == TypeEnum.BOOLEAN
        assert v_bool.get_value() is True
        v = Value.create_string("0")
        v_bool = v.cast(TypeEnum.BOOLEAN)
        assert v_bool.get_value() is False
        v = Value.create_string("1")
        v_bool = v.cast(TypeEnum.BOOLEAN)
        assert v_bool.get_value() is True
        v = Value.create_string("false")
        v_bool = v.cast(TypeEnum.BOOLEAN)
        assert v_bool.get_value() is False

    def test_cast_to_boolean_invalid(self):
        v = Value.create_string("invalid")
        with pytest.raises(ValueError, match="Boolean value format error"):
            v.cast(TypeEnum.BOOLEAN)

    def test_cast_invalid(self):
        v = Value.create_string("test")
        with pytest.raises(
            TypeError, match="String is not coercable to invalid"
        ):
            v.cast(TypeEnum.INVALID)

    def test_compare_equals(self):
        v1 = Value.create_string("hello")
        v2 = Value.create_string("hello")
        assert v1.compare_equals(v2) == ComparisonValue.TRUE
        v3 = Value.create_string("world")
        assert v1.compare_equals(v3) == ComparisonValue.FALSE

    def test_compare_less_than(self):
        v1 = Value.create_string("apple")
        v2 = Value.create_string("banana")
        assert v1.compare_less_than(v2) == ComparisonValue.TRUE
        assert v2.compare_less_than(v1) == ComparisonValue.FALSE

    def test_compare_greater_than(self):
        v1 = Value.create_string("zebra")
        v2 = Value.create_string("apple")
        assert v1.compare_greater_than(v2) == ComparisonValue.TRUE
        assert v2.compare_greater_than(v1) == ComparisonValue.FALSE

    def test_compare_null(self):
        v1 = Value(TypeEnum.STRING, None)
        v2 = Value.create_string("test")
        assert v1.compare_equals(v2) == ComparisonValue.NULL
        assert v1.compare_less_than(v2) == ComparisonValue.NULL

    def test_compare_invalid(self):
        v1 = Value.create_string("test")
        v2 = Value.create_int(1)
        assert not v1.check_comparable(v2)
        with pytest.raises(AssertionError):
            v1.compare_equals(v2)

    def test_to_string(self):
        v = Value.create_string("hello")
        assert v.to_string() == "hello"
        v = Value.create_string("")
        assert v.to_string() == ""

    def test_calculate_modification_invalid(self):
        v1 = Value.create_string("hello")
        v2 = Value.create_string("world")
        with pytest.raises(TypeError):
            v1.add(v2)
        with pytest.raises(TypeError):
            v1.subtract(v2)
        with pytest.raises(TypeError):
            v1.multiply(v2)
        with pytest.raises(TypeError):
            v1.divide(v2)

    def test_calculate_min_max(self):
        v1 = Value.create_string("hello")
        v2 = Value.create_string("world")
        assert v1.max(v2).get_value() == "world"
        assert v1.min(v2).get_value() == "hello"
