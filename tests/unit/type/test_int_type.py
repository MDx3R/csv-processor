import pytest

from type.enums import ComparisonValue
from type.type_enum import TypeEnum
from type.value import Value


class TestIntType:
    def test_cast_to_int(self):
        val = 5
        v = Value.create_int(val)
        v_decimal = v.cast(TypeEnum.INT)
        assert v_decimal.get_type_id() == TypeEnum.INT
        assert v_decimal.get_value() == val

    def test_cast_to_decimal(self):
        val = 5
        v = Value.create_int(val)
        v_decimal = v.cast(TypeEnum.DECIMAL)
        assert v_decimal.get_type_id() == TypeEnum.DECIMAL
        assert v_decimal.get_value() == float(val)

    def test_cast_to_boolean(self):
        v = Value.create_int(1)
        v_bool = v.cast(TypeEnum.BOOLEAN)
        assert v_bool.get_type_id() == TypeEnum.BOOLEAN
        assert v_bool.get_value() is True

    def test_cast_to_string(self):
        v = Value.create_int(42)
        v_str = v.cast(TypeEnum.STRING)
        assert v_str.get_type_id() == TypeEnum.STRING
        assert v_str.get_value() == "42"

    def test_compare_equals(self):
        v1 = Value.create_int(10)
        v2 = Value.create_int(10)
        assert v1.compare_equals(v2) == ComparisonValue.TRUE

    def test_compare_less_than(self):
        v1 = Value.create_int(5)
        v2 = Value.create_int(10)
        assert v1.compare_less_than(v2) == ComparisonValue.TRUE
        assert v2.compare_less_than(v1) == ComparisonValue.FALSE

    def test_compare_less_than_equals(self):
        v1 = Value.create_int(5)
        v2 = Value.create_int(10)
        assert v1.compare_less_than_equals(v2) == ComparisonValue.TRUE
        assert v2.compare_less_than_equals(v1) == ComparisonValue.FALSE
        assert v2.compare_less_than_equals(v2) == ComparisonValue.TRUE

    def test_compare_null(self):
        v1 = Value(TypeEnum.INT, None)
        v2 = Value.create_int(10)
        assert v1.compare_equals(v2) == ComparisonValue.NULL

    def test_cast_invalid(self):
        v = Value.create_int(42)
        with pytest.raises(
            TypeError, match="Integer is not coercable to invalid"
        ):
            v.cast(TypeEnum.INVALID)

    def test_compare_greater_than(self):
        v1 = Value.create_int(10)
        v2 = Value.create_int(5)
        assert v1.compare_greater_than(v2) == ComparisonValue.TRUE
        assert v2.compare_greater_than(v1) == ComparisonValue.FALSE

    def test_compare_greater_than_equals(self):
        v1 = Value.create_int(10)
        v2 = Value.create_int(5)
        assert v1.compare_greater_than_equals(v2) == ComparisonValue.TRUE
        assert v2.compare_greater_than_equals(v1) == ComparisonValue.FALSE
        assert v2.compare_greater_than_equals(v2) == ComparisonValue.TRUE

    def test_compare_with_decimal(self):
        v1 = Value.create_int(5)
        v2 = Value.create_decimal(5.0)
        assert v1.check_comparable(v2)
        assert v1.compare_equals(v2) == ComparisonValue.TRUE
        v3 = Value.create_decimal(5.1)
        assert v1.compare_less_than(v3) == ComparisonValue.TRUE

    def test_to_string(self):
        v = Value.create_int(42)
        assert v.to_string() == "42"
        v = Value.create_int(-42)
        assert v.to_string() == "-42"

    def test_calculate_modification_add(self):
        v1 = Value.create_int(10)
        v2 = Value.create_int(5)
        result = v1.add(v2)
        assert result.get_type_id() == TypeEnum.DECIMAL
        assert result.get_value() == 15.0

    def test_calculate_modification_subtract(self):
        v1 = Value.create_int(10)
        v2 = Value.create_int(5)
        result = v1.subtract(v2)
        assert result.get_type_id() == TypeEnum.DECIMAL
        assert result.get_value() == 5.0

    def test_calculate_modification_multiply(self):
        v1 = Value.create_int(10)
        v2 = Value.create_int(5)
        result = v1.multiply(v2)
        assert result.get_type_id() == TypeEnum.DECIMAL
        assert result.get_value() == 50.0

    def test_calculate_modification_divide(self):
        v1 = Value.create_int(10)
        v2 = Value.create_int(5)
        result = v1.divide(v2)
        assert result.get_type_id() == TypeEnum.DECIMAL
        assert result.get_value() == 2.0

    def test_calculate_modification_divide_by_zero(self):
        v1 = Value.create_int(10)
        v2 = Value.create_int(0)
        result = v1.divide(v2)
        assert result.get_type_id() == TypeEnum.DECIMAL
        assert str(result.get_value()) == "nan"

    def test_calculate_modification_null(self):
        v1 = Value.create_int(10)
        v2 = Value(TypeEnum.INT, None)
        result = v1.add(v2)
        assert result.get_type_id() == TypeEnum.INT
        assert result.is_null()
