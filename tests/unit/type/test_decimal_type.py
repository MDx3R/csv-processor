import pytest

from type.enums import ComparisonValue
from type.type_enum import TypeEnum
from type.value import Value


class TestDecimalType:
    def test_cast_to_int(self):
        v = Value.create_decimal(3.14)
        v_int = v.cast(TypeEnum.INT)
        assert v_int.get_type_id() == TypeEnum.INT
        assert v_int.get_value() == 3

    def test_cast_to_boolean(self):
        v = Value.create_decimal(0.0)
        v_bool = v.cast(TypeEnum.BOOLEAN)
        assert v_bool.get_type_id() == TypeEnum.BOOLEAN
        assert v_bool.get_value() is False

    def test_cast_to_string(self):
        v = Value.create_decimal(3.14)
        v_str = v.cast(TypeEnum.STRING)
        assert v_str.get_type_id() == TypeEnum.STRING
        assert v_str.get_value() == "3.14"

    def test_compare_less_than(self):
        v1 = Value.create_decimal(1.5)
        v2 = Value.create_decimal(2.5)
        assert v1.compare_less_than(v2) == ComparisonValue.TRUE

    def test_compare_numeric(self):
        v1 = Value.create_decimal(5.0)
        v2 = Value.create_int(5)
        assert v1.compare_equals(v2) == ComparisonValue.TRUE

    def test_cast_invalid(self):
        v = Value.create_decimal(3.14)
        with pytest.raises(
            TypeError, match="Decimal is not coercable to invalid"
        ):
            v.cast(TypeEnum.INVALID)

    def test_compare_equals(self):
        v1 = Value.create_decimal(2.5)
        v2 = Value.create_decimal(2.5)
        assert v1.compare_equals(v2) == ComparisonValue.TRUE
        v3 = Value.create_decimal(3.0)
        assert v1.compare_equals(v3) == ComparisonValue.FALSE

    def test_compare_greater_than_equals(self):
        v1 = Value.create_decimal(2.5)
        v2 = Value.create_decimal(2.5)
        v3 = Value.create_decimal(2.0)
        assert v1.compare_greater_than_equals(v2) == ComparisonValue.TRUE
        assert v1.compare_greater_than_equals(v3) == ComparisonValue.TRUE

    def test_compare_with_int(self):
        v1 = Value.create_decimal(5.0)
        v2 = Value.create_int(5)
        assert v1.check_comparable(v2)
        assert v1.compare_equals(v2) == ComparisonValue.TRUE
        v3 = Value.create_int(6)
        assert v1.compare_less_than(v3) == ComparisonValue.TRUE

    def test_compare_null(self):
        v1 = Value(TypeEnum.DECIMAL, None)
        v2 = Value.create_decimal(2.5)
        assert v1.compare_equals(v2) == ComparisonValue.NULL
        assert v1.compare_less_than(v2) == ComparisonValue.NULL

    def test_to_string(self):
        v = Value.create_decimal(3.14)
        assert v.to_string() == "3.14"
        v = Value.create_decimal(-0.001)
        assert v.to_string() == "-0.001"

    def test_calculate_modification_add(self):
        v1 = Value.create_decimal(2.5)
        v2 = Value.create_decimal(1.5)
        result = v1.add(v2)
        assert result.get_type_id() == TypeEnum.DECIMAL
        assert result.get_value() == 4.0

    def test_calculate_modification_subtract(self):
        v1 = Value.create_decimal(2.5)
        v2 = Value.create_decimal(1.5)
        result = v1.subtract(v2)
        assert result.get_type_id() == TypeEnum.DECIMAL
        assert result.get_value() == 1.0

    def test_calculate_modification_multiply(self):
        v1 = Value.create_decimal(2.5)
        v2 = Value.create_decimal(2.0)
        result = v1.multiply(v2)
        assert result.get_type_id() == TypeEnum.DECIMAL
        assert result.get_value() == 5.0

    def test_calculate_modification_divide(self):
        v1 = Value.create_decimal(5.0)
        v2 = Value.create_decimal(2.0)
        result = v1.divide(v2)
        assert result.get_type_id() == TypeEnum.DECIMAL
        assert result.get_value() == 2.5

    def test_calculate_modification_divide_by_zero(self):
        v1 = Value.create_decimal(5.0)
        v2 = Value.create_decimal(0.0)
        result = v1.divide(v2)
        assert result.get_type_id() == TypeEnum.DECIMAL
        assert str(result.get_value()) == "nan"

    def test_calculate_modification_null(self):
        v1 = Value.create_decimal(2.5)
        v2 = Value(TypeEnum.DECIMAL, None)
        result = v1.add(v2)
        assert result.get_type_id() == TypeEnum.BOOLEAN
        assert result.is_null()
