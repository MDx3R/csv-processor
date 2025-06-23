import pytest

from type.enums import ComparisonValue
from type.type_enum import TypeEnum
from type.value import Value


class TestBooleanType:
    def test_cast_to_int(self):
        v = Value.create_boolean(True)
        v_int = v.cast(TypeEnum.INT)
        assert v_int.get_type_id() == TypeEnum.INT
        assert v_int.get_value() == 1
        v = Value.create_boolean(False)
        v_int = v.cast(TypeEnum.INT)
        assert v_int.get_value() == 0

    def test_cast_to_decimal(self):
        v = Value.create_boolean(True)
        v_decimal = v.cast(TypeEnum.DECIMAL)
        assert v_decimal.get_type_id() == TypeEnum.DECIMAL
        assert v_decimal.get_value() == 1.0
        v = Value.create_boolean(False)
        v_decimal = v.cast(TypeEnum.DECIMAL)
        assert v_decimal.get_value() == 0.0

    def test_cast_to_string(self):
        v = Value.create_boolean(True)
        v_str = v.cast(TypeEnum.STRING)
        assert v_str.get_type_id() == TypeEnum.STRING
        assert v_str.get_value() == "True"

    def test_cast_invalid(self):
        v = Value.create_boolean(True)
        with pytest.raises(
            TypeError, match="Integer is not coercable to invalid"
        ):
            v.cast(TypeEnum.INVALID)

    def test_to_string(self):
        v = Value.create_boolean(True)
        assert v.to_string() == "True"
        v = Value.create_boolean(False)
        assert v.to_string() == "False"

    def test_compare_equals(self):
        v1 = Value.create_boolean(True)
        v2 = Value.create_boolean(True)
        assert v1.compare_equals(v2) == ComparisonValue.TRUE
        v3 = Value.create_boolean(False)
        assert v1.compare_equals(v3) == ComparisonValue.FALSE

    def test_compare_not_equals(self):
        v1 = Value.create_boolean(True)
        v2 = Value.create_boolean(False)
        assert v1.compare_not_equals(v2) == ComparisonValue.TRUE
        assert v1.compare_not_equals(v1) == ComparisonValue.FALSE

    def test_compare_invalid(self):
        v1 = Value.create_boolean(True)
        v2 = Value.create_int(1)
        assert not v1.check_comparable(v2)
        with pytest.raises(AssertionError):
            v1.compare_equals(v2)

    def test_compare_null(self):
        v1 = Value(TypeEnum.BOOLEAN, None)
        v2 = Value.create_boolean(True)
        assert v1.compare_equals(v2) == ComparisonValue.NULL

    def test_calculate_modification_invalid(self):
        v1 = Value.create_boolean(True)
        v2 = Value.create_boolean(False)
        with pytest.raises(TypeError):
            v1.add(v2)
        with pytest.raises(TypeError):
            v1.subtract(v2)
        with pytest.raises(TypeError):
            v1.multiply(v2)
        with pytest.raises(TypeError):
            v1.divide(v2)

    def test_calculate_mix_max(self):
        v1 = Value.create_boolean(True)
        v2 = Value.create_boolean(False)
        assert v1.max(v2).get_value() is True
        assert v1.min(v2).get_value() is False
