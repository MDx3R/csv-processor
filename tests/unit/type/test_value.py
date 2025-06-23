import pytest

from type.enums import (
    ComparisonValue,
)
from type.type_enum import TypeEnum
from type.value import Value


class TestValue:
    def test_create_int(self):
        val = 42
        v = Value.create_int(val)
        assert v.get_type_id() == TypeEnum.INT
        assert v.get_value() == val
        assert not v.is_null()

    def test_create_decimal(self):
        val = 3.14
        v = Value.create_decimal(val)
        assert v.get_type_id() == TypeEnum.DECIMAL
        assert v.get_value() == val
        assert not v.is_null()

    @pytest.mark.parametrize("value", [True, False])
    def test_create_boolean(self, value: bool):
        v = Value.create_boolean(value)
        assert v.get_type_id() == TypeEnum.BOOLEAN
        assert v.get_value() is value
        assert not v.is_null()

    @pytest.mark.parametrize(
        "value, boolean",
        [
            (ComparisonValue.NULL, None),
            (ComparisonValue.TRUE, True),
            (ComparisonValue.FALSE, False),
        ],
    )
    def test_create_boolean_from_comparisson_value(
        self, value: ComparisonValue, boolean: bool
    ):
        v = Value.create_boolean(value)
        assert v.get_type_id() == TypeEnum.BOOLEAN
        assert v.get_value() is boolean
        assert v.is_null() == (boolean is None)

    def test_create_string(self):
        v = Value.create_string("hello")
        assert v.get_type_id() == TypeEnum.STRING
        assert v.get_value() == "hello"
        assert not v.is_null()

    def test_null_value(self):
        v = Value(TypeEnum.INT, None)
        assert v.is_null()
        assert v.get_type_id() == TypeEnum.INT
        assert v.get_value() is None

    def test_invalid_value_type(self):
        with pytest.raises(AssertionError):
            Value(TypeEnum.INT, "not_an_int")
        with pytest.raises(AssertionError):
            Value(TypeEnum.DECIMAL, "not_a_float")
        with pytest.raises(AssertionError):
            Value(TypeEnum.BOOLEAN, 1)
        with pytest.raises(AssertionError):
            Value(TypeEnum.STRING, 123)

    def test_create_edge_cases(self):
        v = Value.create_int(2**31 - 1)  # Maximum 32-bit integer
        assert v.get_value() == 2**31 - 1
        v = Value.create_int(-(2**31))  # Minimum 32-bit integer
        assert v.get_value() == -(2**31)

        long_string = "x" * 1000
        v = Value.create_string(long_string)
        assert v.get_value() == long_string

    def test_arithmetic_operations(self):
        v1 = Value.create_int(10)
        v2 = Value.create_int(3)
        assert v1.add(v2).get_value() == 13
        assert v1.subtract(v2).get_value() == 7
        assert v1.multiply(v2).get_value() == 30
        assert v1.divide(v2).get_value() == 10 / 3

    def test_min_max(self):
        v1 = Value.create_int(10)
        v2 = Value.create_int(3)
        assert v1.min(v2).get_value() == 3
        assert v1.max(v2).get_value() == 10

    def test_to_boolean(self):
        v_true = Value.create_int(1).to_boolean()
        v_false = Value.create_int(0).to_boolean()
        assert v_true.get_type_id() == TypeEnum.BOOLEAN
        assert v_true.get_value() is True
        assert v_false.get_value() is False

    def test_get_instance(self):
        v = Value.create_int(1)
        instance = v.get_instance()
        assert hasattr(instance, "cast")

    def test_create_boolean_typeerror(self):
        with pytest.raises(TypeError):
            Value.create_boolean(123.456)  # type: ignore

    def test_create_null_from_type_id(self):
        v = Value.create_null_from_type_id(TypeEnum.INT)
        assert v.get_type_id() == TypeEnum.INT
        assert v.is_null()
