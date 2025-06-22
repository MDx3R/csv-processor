import pytest

from type.boolean_type import BooleanType
from type.decimal_value import DecimalType
from type.enums import ComparisonValue
from type.int_type import IntType
from type.string_value import StringType
from type.type_enum import TypeEnum
from type.value import Value


@pytest.fixture(scope="module", autouse=True)
def setup_types():
    IntType(TypeEnum.INT)
    DecimalType(TypeEnum.DECIMAL)
    BooleanType(TypeEnum.BOOLEAN)
    StringType(TypeEnum.STRING)


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
        v = Value.create_int(2**31 - 1)  # Максимальное 32-битное целое
        assert v.get_value() == 2**31 - 1
        v = Value.create_int(-(2**31))  # Минимальное 32-битное целое
        assert v.get_value() == -(2**31)

        long_string = "x" * 1000
        v = Value.create_string(long_string)
        assert v.get_value() == long_string


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
