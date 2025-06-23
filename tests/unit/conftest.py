import pytest

from type.boolean_type import BooleanType
from type.decimal_value import DecimalType
from type.int_type import IntType
from type.string_value import StringType
from type.type_enum import TypeEnum


@pytest.fixture(scope="module", autouse=True)
def setup_types():
    IntType(TypeEnum.INT)
    DecimalType(TypeEnum.DECIMAL)
    BooleanType(TypeEnum.BOOLEAN)
    StringType(TypeEnum.STRING)
