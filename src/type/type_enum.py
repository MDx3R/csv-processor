from enum import Enum


class TypeEnum(str, Enum):
    INVALID = "invalid"
    INT = "int"
    DECIMAL = "decimal"
    BOOLEAN = "boolean"
    STRING = "string"


NUMERIC_TYPES = {TypeEnum.INT, TypeEnum.DECIMAL}
