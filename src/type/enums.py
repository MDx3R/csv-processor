from enum import Enum


class ComparisonValue(str, Enum):
    NULL = "null"
    TRUE = "true"
    FALSE = "false"


class OperandEnum(str, Enum):
    EQ = "=="
    NEQ = "!="
    GT = ">"
    GTE = ">="
    LT = "<"
    LTE = "<="
