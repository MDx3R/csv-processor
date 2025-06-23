from enum import Enum


class ComparisonValue(str, Enum):
    NULL = "null"
    TRUE = "true"
    FALSE = "false"


class ComparisonOperandEnum(str, Enum):
    EQ = "=="
    NEQ = "!="
    GT = ">"
    GTE = ">="
    LT = "<"
    LTE = "<="


class ModificationOperandEnum(str, Enum):
    ADD = "+"
    SUB = "-"
    MULT = "*"
    DIV = "/"
