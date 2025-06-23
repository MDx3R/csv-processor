from enum import Enum


class ComparisonValue(str, Enum):
    NULL = "null"
    TRUE = "true"
    FALSE = "false"

    @staticmethod
    def to_bool(val: "ComparisonValue") -> bool | None:
        if val == ComparisonValue.TRUE:
            return True
        elif val == ComparisonValue.FALSE:
            return False
        return None

    @classmethod
    def to_bool_strict(cls, val: "ComparisonValue") -> bool:
        return bool(cls.to_bool(val))


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
