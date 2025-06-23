from abc import ABC, abstractmethod

from type.type_enum import TypeEnum
from type.value import Value


class AggregateExpression(ABC):
    @abstractmethod
    def update(self, val: Value) -> None: ...

    @abstractmethod
    def finalize(self) -> Value: ...


class CountExpression(AggregateExpression):
    def __init__(self):
        self._count = 0

    def update(self, val: Value) -> None:
        if val.is_null():
            return
        self._count += 1

    def finalize(self) -> Value:
        return Value.create_int(self._count)


class AvgExpression(AggregateExpression):
    def __init__(self):
        self._count = 0
        self._sum = None

    def update(self, val: Value) -> None:
        if val.is_null():
            return
        self._count += 1
        self._sum = val if self._sum is None else self._sum.add(val)

    def finalize(self) -> Value:
        if self._count == 0 or self._sum is None:
            return Value.create_null_from_type_id(TypeEnum.DECIMAL)
        return self._sum.divide(Value.create_int(self._count))


class SumExpression(AggregateExpression):
    def __init__(self):
        self._sum = None

    def update(self, val: Value) -> None:
        if val.is_null():
            return
        self._sum = val if self._sum is None else self._sum.add(val)

    def finalize(self) -> Value:
        if self._sum is None:
            return Value.create_null_from_type_id(TypeEnum.DECIMAL)
        return self._sum


class MaxExpression(AggregateExpression):
    def __init__(self):
        self._max = None

    def update(self, val: Value) -> None:
        if val.is_null():
            return
        self._max = val if self._max is None else self._max.max(val)

    def finalize(self) -> Value:
        if self._max is None:
            return Value.create_null_from_type_id(TypeEnum.DECIMAL)
        return self._max


class MinExpression(AggregateExpression):
    def __init__(self):
        self._min = None

    def update(self, val: Value) -> None:
        if val.is_null():
            return
        self._min = val if self._min is None else self._min.min(val)

    def finalize(self) -> Value:
        if self._min is None:
            return Value.create_null_from_type_id(TypeEnum.DECIMAL)
        return self._min
