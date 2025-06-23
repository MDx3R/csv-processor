from abc import ABC, abstractmethod

from storage.tuple import Tuple
from type.type_enum import TypeEnum
from type.value import Value


class Expression(ABC):
    def __init__(self, children: list["Expression"]):
        self._children = children

    @abstractmethod
    def evaluate(self, tup: Tuple) -> Value: ...

    @abstractmethod
    def get_return_type(self) -> TypeEnum: ...

    def get_children(self) -> list["Expression"]:
        return self._children.copy()

    @abstractmethod
    def __eq__(self, other: object) -> bool: ...

    @abstractmethod
    def __hash__(self) -> int: ...

    @abstractmethod
    def to_string(self) -> str: ...
