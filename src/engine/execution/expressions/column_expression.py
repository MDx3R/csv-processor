from storage.schema import Column
from storage.tuple import Tuple
from type.type_enum import TypeEnum
from type.value import Value

from .expression import Expression


class ColumnExpression(Expression):
    def __init__(self, column: Column):
        super().__init__([])
        self._column = column

    def evaluate(self, tup: Tuple) -> Value:
        return tup.get_value_by_name(self._column.name)

    def get_return_type(self) -> TypeEnum:
        return self._column.get_type_id()

    def get_column(self) -> Column:
        return self._column

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, ColumnExpression)
            and self._column == other.get_column()
        )

    def __hash__(self) -> int:
        return hash(self._column)

    def to_string(self) -> str:
        return self._column.name
