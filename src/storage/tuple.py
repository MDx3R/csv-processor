from dataclasses import dataclass

from storage.schema import Schema
from type.value import Value


@dataclass(frozen=True)
class Tuple:
    values: list[Value]
    schema: Schema

    def __post_init__(self):
        assert (
            len(self.values) == self.schema.get_column_count()
        ), f"Value count doesn't match schema: {self.values} vs {self.schema.get_columns()}"

    def get_value(self, index: int) -> Value:
        return self.values[index]

    def get_value_by_name(self, name: str) -> Value:
        idx = self.schema.get_column_idx(name)
        return self.values[idx]

    def __str__(self) -> str:
        return ",".join(str(v) for v in self.values)
