from dataclasses import dataclass

from type.type_enum import TypeEnum


@dataclass(frozen=True)
class Column:
    name: str
    type_id: TypeEnum

    def get_name(self) -> str:
        return self.name

    def get_type_id(self) -> TypeEnum:
        return self.type_id

    def __str__(self) -> str:
        return f"name='{self.name}';type_id={self.type_id}"


@dataclass(frozen=True)
class Schema:
    columns: list[Column]

    def get_columns(self) -> list[Column]:
        return self.columns.copy()

    def get_column_count(self) -> int:
        return len(self.columns)

    def get_column_by_name(self, name: str) -> Column:
        idx = self.get_column_idx(name)
        return self.get_column(idx)

    def get_column(self, index: int) -> Column:
        return self.columns[index]

    def get_column_idx(self, name: str) -> int:
        for idx, column in enumerate(self.columns):
            if column.name == name:
                return idx
        raise ValueError(f"Column with name '{name}' not found")

    def __str__(self) -> str:
        return ",".join(str(c) for c in self.columns)
