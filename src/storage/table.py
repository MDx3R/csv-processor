from abc import ABC

from storage.schema import Schema


class Table(ABC):
    def __init__(self, schema: Schema) -> None:
        self._schema = schema

    def get_schema(self) -> Schema:
        return self._schema


class StringTable(Table):
    def __init__(self, data: str, schema: Schema) -> None:
        super().__init__(schema)
        self._data = data

    def get_data(self) -> str:
        return self._data

    def set_data(self, data: str):
        self._data = data


class CSVTable(Table):
    def __init__(self, path: str, schema: Schema) -> None:
        super().__init__(schema)
        self._path = path

    def get_path(self) -> str:
        return self._path
