import csv
from abc import ABC, abstractmethod
from collections.abc import Iterator
from pathlib import Path
from typing import overload

from storage.table import CSVTable, StringTable, Table
from storage.tuple import Tuple
from type.type import Type
from type.value import Value


class TableReader(ABC):
    def __init__(self, table: Table) -> None:
        self._table = table

    @abstractmethod
    def read(self) -> Iterator[Tuple]: ...


class CSVTableReader(TableReader):
    def __init__(self, table: CSVTable) -> None:
        self._table = table

    def read(self) -> Iterator[Tuple]:
        schema = self._table.get_schema()
        path = Path(self._table.get_path())

        with path.open("r", newline="") as csvfile:
            reader = csv.reader(csvfile)
            if self._table.get_skip_first():
                next(reader, None)
            for row in reader:
                values: list[Value] = []
                for i, raw in enumerate(row):
                    column = schema.get_column(i)
                    type_id = column.get_type_id()
                    value = Type.get_instance(type_id).deserialize(
                        raw.encode()
                    )
                    values.append(value)
                yield Tuple(values, schema)


class StringTableReader(TableReader):
    def __init__(self, table: StringTable) -> None:
        self._table = table

    def read(self) -> Iterator[Tuple]:
        schema = self._table.get_schema()
        lines = self._table.get_data().splitlines()
        for line in lines:
            cells = line.strip().split(",")
            values: list[Value] = []
            for i, raw in enumerate(cells):
                type_id = schema.get_column(i).get_type_id()
                value = Type.get_instance(type_id).deserialize(raw.encode())
                values.append(value)

            yield Tuple(values, schema)


class TableReaderFactory:
    @classmethod
    @overload
    def create_reader(cls, table: CSVTable) -> TableReader: ...
    @classmethod
    @overload
    def create_reader(cls, table: StringTable) -> TableReader: ...
    @classmethod
    @overload
    def create_reader(cls, table: Table) -> TableReader: ...

    @classmethod
    def create_reader(cls, table: Table) -> TableReader:
        match table:
            case CSVTable():
                return CSVTableReader(table)
            case StringTable():
                return StringTableReader(table)
            case _:
                pass

        raise TypeError(f"Unsupported table type {type(table)}")
