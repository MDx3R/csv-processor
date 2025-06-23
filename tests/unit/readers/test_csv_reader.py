import tempfile
from typing import Any

import pytest

from storage.reader import CSVTableReader
from storage.schema import Column, Schema
from storage.table import CSVTable
from storage.tuple import Tuple
from type.type_enum import TypeEnum
from type.value import Value


class TestCSVTableReader:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.column1 = Column("col1", TypeEnum.STRING)
        self.column2 = Column("col2", TypeEnum.INT)
        self.schema = Schema([self.column1, self.column2])

    def write_file(self, file: Any, data: str):
        file.write(data)
        file.flush()

    def test_read_yields_tuples(self):
        with tempfile.NamedTemporaryFile(
            mode="w+", delete=False, suffix=".csv"
        ) as temp_file:
            self.write_file(temp_file, "value1,123\nvalue2,456\n")
            table = CSVTable(temp_file.name, self.schema)
            reader = CSVTableReader(table)

            tuples = list(reader.read())
            assert len(tuples) == 2
            assert isinstance(tuples[0], Tuple)
            assert isinstance(tuples[1], Tuple)
            assert (
                tuples[0]
                .values[0]
                .compare_equals(Value(TypeEnum.STRING, "value1"))
            )
            assert tuples[0].values[1].compare_equals(Value(TypeEnum.INT, 123))
            assert tuples[0].schema == self.schema
            assert (
                tuples[1]
                .values[0]
                .compare_equals(Value(TypeEnum.STRING, "value2"))
            )
            assert tuples[1].values[1].compare_equals(Value(TypeEnum.INT, 456))
            assert tuples[1].schema == self.schema

    def test_read_empty_file(self):
        with tempfile.NamedTemporaryFile(
            mode="w+", delete=False, suffix=".csv"
        ) as empty_file:
            table = CSVTable(empty_file.name, self.schema)
            reader = CSVTableReader(table)
            tuples = list(reader.read())
            assert len(tuples) == 0

    def test_read_trailing_newline(self):
        with tempfile.NamedTemporaryFile(
            mode="w+", delete=False, suffix=".csv"
        ) as temp_file:
            temp_file.write("value1,123\n")
            temp_file.flush()
            table = CSVTable(temp_file.name, self.schema)
            reader = CSVTableReader(table)
            tuples = list(reader.read())
            assert len(tuples) == 1
            assert (
                tuples[0]
                .values[0]
                .compare_equals(Value(TypeEnum.STRING, "value1"))
            )
            assert tuples[0].values[1].compare_equals(Value(TypeEnum.INT, 123))

    def test_read_invalid_row(self):
        with tempfile.NamedTemporaryFile(
            mode="w+", delete=False, suffix=".csv"
        ) as temp_file:
            temp_file.write("value1\n")  # Missing second column
            temp_file.flush()
            table = CSVTable(temp_file.name, self.schema)
            reader = CSVTableReader(table)
            with pytest.raises(
                AssertionError, match="Value count doesn't match schema"
            ):
                list(reader.read())
