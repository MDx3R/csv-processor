import pytest

from storage.reader import StringTableReader
from storage.schema import Column, Schema
from storage.table import StringTable
from storage.tuple import Tuple
from type.type_enum import TypeEnum
from type.value import Value


class TestStringTableReader:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.column1 = Column("column1", TypeEnum.STRING)
        self.column2 = Column("column2", TypeEnum.INT)

        self.schema = Schema([self.column1, self.column2])

        self.data = "value1,123\nvalue2,456\n"
        self.table = StringTable(self.data, self.schema)

        self.reader = StringTableReader(self.table)

    def test_read_yields_tuples(self):
        tuples = list(self.reader.read())

        assert len(tuples) == len(self.data.split())
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

    def test_read_empty_data(self):
        self.table.set_data("")
        tuples = list(self.reader.read())
        assert len(tuples) == 0

    def test_read_handles_trailing_newline(self):
        self.table.set_data("value1,123\n")
        tuples = list(self.reader.read())
        assert len(tuples) == 1
        assert (
            tuples[0]
            .values[0]
            .compare_equals(Value(TypeEnum.STRING, "value1"))
        )
        assert tuples[0].values[1].compare_equals(Value(TypeEnum.INT, 123))

    def test_read_invalid_data_raises(self):
        self.table.set_data("value1\n")
        with pytest.raises(AssertionError):
            list(self.reader.read())
