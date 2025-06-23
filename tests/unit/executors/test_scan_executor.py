from unittest.mock import Mock

import pytest

from engine.execution.executors.scan_executor import ScanExecutor
from engine.execution.plan.scan_plan import ScanPlan
from storage.reader import TableReader
from storage.schema import Schema
from storage.table import Table
from storage.tuple import Tuple


class TestScanExecutor:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.mock_schema = Mock(spec=Schema)
        self.mock_table = Mock(spec=Table)
        self.mock_reader = Mock(spec=TableReader)
        self.mock_reader.read.return_value = iter(
            [Mock(spec=Tuple), Mock(spec=Tuple)]
        )

        self.plan = ScanPlan(self.mock_table, self.mock_schema)

        self.executor = ScanExecutor(self.plan, self.mock_reader)

    def test_init(self):
        with pytest.raises(
            RuntimeError,
        ):
            assert self.executor.get_iterator() is None
        self.executor.init()
        assert self.executor.get_iterator() is not None
        self.mock_reader.read.assert_called_once()

    def test_next_without_init_raises(self):
        with pytest.raises(
            RuntimeError,
        ):
            self.executor.next()

    def test_next_returns_tuples(self):
        tuple1, tuple2 = Mock(spec=Tuple), Mock(spec=Tuple)
        self.mock_reader.read.return_value = iter([tuple1, tuple2])
        self.executor.init()
        assert self.executor.next() == tuple1
        assert self.executor.next() == tuple2
        assert self.executor.next() is None

    def test_next_handles_stop_iteration(self):
        self.mock_reader.read.return_value = iter([])
        self.executor.init()
        assert self.executor.next() is None
