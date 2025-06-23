from abc import ABC

from storage.schema import Schema


class ExecutionPlan(ABC):
    def __init__(
        self, output_schema: Schema, children: list["ExecutionPlan"]
    ) -> None:
        self._output_schema = output_schema
        self._children = children

    def get_output_schema(self) -> Schema:
        return self._output_schema

    def get_children(self) -> list["ExecutionPlan"]:
        return self._children.copy()
