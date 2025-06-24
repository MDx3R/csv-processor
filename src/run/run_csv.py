import sys

from tabulate import tabulate

from engine.engine import Engine
from engine.execution.executors.executor_factory import ExecutorFactory
from engine.execution.parser import ConsoleSelectParser, ExpressionResolver
from engine.execution.planner import QueryPlanner
from storage.schema import Column, Schema
from storage.table import CSVTable, Table
from storage.tuple import Tuple
from type.type_enum import TypeEnum


example_schema = Schema(
    columns=[
        Column(name="id", type_id=TypeEnum.INT),
        Column(name="name", type_id=TypeEnum.STRING),
        Column(name="price", type_id=TypeEnum.DECIMAL),
    ]
)
products_schema = Schema(
    columns=[
        Column(name="name", type_id=TypeEnum.STRING),
        Column(name="brand", type_id=TypeEnum.STRING),
        Column(name="price", type_id=TypeEnum.INT),
        Column(name="rating", type_id=TypeEnum.DECIMAL),
    ]
)
example_table = CSVTable(schema=example_schema, path="data/example.csv")
products_table = CSVTable(schema=products_schema, path="data/products.csv")
table_registry: dict[str, Table] = {
    "example": example_table,
    "products": products_table,
}


def display_tuples(tuples: list[Tuple]) -> str:
    if not tuples:
        return "No data to display."

    schema = tuples[0].schema
    headers = [col.name for col in schema.get_columns()]
    rows = [[str(value) for value in tup.values] for tup in tuples]

    return tabulate(rows, headers=headers, tablefmt="grid")


def main():
    args = sys.argv[1:]
    assert args, "Args required"

    query_planner = QueryPlanner(table_registry)
    resolver = ExpressionResolver(table_registry)
    parser = ConsoleSelectParser(resolver)

    stmt = parser.parse(args)
    plan = query_planner.create_plan(stmt)

    executor_factory = ExecutorFactory(table_registry)
    engine = Engine(plan, executor_factory)

    try:
        result = engine.run()
    except Exception as e:
        import traceback

        traceback.print_exc()
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    print(display_tuples(result))


if __name__ == "__main__":
    main()
