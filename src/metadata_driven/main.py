"""Just an example module."""

from argparse import ArgumentParser, Namespace
from glom import glom

from pyspark.sql import DataFrame
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType

from metadata_driven.utils.databricks import spark, load_json_dbfs


def get_args() -> Namespace:
    """Parse args."""
    parser = ArgumentParser(__file__)
    parser.add_argument("metadatajsonpath", type=str)
    parser.add_argument("--added_column", type=str,
                        default='🐌🚬😁🤦‍♂️', required=False)
    return parser.parse_args()


def read(metajsonpath: str) -> DataFrame:
    """Read folder containing metadata."""
    meta = load_json_dbfs(metajsonpath)
    format = glom(meta, 'input.format', default='text')
    schema = glom(meta, 'input.schema', default=None)
    # fields = glom(meta, 'fields', default=[])
    path = glom(meta, 'input.path')
    return (
        spark
        .read
        .load(
            path=path,
            format=format,
            schema=StructType.fromJson(schema) if schema else schema,
            **glom(meta, 'input.options', default={})
        )
    )


def main(metadatajsonpath: str) -> DataFrame:
    """Show some data.

    This function is merely proof that we can read a particular filepath
    locally, in a DevOps pipeline, and on Databricks.
    """
    return read(metadatajsonpath)


if __name__ == "__main__":
    args = get_args()

    try:
        print(f"Read data using metadata: {args.metadatajsonpath}")
        main(args.metadatajsonpath).show()
    except KeyboardInterrupt:
        print("\nYou stopped the program.")
