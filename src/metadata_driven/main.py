"""Just an example module."""

from argparse import ArgumentParser, Namespace
from contextlib import contextmanager
from typing import Callable, Iterable

from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import StructType
from toolz import curry, pipe

from metadata_driven.utils.databricks import load_json_dbfs, spark


def get_args() -> Namespace:
    """Parse args."""
    parser = ArgumentParser(__file__)
    parser.add_argument("metadatajsonpath", type=str)
    parser.add_argument("--added_column", type=str,
                        default='🐌🚬😁🤦‍♂️', required=False)
    return parser.parse_args()


def read(meta: dict) -> DataFrame:
    """Read data using metadata."""
    schema = meta.get('schema', None)
    return spark.read.load(
        path=meta.get('path'),
        format=meta.get('format', 'text'),
        schema=StructType.fromJson(schema) if schema else schema,
        **meta.get('options', {})
    )


@curry
def write(meta: dict, df: DataFrame) -> None:
    """Write data using metadata."""
    schema = meta.get('schema', None)
    df_with_schema_applied = spark.createDataFrame(
        data=df.rdd,
        schema=StructType.fromJson(schema) if schema else schema
    )
    df_with_schema_applied.write.save(
        path=meta.get('path'),
        format=meta.get('format', 'parquet'),
        mode=meta.get('mode', 'error'),
        partitionBy=meta.get('partitionBy', None),
        **meta.get('options', {})
    )


def expressions_to_transformations(
    template_transformations: Iterable[dict]
) -> Iterable[Callable]:
    """Convert template expressions to callable functions."""

    def exec_and_return(expression):
        exec(f"""locals()['x'] = {expression}""")
        return locals()['x']

    return [
        lambda df: df.withColumn(
            transformation['col'],
            exec_and_return(transformation['value'])
        )
        for transformation in template_transformations
    ]


@contextmanager
def pipeline(metadatajsonpath: str, *transformations: Callable) -> DataFrame:
    """Load data, inspect it, then write it."""
    meta = load_json_dbfs(metadatajsonpath)
    df = pipe(
        read(meta['input']),
        *expressions_to_transformations(meta['transformations']),
        *transformations
    )
    yield df
    write(meta['output'], df)


if __name__ == "__main__":
    args = get_args()

    try:
        print(f"Processing using metadata: {args.metadatajsonpath}")

        with pipeline(args.metadatajsonpath) as result:
            result.show()

    except KeyboardInterrupt:
        print("\nYou stopped the program.")
