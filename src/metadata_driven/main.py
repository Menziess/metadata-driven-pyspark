"""Just an example module."""

from argparse import ArgumentParser, Namespace
from contextlib import contextmanager
from typing import Callable

from pyspark.sql import DataFrame
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
    df.write.save(
        path=meta.get('path'),
        format=meta.get('format', 'parquet'),
        mode=meta.get('mode', 'error'),
        partitionBy=meta.get('partitionBy', None),
        **meta.get('options', {})
    )


@contextmanager
def job(metadatajsonpath: str, *transformations: Callable) -> DataFrame:
    """Load data, inspect it, then write it."""
    meta = load_json_dbfs(metadatajsonpath)
    df = pipe(
        read(meta['input']),
        *transformations
    )
    yield df
    write(meta['output'], df)


if __name__ == "__main__":
    args = get_args()

    try:
        print(f"Processing using metadata: {args.metadatajsonpath}")

        def with_lol_col(df):
            """Add different column."""
            return df.withColumn('lol', df['city'])

        with job(args.metadatajsonpath, with_lol_col) as df:
            df.show()

    except KeyboardInterrupt:
        print("\nYou stopped the program.")
