"""Test common useful transformations."""

from pyspark.sql import DataFrame, SparkSession

from metadata_driven.utils.transformations import (dedupe, flatten_structs,
                                                   rename_columns)


def test_dedupe(test_df: DataFrame, spark: SparkSession) -> None:
    """Test whether data is correctly deduplicated."""
    expected_df = spark.createDataFrame([
        (1, '$100.000', 17),
        (2, '-$100', 17)
    ], ['id', 'money', 'timestamp'])

    result_df = dedupe(
        test_df.select('id', 'money', 'timestamp'),
        'id',
        'timestamp'
    )

    # DataFrames should be equal
    assert not (
        result_df
        .exceptAll(expected_df)
        .take(1)
    )


def test_flatten_structs(test_df: DataFrame) -> None:
    """Test whether structs are flattened."""
    assert flatten_structs(test_df).columns == [
        "id",
        "money",
        "timestamp",
        "structtype.number1",
        "structtype.number2",
        "structtype.number3"
    ]


def test_rename_columns(test_df: DataFrame) -> None:
    """Test whether columns are properly renamed and excluded."""
    df = rename_columns(test_df, {
        'id': 'money',
        'money': 'id',
        'timestamp': 'created_at',
        'arraytype': 'myarray',
    })

    # Order matters, so after swapping two columns, the order should match,
    # and omitted columns should be present at the same index.
    assert df.columns == ['money', 'id', 'created_at', 'structtype',
                          'rootstructtype', 'myarray']
