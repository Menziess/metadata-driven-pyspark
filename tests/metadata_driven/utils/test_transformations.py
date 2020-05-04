"""Test common useful transformations."""

from datetime import date
from datetime import datetime as dt

from pyspark.sql import DataFrame, SparkSession

from metadata_driven.utils.transformations import (
    add_business_date_year_month_day_fields, dedupe, filter_on_processDate,
    flatten_structs, rename_columns, replace_special_characters)


def test_replace_special_characters(spark: SparkSession) -> None:
    """Test whether this function removes special characters."""
    test_df = spark.createDataFrame([
        ("hello there!", '!@#$%"^&*()')
    ], ['text1', 'text2'])
    assert replace_special_characters(test_df).first().text1 == 'hello there!'
    assert replace_special_characters(test_df).first().text2 == '!@#$%^&*()'


def test_add_business_date_year_month_day_fields(spark: SparkSession) -> None:
    """Test whether columns are correctly added."""
    test_df = spark.createDataFrame([
        (0, dt(1991, 5, 7, 1, 2, 3))
    ], ['id', 'timestamp'])
    df = add_business_date_year_month_day_fields(test_df, 'timestamp')
    row = df.first()
    assert df.columns == ['id', 'timestamp', 'business_date',
                          'Year', 'Month', 'Day']
    assert row.business_date == date(1991, 5, 7)
    assert row.Year == 1991
    assert row.Month == 5
    assert row.Day == 7


def test_filter_on_processDate(spark: SparkSession) -> None:
    """Test whether selection based on process_date works."""
    d1 = dt(1991, 5, 7, 1, 2, 3)
    d2 = dt(1991, 5, 9, 4, 5, 6)
    test_df = spark.createDataFrame([
        (1, d1),
        (2, d2),
    ], ['id', 'process_date'])
    assert filter_on_processDate(test_df, d1).first().id == 1
    assert filter_on_processDate(test_df, d2).first().id == 2


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
