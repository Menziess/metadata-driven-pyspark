"""Fixture functions accessible across multiple test files."""

from pytest import fixture
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import types as st
from python_package.utils.databricks import spark as package_spark


@fixture
def spark() -> SparkSession:
    """Return SparkSession."""
    return package_spark


@fixture
def test_df() -> DataFrame:
    """Return some data."""
    spark = package_spark

    schema = st.StructType([
        st.StructField('id', st.LongType()),
        st.StructField('money', st.StringType()),
        st.StructField('timestamp', st.LongType()),
        st.StructField('structtype', st.StructType([
            st.StructField('number1', st.StringType()),
            st.StructField('number2', st.StringType()),
            st.StructField('number3', st.StringType()),
        ])),
        st.StructField('rootstructtype', st.StructType([
            st.StructField('nestedstructtype', st.StructType([
                st.StructField('fieldtype', st.StringType()),
            ])),
        ])),
        st.StructField('arraytype', st.ArrayType(st.StringType())),
    ])

    test_df = spark.createDataFrame([
        [1, '$100.000', 17, (1, 2, 3), ((2,),), ['meta', 'data']],
        [1, '$200.000', 17, (3, 2, 1), ((2,),), ['meta', 'data']],
        [1, '$10.000', 16, (1, 3, 2), ((2,),), ['meta', 'data']],
        [2, '-$100', 17, (3, 1, 2), ((2,),), ['meta', 'data']],
        [2, '$100', 14, (2, 1, 3), ((2,),), ['meta', 'data']],
    ], [
        'id', 'money', 'timestamp', 'structtype', 'rootstructtype', 'arraytype'
    ])

    return spark.createDataFrame(test_df.rdd, schema)
