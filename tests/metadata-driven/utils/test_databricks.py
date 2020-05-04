"""Tests related to databricks-connect utilities."""

from pyspark.sql import SparkSession
from pytest import mark

from python_package.utils.databricks import get_dbutils, get_spark

try:
    # pyspark.dbutils only exists when databricks-connect is installed
    from pyspark.dbutils import DBUtils
    databricks_connect_installed = True
except ModuleNotFoundError:
    databricks_connect_installed = False


def databricks_cfg_exists():
    """See whether databricks-connect has been configured."""
    from getpass import getuser
    import os
    path = "/home/{}/.databricks-connect".format(getuser())
    return os.path.isfile(path)


def test_get_spark():
    """Test wheter get_spark returns a SparkSession succesfully."""
    spark = get_spark()
    assert isinstance(spark, SparkSession)


def test_is_local():
    """Test if errors are thrown."""
    spark = get_spark()
    assert spark.conf.get("spark.master")


@mark.skipif(
    not databricks_connect_installed or not databricks_cfg_exists(),
    reason='Databricks-connect not set up.'
)
def test_get_dbutils():
    """Test whether get_dbutils returns a DBUtils instance succesfully."""
    spark = get_spark()
    dbutils = get_dbutils(spark)
    assert isinstance(dbutils, DBUtils)
