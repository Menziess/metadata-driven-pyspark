"""Common useful generic functions."""

from datetime import date, datetime, timedelta
from typing import Iterable, List, Tuple, Union, Any

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as sf
from pyspark.sql import types as st

from metadata_driven.utils.databricks import spark


def get_processdate_default_today(
    windowsStartTime: str,
    format: str
) -> date:
    """Get datetime from string, using format, having a default of today."""
    if windowsStartTime == "":
        return date.today()
    else:
        return datetime.strptime(windowsStartTime, format)


def get_processdate_default_yesterday(
    windowsStartTime: str,
    format: str
) -> date:
    """Get datetime from formatted string, having a default of yesterday."""
    if windowsStartTime == "":
        return date.today() - timedelta(1)
    else:
        return datetime.strptime(windowsStartTime, format)


def are_col_vals_digits(col_val: Column) -> Union[Column, None]:
    """Return whether string value of column is a digit."""
    if col_val is not None:
        col_val = col_val.str.lstrip("-")
        return col_val.str.isdigit()
    else:
        return None


@sf.pandas_udf(returnType=st.BooleanType())
def are_col_vals_digits_spark(col_val: Column) -> Union[Column, None]:
    """Convert `are_col_vals_digits` to udf."""
    return are_col_vals_digits(col_val)


def get_file_name(df: DataFrame):
    """Sort by column `filename`, get largest value."""
    return (
        df
        .agg(
            sf.max(sf.col('filename')).alias('max_filename'))
        .collect()
    )[0].max_filename


def check_primary_key_is_unique(primary_key: str, df: DataFrame) -> None:
    """Print whether pk is unique."""
    if len(df.groupBy(primary_key).agg(
        sf.count("*").alias("counts")
    ).orderBy(
        sf.col("counts").desc()
    ).filter(
        sf.col("counts") > 1
    ).count()):
        print("primary key is unqiue")
    else:
        print("Primary key is not unique!!!!")


def create_processDate_log(df: DataFrame) -> Tuple[DataFrame, str]:
    """Create processDate log."""
    df1 = (
        df
        .select('business_date', 'process_date')
        .withColumn('ProcessDate',
                    sf.col("process_date").cast(st.StringType()))
        .withColumn('BusinessDates',
                    sf.col("business_date").cast(st.StringType()))
        .agg(sf.collect_set(sf.col("BusinessDates")).alias("BusinessDates"),
             sf.first(sf.col("ProcessDate")).alias("ProcessDate"))
        .withColumn("LogCreatedDate", sf.current_date())
    )

    ProcessDate = df1.select(
        sf.date_format(sf.col("ProcessDate"), "yyyyMMdd")).first()[0]
    fileName = "ProcessDate_{}.json".format(ProcessDate)

    df2 = df1.select(sf.struct(sf.col("ProcessDate"), sf.col(
        "LogCreatedDate"), sf.col("BusinessDates")).alias("map"))

    df3 = df2.select(sf.to_json(sf.col("map")).alias("processDate"))

    json_process_log = df3.select("processDate")

    return json_process_log, fileName


def getBusinessDatesFromProcessDate(
    process_date: datetime,
    url: str
) -> Any:
    """Get an array of business dates for a given process date.

    @TODO: this function should be replaced by a generic function.
    """
    filePath = "{}ProcessDate_{}.json".format(
        url, process_date.strftime('%Y%m%d'))
    return spark.read.json(filePath).select("BusinessDates").first()[0]


def createListOfBusinessDateUrl(
    businessDates: Iterable[str],
    url: str
) -> List[str]:
    """Create a list of filespaths from given business date array."""
    return [
        "{}{}/".format(url, (datetime.strptime(
            i, "%Y-%m-%d")).strftime('Year=%Y/Month=%m/Day=%d'))
        for i in businessDates
    ]
