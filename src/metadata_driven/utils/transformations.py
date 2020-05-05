"""Common useful transformations."""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as sf


def dedupe(
    df: DataFrame,
    partition_col='',
    order_col='',
    order_func=sf.desc,
    window_func=sf.row_number,
    debug=False
) -> DataFrame:
    """Dedupe data by getting the last distinct value.

    A function that selects the latest rows
    distinctly based on the given id column.

    >>> spark = getfixture('spark')
    >>> test_df = getfixture('test_df').select('id', 'money', 'timestamp')

    Keep all unique last records:
    >>> dedupe(
    ...  test_df,
    ...   'id',
    ...   'timestamp',
    ...   window_func=sf.row_number
    ... ).show() # doctest: +NORMALIZE_WHITESPACE
    +---+--------+---------+
    | id|   money|timestamp|
    +---+--------+---------+
    |  1|$100.000|       17|
    |  2|   -$100|       17|
    +---+--------+---------+

    Keep all non-unique last records:
    >>> dedupe(
    ...  test_df,
    ...   'id',
    ...   'timestamp',
    ...   window_func=sf.rank
    ... ).show() # doctest: +NORMALIZE_WHITESPACE
    +---+--------+---------+
    | id|   money|timestamp|
    +---+--------+---------+
    |  1|$100.000|       17|
    |  1|$200.000|       17|
    |  2|   -$100|       17|
    +---+--------+---------+
    """
    try:
        first_col = df.columns[0]
    except IndexError:
        raise IndexError("Can't deduplicate empty DataFrame")

    partition_col = partition_col or first_col
    order_col = order_col or partition_col

    w = Window.partitionBy(partition_col).orderBy(order_func(order_col))
    df_with_rn = df.withColumn("_rn", window_func().over(w))

    return (
        df_with_rn.withColumn(
            'keep_record',
            sf.when(sf.col('_rn') == '1', 'keep').otherwise('drop')
        ) if debug
        else df_with_rn.filter("_rn = 1").drop("_rn")
    )


def flatten_structs(df: DataFrame) -> DataFrame:
    """Omits lists, and flattens structs into regular columns.

    >>> test_df = getfixture('test_df').drop('money')
    >>> flatten_structs(test_df).show() # doctest: +NORMALIZE_WHITESPACE
    Omitted column rootstructtype.nestedstructtype
    Omitted column arraytype
    +---+---------+------------------+------------------+------------------+
    | id|timestamp|structtype.number1|structtype.number2|structtype.number3|
    +---+---------+------------------+------------------+------------------+
    |  1|       17|                 1|                 2|                 3|
    |  1|       17|                 3|                 2|                 1|
    |  1|       16|                 1|                 3|                 2|
    |  2|       17|                 3|                 1|                 2|
    |  2|       14|                 2|                 1|                 3|
    +---+---------+------------------+------------------+------------------+
    """
    struct_selectors = []

    for col in df.schema.jsonValue()['fields']:
        if isinstance(col['type'], str):
            struct_selectors.append(col['name'])
        elif isinstance(col['type'], dict) and col['type']['type'] == 'struct':
            for field in col['type']['fields']:
                if (
                    isinstance(field['type'], dict)
                    or isinstance(field['type'], list)
                ):
                    print('Omitted column', col['name'] + '.' + field['name'])
                else:
                    struct_selectors.append(
                        '.'.join([col['name'], field['name']]))
        else:
            print('Omitted column', col['name'])

    return df.select(*[
        sf.col(selector).alias(selector)
        for selector
        in struct_selectors
    ])


def rename_columns(df: DataFrame, mapping: dict) -> DataFrame:
    """Swap or rename columns names.

    Order of the columns should not change.

    >>> test_df = getfixture('test_df').drop('rootstructtype')
    >>> mapping = {
    ...   'id': 'money',
    ...   'money': 'id',
    ...   'timestamp': 'created_at',
    ...   'arraytype': 'myarray',
    ... }
    >>> df = rename_columns(test_df, mapping)
    >>> df.columns
    ['money', 'id', 'created_at', 'structtype', 'myarray']
    >>> df.show() # doctest: +NORMALIZE_WHITESPACE
    +-----+--------+----------+----------+------------+
    |money|      id|created_at|structtype|     myarray|
    +-----+--------+----------+----------+------------+
    |    1|$100.000|        17| [1, 2, 3]|[meta, data]|
    |    1|$200.000|        17| [3, 2, 1]|[meta, data]|
    |    1| $10.000|        16| [1, 3, 2]|[meta, data]|
    |    2|   -$100|        17| [3, 1, 2]|[meta, data]|
    |    2|    $100|        14| [2, 1, 3]|[meta, data]|
    +-----+--------+----------+----------+------------+
    """
    # We add the columns that are not renamed, just to keep the same order
    tailored_mapping = {
        c: mapping.get(c, c)
        for c in df.columns
    }

    # Cannot have non-unique values result in column name conflicts
    if len(set(tailored_mapping.values())) != len(tailored_mapping.values()):
        raise ValueError("Values in dictionary must be unique")

    # Back up all the column names, swapping columns by merely renaming
    # would cause some columns to be overwritten
    for k, v in tailored_mapping.items():
        df = df.withColumn('__' + v, sf.col('`' + k + '`'))

    # Next, we drop the original columns
    df = df.drop(*tailored_mapping.keys())

    # Finally, we remove the prefix again
    for c in df.columns:
        df = df.withColumnRenamed(c, c[2:])
    return df
