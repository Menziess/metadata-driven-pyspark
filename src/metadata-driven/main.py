"""Just an example module."""

from argparse import ArgumentParser, Namespace

from pyspark.sql import DataFrame
from pyspark.sql.functions import lit

from python_package.utils.databricks import spark


def get_args() -> Namespace:
    """Parse args."""
    parser = ArgumentParser('example')
    parser.add_argument("filepath", type=str)
    parser.add_argument("--added_column", type=str,
                        default='🐌🚬😁🤦‍♂️', required=False)
    return parser.parse_args()


def main(
    filepath: str,
    added_column: str
) -> DataFrame:
    """Show some data.

    This function is merely proof that we can read a particular filepath
    locally, in a DevOps pipeline, and on Databricks.
    """
    return (
        spark.read
        .option('inferSchema', True)
        .csv(filepath, header=True)
        .withColumn('added_column', lit(added_column))
    )


if __name__ == "__main__":
    args = get_args()

    try:
        print("Added a column to the data that has been read:")
        main(args.filepath, args.added_column).show()
    except KeyboardInterrupt:
        print("\nYou stopped the program.")
