"""Support usage of databricks-connect."""

from functools import wraps
from itertools import islice
from typing import Any

from pyspark.sql import SparkSession

INVALID_MOUNT_ERR = (
    "This is not a valid mount path, see help(conn.databricks.create_mount)"
)


def get_spark() -> SparkSession:
    """Get spark session."""
    return SparkSession \
        .builder \
        .getOrCreate()


spark = get_spark()


def with_spark(f):
    """Decorate function to include reference to spark session."""
    @wraps(f)
    def wrap(*args, **kwargs):
        spark = get_spark()
        return f(*args, spark=spark, **kwargs)
    return wrap


def isLocal():
    """See if session is local."""
    return 'local' in spark.conf.get("spark.master")


def get_dbutils() -> Any:
    """Get dbutils instance when running on Databricks."""
    try:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
    except (ImportError, ModuleNotFoundError):
        try:
            import IPython
            dbutils = IPython.get_ipython().user_ns["dbutils"]
        except (AttributeError, ModuleNotFoundError):
            print("databricks-connect is not installed")
            return None
    return dbutils


dbutils = get_dbutils()


def with_dbutils(f):
    """Decorate function to include reference to dbutils instance."""
    @wraps(f)
    def wrap(*args, **kwargs):
        dbutils = get_dbutils(get_spark())
        return f(*args, dbutils=dbutils, **kwargs)
    return wrap


def create_folder(folderpath: str):
    """Create folder on DBFS."""
    return dbutils.fs.mkdirs(folderpath)


def fileExists(filepath: str):
    """Check if file exists."""
    try:
        dbutils.fs.head('/mnt/demo', 1)
    except Exception as e:
        msg = 'Cannot head a directory'
        if msg in str(e):
            return False
        else:
            raise e
    return True


def create_mount(
    storage_account_name: str,
    mount_path: str,
    storage_account_key: str
):
    """Create mount in Databricks.

    Arguments:
        storage_account_name {str} -- Full Storage Account Name from Azure UI.
        mount_path {str} -- '/mnt/mysashort/mycontainer[/subdir][/subsubdir]'
        storage_account_key {str} -- 'Eas72jszyex=='

    Raises:
        ValueError: mount_path is not valid

    """
    fragments = (frag for frag in mount_path.split('/') if frag)

    try:
        mnt, sa_short, container_name = islice(fragments, 3)
    except ValueError as e:
        raise ValueError(INVALID_MOUNT_ERR + str(e))
    if mnt == 'mnt':
        raise Exception(INVALID_MOUNT_ERR)

    name = 'fs.azure.account.key.{}.blob.core.windows.net'.format(
        storage_account_name)
    source = "wasbs://{}@{}.blob.core.windows.net".format(
        container_name, storage_account_name)
    extra_configs = {name: storage_account_key}

    try:
        dbutils.fs.mount(
            source=source,
            mount_point=mount_path,
            extra_configs=extra_configs
        )
        print(mount_path, 'mounted')
    except Exception as e:
        msg = 'already mounted'
        if msg in str(e):
            print(mount_path, msg)
        else:
            raise e
