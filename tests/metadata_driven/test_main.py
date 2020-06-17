"""Test main module."""

from pytest import raises
from pyspark.sql import Row
from toolz import pipe
from metadata_driven.main import load_json_dbfs, read, write


def test_load_json_dbfs(metadatajsonpath) -> None:
    """Test reading of metadata."""
    metadata = load_json_dbfs(metadatajsonpath)
    assert list(metadata.keys()) == [
        'input',
        'output',
        'transformations'
    ]


def test_read(meta: dict) -> None:
    """Test reading data using metadata."""
    with raises(KeyError):
        read(meta)

    df = read(meta['input'])
    expected = [
        Row(city='Enkhuizen', count=3),
        Row(city='Lutjebroek', count=1),
        Row(city='Zwaag', count=1),
        Row(city='Venhuizen', count=1)
    ]

    assert list(df.groupBy('city').count().toLocalIterator()) == expected


def test_write(meta: dict) -> None:
    """Test whether data can be written using metadata."""
    from shutil import rmtree

    # Read some data
    test_metadata = {
        "format": "csv",
        "path": "mnt/test/",
        "mode": "overwrite"
    }
    df = read(meta['input'])

    # Write and assert data existence
    pipe(df, write(test_metadata))
    assert read(test_metadata).exceptAll(df).count() == 0

    # Cleanup
    rmtree(test_metadata['path'])
