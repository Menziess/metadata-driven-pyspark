"""Test main module."""

from pytest import raises
from pyspark.sql import Row

from metadata_driven.main import load_json_dbfs, read


def test_load_json_dbfs(metadatajsonpath) -> None:
    """Test reading of metadata."""
    metadata = load_json_dbfs(metadatajsonpath)
    assert list(metadata.keys()) == [
        'input',
        'output',
        'transformations'
    ]


def test_read(meta):
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
