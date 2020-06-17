"""Test main module."""

from metadata_driven.main import read, load_json_dbfs


def test_load_json_dbfs(metadatajsonpath) -> None:
    """Test reading of metadata."""
    assert load_json_dbfs(metadatajsonpath)


def test_read(meta):
    """Test reading data using metadata."""
    assert read(meta)
