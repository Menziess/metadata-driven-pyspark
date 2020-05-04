"""Test main module."""

from metadata_driven.main import main


def test_main():
    """Test whether main function works."""
    expected_value = '(╯°□°）╯︵ ┻━┻)'
    assert all(
        row.added_column == expected_value for row
        in (
            main('mnt/demo/*.csv', expected_value)
            .select('added_column')
            .collect()
        )
    )
