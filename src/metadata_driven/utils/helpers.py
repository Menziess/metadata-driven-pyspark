"""Common useful generic helper functions."""


from json import load


def abs_to_rel(filepath: str) -> str:
    """Convert `/home` to `home`."""
    return ''.join(filepath[1:]) if filepath[0] == '/' else filepath


def load_json(filepath: str) -> dict:
    """Load json local file api."""
    with open(filepath, 'r') as f:
        return load(f)
