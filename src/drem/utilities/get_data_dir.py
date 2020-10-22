from pathlib import Path


def get_data_dir() -> str:
    """Get a filepath to the drem data directory.

    Returns:
        str: Filepath to the drem data directory
    """
    cwd = Path(__file__)
    base_dir = cwd.resolve().parents[2]
    data_dir = base_dir / "data"

    return str(data_dir)
