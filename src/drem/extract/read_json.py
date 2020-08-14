import json

from pathlib import Path
from typing import Any
from typing import Dict


def read_json(filepath: Path) -> Dict[str, Any]:
    """Read json file to Python Dict.

    Args:
        filepath (Path): File path to json file of interest

    Returns:
        Dict[str, Any]: A Python dictionary corresponding to json
    """
    with open(filepath, "r") as json_file:
        json_file_data = json_file.read()

    return json.loads(json_file_data)
