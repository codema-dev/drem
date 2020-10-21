from pathlib import Path

from drem.utilities.filepath_tasks import get_data_dir


def test_get_data_dir():

    expected_dirname = "data"

    output = get_data_dir()
    output_dirname = Path(output).stem

    assert output_dirname == expected_dirname
