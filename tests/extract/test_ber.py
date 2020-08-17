# flake8: noqa

from pathlib import Path
from shutil import copyfile

from _pytest.monkeypatch import MonkeyPatch

import drem

from drem.filepaths import TEST_DATA_EXTERNAL


BER_ZIPPED = TEST_DATA_EXTERNAL / "BERPublicsearch.zip"
BER_EOUT = TEST_DATA_EXTERNAL / "BERPublicsearch.csv"


def mock_download_ber(*args, **kwargs) -> None:

    return None


def test_extract_ber(monkeypatch: MonkeyPatch, tmp_path: Path) -> None:
    """Extracted BER DataFrame parquet exists.

    Args:
        monkeypatch (MonkeyPatch): a pytest plugin to mock out objects
        tmpdir (py.path.local): a pytest plugin to create temporary directories
    """
    copyfile(BER_ZIPPED, tmp_path / "BERPublicsearch.zip")
    monkeypatch.setattr(drem.extract.ber, "_download_ber", mock_download_ber)

    output = drem.extract_ber.run("fake-email@fake-company.ie", tmp_path)
    expected_file_output = tmp_path / "BERPublicsearch.parquet"

    assert expected_file_output.exists()
