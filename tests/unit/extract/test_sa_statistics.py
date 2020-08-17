# flake8: noqa

from pathlib import Path
from shutil import copyfile

from _pytest.monkeypatch import MonkeyPatch

import drem

from drem.filepaths import TEST_DATA_EXTERNAL


SA_STATISTICS = TEST_DATA_EXTERNAL / "sa_statistics.csv"
SA_GLOSSARY = TEST_DATA_EXTERNAL / "sa_glossary.xlsx"


def mock_download(*args, **kwargs) -> None:

    return None


def test_extract_sa_statistics(monkeypatch: MonkeyPatch, tmp_path: Path) -> None:
    """Extracted SA Statistics parquet exists.

    Args:
        monkeypatch (MonkeyPatch): a pytest plugin to mock out objects
        tmpdir (py.path.local): a pytest plugin to create temporary directories
    """
    copyfile(SA_STATISTICS, tmp_path / "sa_statistics.csv")
    monkeypatch.setattr(drem.extract.sa_statistics, "download", mock_download)

    drem.extract_sa_statistics.run(tmp_path)
    expected_file_output = tmp_path / "sa_statistics.parquet"

    assert expected_file_output.exists()
