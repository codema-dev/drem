# flake8: noqa

from pathlib import Path
from shutil import copyfile

from _pytest.monkeypatch import MonkeyPatch

import drem

from drem.filepaths import TEST_DATA_EXTERNAL


SA_GEOM_ZIPPED = TEST_DATA_EXTERNAL / "sa_geometries.zip"


def mock_download(*args, **kwargs) -> None:

    return None


def test_extract_sa_geometries(monkeypatch: MonkeyPatch, tmp_path: Path) -> None:
    """Extracted SA Geometries parquet exists.

    Args:
        monkeypatch (MonkeyPatch): a pytest plugin to mock out objects
        tmpdir (py.path.local): a pytest plugin to create temporary directories
    """
    copyfile(SA_GEOM_ZIPPED, tmp_path / "sa_geometries.zip")
    monkeypatch.setattr(drem.extract.sa_geometries, "download", mock_download)

    drem.extract_sa_geometries.run(tmp_path)
    expected_file_output = tmp_path / "sa_geometries.parquet"

    assert expected_file_output.exists()
