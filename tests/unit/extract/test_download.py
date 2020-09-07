# flake8: noqa

from pathlib import Path

from drem.filepaths import COMMERCIAL_BENCHMARKS_DIR
from drem.filepaths import EXTERNAL_DIR
from drem.filepaths import INTERIM_DIR
from drem.filepaths import RAW_DIR
from drem.filepaths import REQUESTS_DIR
from drem.filepaths import ROUGHWORK_DIR


def test_external_dir_exists_in_source_control(tmp_path: Path) -> None:

    assert EXTERNAL_DIR.exists()


def test_raw_dir_exists_in_source_control(tmp_path: Path) -> None:

    assert RAW_DIR.exists()


def test_interim_dir_exists_in_source_control(tmp_path: Path) -> None:

    assert INTERIM_DIR.exists()


def test_requests_dir_exists_in_source_control(tmp_path: Path) -> None:

    assert REQUESTS_DIR.exists()


def test_roughwork_dir_exists_in_source_control(tmp_path: Path) -> None:

    assert ROUGHWORK_DIR.exists()


def test_commercial_benchmarks_dir_exists_in_source_control(tmp_path: Path) -> None:

    assert COMMERCIAL_BENCHMARKS_DIR.exists()
