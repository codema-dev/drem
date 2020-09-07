from pathlib import Path

import pytest

from drem.filepaths import COMMERCIAL_BENCHMARKS_DIR
from drem.filepaths import EXTERNAL_DIR
from drem.filepaths import INTERIM_DIR
from drem.filepaths import RAW_DIR
from drem.filepaths import REQUESTS_DIR
from drem.filepaths import ROUGHWORK_DIR


@pytest.mark.parametrize(
    "data_dirpath",
    [
        COMMERCIAL_BENCHMARKS_DIR,
        EXTERNAL_DIR,
        INTERIM_DIR,
        RAW_DIR,
        REQUESTS_DIR,
        ROUGHWORK_DIR,
    ],
)
def test_data_dir_exists_in_source_control(data_dirpath: Path) -> None:
    """Data dir exists in source control.

    Args:
        data_dirpath (Path): Path to data directory (parametrized...)
    """
    assert data_dirpath.exists()
