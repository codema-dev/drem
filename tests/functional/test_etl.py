# flake8: noqa

from pathlib import Path

from prefect.utilities.debug import raise_on_exception

from drem import etl
from drem.filepaths import FTEST_DATA


def test_etl_runs_without_errors() -> None:

    with raise_on_exception():
        state = etl.flow.run(data_dir=FTEST_DATA)

    assert state.is_successful()
