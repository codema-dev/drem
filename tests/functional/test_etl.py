# flake8: noqa

from pathlib import Path

from prefect.utilities.debug import raise_on_exception

from drem import etl
from drem.filepaths import FTEST_DATA


def mock_prefect_secret(*args, **kwargs) -> str:

    return "fake-email@fake-company.ie"


def test_etl_runs_without_errors(monkeypatch) -> None:

    monkeypatch.setattr(
        etl, "PrefectSecret", mock_prefect_secret,
    )
    with raise_on_exception():
        state = etl.flow.run(data_dir=FTEST_DATA)

    assert state.is_successful()
