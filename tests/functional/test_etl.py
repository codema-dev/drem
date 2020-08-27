# flake8: noqa

from pathlib import Path

import pytest

from _pytest.monkeypatch import MonkeyPatch
from prefect import Task
from prefect.engine.state import State
from prefect.utilities.debug import raise_on_exception

from drem import etl
from drem.filepaths import FTEST_DATA


def mock_prefectsecret_run(name: str = None) -> str:
    return "fake-email@fake-company.ie"


@pytest.fixture
def etl_flow_state(monkeypatch: MonkeyPatch) -> State:
    """Run etl flow with dummy test data.

    Args:
        monkeypatch (MonkeyPatch): Pytest fixture to mock out objects s.a. PrefectSecret

    Returns:
        [State]: A Prefect State object containing flow run information
    """
    monkeypatch.setattr(
        etl.PrefectSecret, "run", mock_prefectsecret_run,
    )
    with raise_on_exception():
        state = etl.flow.run(data_dir=FTEST_DATA)

    return state


def test_no_etl_tasks_fail(etl_flow_state: State) -> None:
    """No etl tasks fail.

    Args:
        etl_flow_state (State): A Prefect State object containing flow run information
    """
    assert etl_flow_state.is_successful()
