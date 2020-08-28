# flake8: noqa

from pathlib import Path

import pytest

from _pytest.monkeypatch import MonkeyPatch
from prefect import Task
from prefect.engine.state import State
from prefect.utilities.debug import raise_on_exception

from drem import etl
from drem.filepaths import FTEST_DATA


def mock_prefectsecret_run(*args, **kwargs) -> str:
    return "fake-email@fake-company.ie"


def mock_task_run(*args, **kwargs) -> None:
    return None


@pytest.fixture
def etl_flow_state(monkeypatch: MonkeyPatch) -> State:
    """Run etl flow with dummy test data.

    Args:
        monkeypatch (MonkeyPatch): Pytest fixture to mock out objects s.a. PrefectSecret

    Returns:
        [State]: A Prefect State object containing flow run information
    """
    # Mock out PrefectSecret as no secrets are required in CI (data already downloaded)
    monkeypatch.setattr(
        etl.PrefectSecret, "run", mock_prefectsecret_run,
    )
    # Mock out task load as CI doesn't need flow outputs on-disk
    monkeypatch.setattr(etl.drem.LoadToParquet, "run", mock_task_run)
    with raise_on_exception():
        state = etl.flow.run(data_dir=FTEST_DATA)

    return state


@pytest.mark.e2e
def test_no_etl_tasks_fail(etl_flow_state: State) -> None:
    """No etl tasks fail.

    Args:
        etl_flow_state (State): A Prefect State object containing flow run information
    """
    assert etl_flow_state.is_successful()
