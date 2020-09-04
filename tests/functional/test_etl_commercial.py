# flake8: noqa

from pathlib import Path

import pytest

from _pytest.monkeypatch import MonkeyPatch
from prefect import Task
from prefect.engine.state import State
from prefect.utilities.debug import raise_on_exception

from drem.etl import commercial
from drem.filepaths import FTEST_DATA


@pytest.fixture
def etl_flow_state() -> State:
    """Run etl flow with dummy test data.

    Returns:
        [State]: A Prefect State object containing flow run information
    """
    with raise_on_exception():
        state: State = commercial.flow.run(data_dir=FTEST_DATA)

    return state


@pytest.mark.e2e
def test_no_etl_tasks_fail(etl_flow_state: State) -> None:
    """No etl tasks fail.

    Args:
        etl_flow_state (State): A Prefect State object containing flow run information
    """
    assert etl_flow_state.is_successful()
