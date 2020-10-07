# flake8: noqa

from os import mkdir
from pathlib import Path
from shutil import copytree

import pytest

from _pytest.monkeypatch import MonkeyPatch
from prefect import Task
from prefect.engine.state import State
from prefect.utilities.debug import raise_on_exception

from drem.etl import residential
from drem.filepaths import FTEST_EXTERNAL_DIR


def mock_prefectsecret_run(*args, **kwargs) -> str:
    return "fake-email@fake-company.ie"


def mock_task_run(*args, **kwargs) -> None:
    return None


@pytest.fixture
def etl_flow_state(monkeypatch: MonkeyPatch, tmp_path: Path) -> State:
    """Run etl flow with dummy test data.

    Args:
        monkeypatch (MonkeyPatch): Pytest fixture to mock out objects s.a. PrefectSecret
        tmp_path (Path): See https://docs.pytest.org/en/stable/tmpdir.html

    Returns:
        [State]: A Prefect State object containing flow run information
    """
    # Mock out PrefectSecret as no secrets are required in CI (data already downloaded)
    monkeypatch.setattr(
        residential.PrefectSecret, "run", mock_prefectsecret_run,
    )

    # Mock out task load as CI doesn't need flow outputs on-disk
    monkeypatch.setattr(residential.LoadToParquet, "run", mock_task_run)
    monkeypatch.setattr(
        residential.Download, "run", mock_task_run,
    )
    monkeypatch.setattr(
        residential.DownloadBER, "run", mock_task_run,
    )

    # Copy test data to temporary directory
    external_dir = tmp_path / "external"
    copytree(FTEST_EXTERNAL_DIR, external_dir)

    with raise_on_exception():
        state = residential.flow.run(external_dir=external_dir)

    return state


@pytest.mark.e2e
def test_no_residential_etl_tasks_fail(etl_flow_state: State) -> None:
    """No etl tasks fail.

    Args:
        etl_flow_state (State): A Prefect State object containing flow run information
    """
    assert etl_flow_state.is_successful()
