# flake8: noqa

import sys

from os import mkdir
from pathlib import Path
from shutil import copytree
from unittest.mock import Mock
from unittest.mock import patch

import pytest

from _pytest.monkeypatch import MonkeyPatch
from prefect import Task
from prefect.engine.state import State
from prefect.utilities.debug import raise_on_exception

from drem.filepaths import DTYPES_DIR
from drem.filepaths import FTEST_EXTERNAL_DIR


@pytest.fixture
def mock_data_dir(tmp_path) -> Mock:
    """Replace DATA_DIR with FTEST_DATA_DIR

    Args:
        tmp_path(Path): See https://docs.pytest.org/en/stable/tmpdir.html
    """
    # Copy test data to temporary directory
    copytree(FTEST_EXTERNAL_DIR, tmp_path / "external")
    copytree(DTYPES_DIR, tmp_path / "dtypes")
    mkdir(tmp_path / "interim")
    mkdir(tmp_path / "processed")

    with patch(
        "drem.utilities.get_data_dir.get_data_dir", autospec=True,
    ) as _mock_data_dir, patch.dict("sys.modules"):
        sys.modules.pop("drem.etl.residential", None)
        _mock_data_dir.return_value = tmp_path

        yield _mock_data_dir


def mock_prefectsecret_run(*args, **kwargs) -> str:
    return "fake-email@fake-company.ie"


def mock_task_run(*args, **kwargs) -> None:
    return None


@pytest.fixture
def etl_flow_state(monkeypatch: MonkeyPatch, mock_data_dir: Mock) -> State:
    """Run etl flow with dummy test data.

    Args:
        monkeypatch (MonkeyPatch): Pytest fixture to mock out objects s.a. PrefectSecret
        mock_data_dir (Mock): See https://docs.pytest.org/en/stable/tmpdir.html

    Returns:
        [State]: A Prefect State object containing flow run information
    """
    from drem.etl import residential

    # Mock out PrefectSecret as no secrets are required in CI (data already downloaded)
    monkeypatch.setattr(
        residential.PrefectSecret, "run", mock_prefectsecret_run,
    )

    # Mock out task load as CI doesn't need flow outputs on-disk
    monkeypatch.setattr(
        residential.Download, "run", mock_task_run,
    )
    monkeypatch.setattr(
        residential.DownloadBER, "run", mock_task_run,
    )

    with raise_on_exception():
        state = residential.flow.run()

    return state


@pytest.mark.e2e
def test_no_residential_etl_tasks_fail(etl_flow_state: State) -> None:
    """No etl tasks fail.

    Args:
        etl_flow_state (State): A Prefect State object containing flow run information
    """
    assert etl_flow_state.is_successful()
