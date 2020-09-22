from os import mkdir
from pathlib import Path

import pandas as pd
import pytest

from prefect.engine.state import State

from drem.plot.elec_diversity_curve import flow
from drem.plot.elec_diversity_curve import relative_peak_demands


@pytest.fixture
def elec_demands_dirpath(tmp_path: Path) -> Path:
    """Create a temporary directory of dummy data called 'SM_electricity'.

    Args:
        tmp_path (Path): see https://docs.pytest.org/en/stable/tmpdir.html

    Returns:
        Path: Path to a temporary directory of dummy data called 'SM_electricity'.
    """
    dirpath = tmp_path / "SM_electricity"
    mkdir(dirpath)

    for filename in ("part.0", "part.1"):

        savepath = dirpath / f"{filename}.parquet"
        pd.DataFrame(
            {
                "id": pd.Series([1392, 1000, 1392], dtype="int16"),
                "demand": pd.Series([0.14, 1, 0.138], dtype="float32"),
                "datetime": pd.Series(
                    [
                        "2009-07-15 01:30:00",
                        "2009-07-15 01:30:00",
                        "2009-07-15 02:00:00",
                    ],
                    dtype="datetime64[ns]",
                ),
            },
        ).to_parquet(savepath)

    return dirpath


@pytest.fixture
def flow_state(elec_demands_dirpath: Path) -> State:
    """Run dummy flow to generate a flow state.

    Args:
        elec_demands_dirpath (Path): Path to a temporary directory of dummy data
            called 'SM_electricity'.

    Returns:
        State: see https://docs.prefect.io/api/latest/engine/state.html#state-2
    """
    sample_sizes = [1]
    random_seed = 1
    return flow.run(
        parameters=dict(
            dirpath=elec_demands_dirpath,
            sample_size=sample_sizes,
            random_seed=random_seed,
        ),
    )


@pytest.mark.e2e
def test_calculate_rel_peak_demands_flow(flow_state: Path) -> None:
    """Ensure flow result exists.

    Args:
        flow_state (Path): see
            https://docs.prefect.io/api/latest/engine/state.html#state-2
    """
    flow_result = flow_state.result[relative_peak_demands].result

    assert flow_result is not None
