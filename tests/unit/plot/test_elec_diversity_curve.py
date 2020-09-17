from os import mkdir
from pathlib import Path

import dask.dataframe as dd
import numpy as np
import pandas as pd
import pytest

from numpy.testing import assert_array_equal
from pandas.testing import assert_frame_equal
from pandas.testing import assert_series_equal

from drem.plot.elec_diversity_curve import _calculate_relative_peak_demand
from drem.plot.elec_diversity_curve import _extract_sample
from drem.plot.elec_diversity_curve import _get_random_sample
from drem.plot.elec_diversity_curve import _get_unique_column_values


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
def elec_demands(elec_demands_dirpath: Path) -> dd.DataFrame:
    """Create a Dask DataFrame of elec demands using dummy, temporary data.

    Args:
        elec_demands_dirpath (Path):  Path to a temporary directory of dummy data
            called 'SM_electricity'.

    Returns:
        dd.DataFrame: Electricity demand data
    """
    return dd.read_parquet(elec_demands_dirpath)


def test_get_unique_column_values(elec_demands: dd.DataFrame) -> None:
    """Extract unique ids from Dask DataFrame column.

    Args:
        elec_demands (dd.DataFrame): Electricity demand data
    """
    expected_output = pd.Series([1392, 1000], dtype="int16", name="id")

    output = _get_unique_column_values.run(elec_demands, on="id")

    assert_series_equal(output, expected_output)


def test_get_random_sample() -> None:
    """Get id corresponding to sample from series of ids."""
    ids = pd.Series([1392, 1000])
    expected_output = np.array([1392])

    output = _get_random_sample.run(ids, size=1, seed=0)

    assert_array_equal(output, expected_output)


def test_extract_sample(elec_demands: dd.DataFrame) -> None:
    """Get sample data corresponding to sample ids.

    Args:
        elec_demands (dd.DataFrame): Electricity demand data
    """
    sample_ids = pd.Series([1392])
    expected_output = pd.DataFrame(
        {
            "id": pd.Series([1392, 1392, 1392, 1392], dtype="int16"),
            "demand": pd.Series([0.14, 0.138, 0.14, 0.138], dtype="float32"),
            "datetime": pd.Series(
                [
                    "2009-07-15 01:30:00",
                    "2009-07-15 02:00:00",
                    "2009-07-15 01:30:00",
                    "2009-07-15 02:00:00",
                ],
                dtype="datetime64[ns]",
            ),
        },
    )

    output = _extract_sample.run(elec_demands, on="id", ids=sample_ids).reset_index(
        drop=True,
    )

    assert_frame_equal(output, expected_output)


def test_calculate_relative_peak_demand() -> None:
    """Calculate peak demand of time-series relative to sample size."""
    sample_demand = pd.DataFrame(
        {
            "id": pd.Series([1392, 1392, 1000, 1000], dtype="int16"),
            "demand": pd.Series([0.14, 0.138, 0.14, 0.138], dtype="float32"),
            "datetime": pd.Series(
                [
                    "2009-07-15 01:30:00",
                    "2009-07-15 02:00:00",
                    "2009-07-15 01:30:00",
                    "2009-07-15 02:00:00",
                ],
                dtype="datetime64[ns]",
            ),
        },
    )
    expected_output = np.float32(0.14)

    output = _calculate_relative_peak_demand.run(
        sample_demand, group_on="datetime", target="demand", size=2,
    )

    assert output == expected_output
