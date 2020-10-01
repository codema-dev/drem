import pandas as pd

from pandas.testing import assert_frame_equal

from drem.estimate.ber_archetypes import _get_mean_heat_demand_per_archetype


def test_get_mean_total_heat_demand_per_archetype() -> None:
    """Evaluate mean total heat demand."""
    ber: pd.DataFrame = pd.DataFrame(
        {
            "postcodes": ["Co. Dublin", "Co. Dublin"],
            "period_built": ["2001 - 2010", "2001 - 2010"],
            "total_heat_demand_per_hh": [8315.668, 3053.864],
        },
    )

    expected_output: pd.DataFrame = pd.DataFrame(
        {
            "postcodes": ["Co. Dublin"],
            "period_built": ["2001 - 2010"],
            "mean_heat_demand_per_archetype": [5684.766],
        },
    )

    output = _get_mean_heat_demand_per_archetype.run(
        ber,
        group_by=["postcodes", "period_built"],
        target="total_heat_demand_per_hh",
        result="mean_heat_demand_per_archetype",
    )
    assert_frame_equal(output, expected_output)
