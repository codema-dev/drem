import pandas as pd

from pandas.testing import assert_frame_equal

from drem.estimate.sa_demand import _aggregate


def test_aggregate_to_small_areas() -> None:
    """Aggregate archetype demand to Small Areas."""
    archetype_demand = pd.DataFrame(
        {"archetype_demand": [735830, 735830], "small_area": [1, 1]},
    )
    expected_output = pd.DataFrame({"small_area": [1], "total_demand": [1471660]})

    output = _aggregate.run(
        archetype_demand, on="small_area", to="archetype_demand", result="total_demand",
    )

    assert_frame_equal(output, expected_output)
