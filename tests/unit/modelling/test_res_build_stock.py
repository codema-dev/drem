import pandas as pd

from pandas.testing import assert_frame_equal

from drem.modelling.res_build_stock import _count_dwellings_by_sa


def test_group_buildings_by_sa() -> None:
    """Find comprising building structure of each SA"""

    df_input = pd.DataFrame(
        {"SMALL_AREA": [1, 1, 1], "dwelling_type": ["apartment", "apartment", "semi-d"]}
    )

    expected_output = pd.DataFrame(
        {
            "SMALL_AREA": [1, 1, 1],
            "dwelling_type": ["apartment", "apartment", "semi-d"],
            "apartments_percentage": [0.66, 0.66, 0.66],
            "semi_d_percentage": [0.33, 0.33, 0.33],
        }
    )

    breakpoint()

    output: pd.DataFrame = _count_dwellings_by_sa.run(
        df_input, on="dwelling_type", renamed="cso_period_built",
    )

    assert_frame_equal(output, expected_output)
