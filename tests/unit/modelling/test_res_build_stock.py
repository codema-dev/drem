import pandas as pd

from pandas.testing import assert_frame_equal

from drem.modelling.res_build_stock import _count_dwellings_by_sa


def test_group_buildings_by_sa() -> None:
    """Find comprising building structure of each SA"""

    df_input = pd.DataFrame(
        {
            "cso_small_area": [1, 1, 1],
            "dwelling_type": ["apartment", "apartment", "semi-d"],
        }
    )

    expected_output = pd.DataFrame(
        {
            "dwelling_type": ["apartment", "semi-d"],
            "dwelling_percentage": [0.666667, 0.333333],
        }
    ).reset_index(drop=True)

    output: pd.DataFrame = _count_dwellings_by_sa.run(
        df_input, on="dwelling_type", renamed="dwelling_percentage",
    )

    assert_frame_equal(output, expected_output)
