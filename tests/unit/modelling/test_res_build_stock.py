import pandas as pd

from pandas.testing import assert_frame_equal

from drem.modelling.res_build_stock import _group_buildings_by_sa


def test_group_buildings_by_sa() -> None:
    """Find comprising building structure of each SA"""

    df_input = pd.DataFrame(
        {"SMALL_AREA": [1, 1, 1], "dwelling_type": ["apartment", "apartment", "semi-d"]}
    )

    expected_output = pd.DataFrame(
        {
            "SMALL_AREA": [1, 1, 1],
            "dwelling_type": ["apartment", "apartment", "semi-d"],
            "apartments_percentage": [66.66, 66.66, 66.66],
            "semi_d_percentage": [33.33, 33.33, 33.33],
        }
    )

    output: pd.DataFrame = _group_buildings_by_sa.run(
        df_input, target="Dwelling type description", result="cso_period_built",
    )

    assert_frame_equal(output, expected_output)
