import pandas as pd

from pandas.testing import assert_frame_equal

from drem.modelling.res_build_stock import _count_buildings_by_sa


def test_group_buildings_by_sa() -> None:
    """Find comprising building structure of each SA."""
    df_input = pd.DataFrame(
        {
            "cso_small_area": [1, 1, 1],
            "Dwelling type description": ["apartment", "apartment", "semi-d"],
        },
    )

    expected_output = pd.DataFrame(
        {
            "cso_small_area": [1, 1],
            "Dwelling type description": ["apartment", "semi-d"],
            "Dwelling Percentage": [0.666667, 0.333333],
        },
    )

    output: pd.DataFrame = _count_buildings_by_sa.run(
        df_input,
        by="cso_small_area",
        on="Dwelling type description",
        renamed="Dwelling Percentage",
    )

    assert_frame_equal(output, expected_output)
