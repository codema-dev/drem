import pytest
import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal

from io import StringIO
from codema_drem.preprocess.BER import (
    _drop_imaginary_postcodes,
    _decode_column_suppl_wh_fuel,
    _decode_column_chp_system_type,
    _create_cso_period_built_column,
    _create_cso_dwelling_type_column,
    run_ber_etl_flow,
)


def test_drop_imaginary_postcodes():

    input = pd.read_csv(
        StringIO(
            """postcodes
            dublin 12
            dublin 19
            dublin 21
            dublin 23"""
        ),
        sep="\s*,\s*",
    )

    expected_output = pd.read_csv(
        StringIO(
            """postcodes
            dublin 12
            """
        ),
        sep="\s*,\s*",
    )

    output = _drop_imaginary_postcodes.run(input)

    assert_frame_equal(output, expected_output)


def test_replace_null_values() -> None:

    input = pd.DataFrame(
        {
            "A": pd.Series([1, 1], dtype=np.float64),
            "B": pd.Series(["blah", np.nan], dtype=str),
            "C": pd.Series([1, np.nan], dtype=np.float64),
        }
    )

    expected_output = pd.DataFrame(
        {
            "A": pd.Series([1, 1], dtype=np.float64),
            "B": pd.Series(["blah", ""], dtype=str),
            "C": pd.Series([1, 0], dtype=np.float64),
        }
    )

    output = _replace_null_values.run(input)

    assert_frame_equal(output, expected_output)


def test_decode_column_suppl_wh_fuel() -> None:

    input = pd.DataFrame({"SupplWHFuel": pd.Series([0, 1], dtype=np.float64),})

    expected_output = pd.DataFrame(
        {
            "SupplWHFuel": pd.Series([0, 1], dtype=np.float64),
            "suppl_hw_fuel": pd.Series(["", "Electricity"], dtype=str),
        }
    )

    output = _decode_column_suppl_wh_fuel.run(input)

    assert_frame_equal(output, expected_output)


def test_decode_column_chp_system_type() -> None:

    input = pd.DataFrame({"CHPSystemType": pd.Series([1, 2], dtype=np.float64),})

    expected_output = pd.DataFrame(
        {
            "CHPSystemType": pd.Series([1, 2], dtype=np.float64),
            "chp_system_type": pd.Series(
                ["CHP", "Waste heat from power plant"], dtype=str,
            ),
        }
    )

    output = _decode_column_chp_system_type.run(input)

    assert_frame_equal(output, expected_output)


def test_create_cso_period_built_column() -> None:

    input = pd.DataFrame(
        {
            "Year_of_Construction": pd.Series(
                [1919, 1930, 1950, 1961, 2021,], dtype=np.float64
            ),
        }
    )

    expected_output = pd.DataFrame(
        {
            "Year_of_Construction": pd.Series(
                [1919, 1930, 1950, 1961, 2021,], dtype=np.float64
            ),
            "period_built": pd.Series(
                [
                    "before 1919",
                    "1919 - 1945",
                    "1946 - 1960",
                    "1961 - 1970",
                    "2011 or later",
                ],
                dtype=str,
            ),
        }
    )

    output = _create_cso_period_built_column.run(input)

    assert_frame_equal(output, expected_output)


def test_create_cso_dwelling_type_column() -> None:

    input = pd.DataFrame(
        {
            "DwellingTypeDescr": pd.Series(
                [
                    "mid-floor apartment",
                    "end of terrace house",
                    "semi-detached house",
                    "detached house",
                    "basement dwelling",
                ],
                dtype=str,
            ),
        }
    )

    expected_output = pd.DataFrame(
        {
            "DwellingTypeDescr": pd.Series(
                [
                    "mid-floor apartment",
                    "end of terrace house",
                    "semi-detached house",
                    "detached house",
                    "basement dwelling",
                ],
                dtype=str,
            ),
            "dwelling_type": pd.Series(
                [
                    "apartments",
                    "terraced house",
                    "semi-detached house",
                    "detached house",
                    np.nan,
                ],
                dtype=str,
            ),
        }
    )

    output = _create_cso_dwelling_type_column.run(input)

    assert_frame_equal(output, expected_output)
