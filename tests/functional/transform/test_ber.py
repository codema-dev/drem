import pandas as pd
import pytest

from icontract import ViolationError
from pandas.testing import assert_frame_equal

from drem.transform.ber import TransformBER


transform_ber = TransformBER()


@pytest.fixture
def raw_ber() -> pd.DataFrame:
    """Create raw BER data.

    Returns:
        pd.DataFrame: Raw BER Data
    """
    return pd.DataFrame(
        {
            "CountyName": ["Co. Dublin"],
            "Year_of_Construction": [1912],
            "DeliveredEnergyMainSpace": [1],
            "DeliveredEnergyMainWater": [1],
            "DeliveredEnergySecondarySpace": [1],
            "DeliveredEnergySupplementaryWater": [1],
        },
    )


@pytest.fixture
def clean_ber() -> pd.DataFrame:
    """Create clean BER data.

    Returns:
        pd.DataFrame: Clean BER Data
    """
    return pd.DataFrame(
        {
            "postcodes": ["Co. Dublin"],
            "Year_of_Construction": [1912],
            "DeliveredEnergyMainSpace": [1],
            "DeliveredEnergyMainWater": [1],
            "DeliveredEnergySecondarySpace": [1],
            "DeliveredEnergySupplementaryWater": [1],
            "cso_period_built": ["before 1919"],
        },
    )


def test_clean_ber_raises_error_with_invalid_input() -> None:
    """Raise error with invalid input."""
    i_am_not_a_dataframe = 12

    with pytest.raises(ViolationError):
        transform_ber.run(raw_ber=i_am_not_a_dataframe)


def test_clean_ber_as_expected(raw_ber: pd.DataFrame, clean_ber: pd.DataFrame) -> None:
    """Create archetype as expected with dummy data.

    Args:
        raw_ber (pd.DataFrame): Raw BER Data
        clean_ber (pd.DataFrame): Clean BER Data
    """
    expected_output = clean_ber

    output = transform_ber.run(raw_ber=raw_ber)

    assert_frame_equal(output, expected_output)
