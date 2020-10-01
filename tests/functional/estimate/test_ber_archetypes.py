import pandas as pd
import pytest

from pandas.testing import assert_frame_equal

from drem.estimate.ber_archetypes import CreateBERArchetypes


create_ber_archetypes = CreateBERArchetypes()


@pytest.fixture
def clean_ber() -> pd.DataFrame:
    """Create clean BER data.

    Returns:
        pd.DataFrame: Clean BER Data
    """
    return pd.DataFrame(
        {
            "postcodes": ["Co. Dublin"],
            "cso_period_built": ["before 1919"],
            "DeliveredEnergyMainSpace": [1],
            "DeliveredEnergyMainWater": [1],
            "DeliveredEnergySecondarySpace": [1],
            "DeliveredEnergySupplementaryWater": [1],
        },
    )


def test_create_ber_archetypes_as_expected(clean_ber: pd.DataFrame) -> None:
    """Create archetype as expected with dummy data.

    Args:
        clean_ber (pd.DataFrame): Clean BER Data
    """
    expected_output = pd.DataFrame(
        {
            "postcodes": ["Co. Dublin"],
            "cso_period_built": ["before 1919"],
            "mean_heat_demand_per_archetype": [4],
        },
    )

    output = create_ber_archetypes.run(ber=clean_ber)

    assert_frame_equal(output, expected_output)
