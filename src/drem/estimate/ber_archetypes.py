import pandas as pd

from icontract import require
from prefect import task


@require(
    lambda ber: set(ber.columns)
    == {"postcodes", "period_built", "total_heat_demand_per_hh"},
)
def _get_mean_total_heat_demand_per_hh(ber: pd.DataFrame) -> pd.DataFrame:

    return (
        ber.groupby(["postcodes", "period_built"])["total_heat_demand_per_hh"]
        .mean()
        .rename("mean_heat_demand_per_hh")
        .reset_index()
    )


@task
def create_ber_archetypes(ber_raw: pd.DataFrame) -> pd.DataFrame:
    """Tidy BER data set.

    See tests/functional for more information

    Args:
        ber_raw (pd.DataFrame): Raw Ireland BER Residential Housing Data set

    Returns:
        pd.DataFrame: Clean Dublin BER Residential Housing Data set
    """
    return (
        ber_raw.loc[
            :,
            [
                "CountyName",
                "Year_of_Construction",
                "DeliveredEnergyMainSpace",
                "DeliveredEnergyMainWater",
                "DeliveredEnergySecondarySpace",
                "DeliveredEnergySupplementaryWater",
            ],
        ]
        .assign(
            total_heat_demand_per_hh=lambda x: x[
                [
                    "DeliveredEnergyMainSpace",
                    "DeliveredEnergyMainWater",
                    "DeliveredEnergySecondarySpace",
                    "DeliveredEnergySupplementaryWater",
                ]
            ].sum(axis=1),
        )
        .rename(columns={"CountyName": "postcodes"})
        .loc[:, ["postcodes", "period_built", "total_heat_demand_per_hh"]]
        .pipe(_get_mean_total_heat_demand_per_hh)
    )
