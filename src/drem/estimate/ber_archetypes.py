from typing import Iterable

import pandas as pd

from icontract import require
from prefect import Flow
from prefect import Parameter
from prefect import Task
from prefect import task
from prefect.utilities.debug import raise_on_exception

import drem.utilities.pandas_tasks as pdt


@task
def _get_mean_heat_demand_per_archetype(
    ber: pd.DataFrame, group_by: Iterable[str], target: str, result: str,
) -> pd.DataFrame:

    return ber.copy().groupby(group_by)[target].mean().rename(result).reset_index()


with Flow("Create BER Archetypes") as flow:

    clean_ber = Parameter("clean_ber")

    select_columns_from_raw = pdt.get_columns(
        clean_ber,
        column_names=[
            "postcodes",
            "period_built",
            "DeliveredEnergyMainSpace",
            "DeliveredEnergyMainWater",
            "DeliveredEnergySecondarySpace",
            "DeliveredEnergySupplementaryWater",
        ],
    )
    sum_hh_heat_demand_columns = pdt.get_sum_of_columns(
        select_columns_from_raw,
        target=[
            "DeliveredEnergyMainSpace",
            "DeliveredEnergyMainWater",
            "DeliveredEnergySecondarySpace",
            "DeliveredEnergySupplementaryWater",
        ],
        result="total_heat_demand_per_hh",
    )
    select_columns_from_aggregated = pdt.get_columns(
        sum_hh_heat_demand_columns,
        column_names=["postcodes", "period_built", "total_heat_demand_per_hh"],
    )
    ber_archetypes = _get_mean_heat_demand_per_archetype(
        select_columns_from_aggregated,
        group_by=["postcodes", "period_built"],
        target="total_heat_demand_per_hh",
        result="mean_heat_demand_per_archetype",
    )


class CreateBERArchetypes(Task):
    """Create BER Archetypes.

    Args:
        Task (prefect.Task): See
            https://docs.prefect.io/api/latest/core/task.html#task-2
    """

    @require(lambda clean_ber: isinstance(clean_ber, pd.DataFrame))
    def run(self, clean_ber: pd.DataFrame) -> pd.DataFrame:
        """Run Flow.

        Args:
            clean_ber (pd.DataFrame): Clean BER Data

        Returns:
            pd.DataFrame: BER archetype averages
        """
        with raise_on_exception():
            state = flow.run(parameters=dict(clean_ber=clean_ber))

        return state.result[ber_archetypes].result
