from pathlib import Path
from typing import Any
from typing import Iterable

import pandas as pd

from prefect import Flow
from prefect import Parameter
from prefect import Task
from prefect import task
from prefect.utilities.debug import raise_on_exception

import drem.utilities.pandas_tasks as pdt

from drem.utilities.visualize import VisualizeMixin


@task
def _get_mean_heat_demand_per_archetype(
    ber: pd.DataFrame, group_by: Iterable[str], target: str, result: str,
) -> pd.DataFrame:

    return ber.copy().groupby(group_by)[target].mean().rename(result).reset_index()


with Flow("Create BER Archetypes") as flow:

    ber_fpath = Parameter("ber_fpath")

    clean_ber = pdt.read_parquet(ber_fpath)

    select_columns_from_raw = pdt.get_columns(
        clean_ber,
        column_names=[
            "postcodes",
            "cso_period_built",
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
        column_names=["postcodes", "cso_period_built", "total_heat_demand_per_hh"],
    )
    ber_archetypes = _get_mean_heat_demand_per_archetype(
        select_columns_from_aggregated,
        group_by=["postcodes", "cso_period_built"],
        target="total_heat_demand_per_hh",
        result="mean_heat_demand_per_archetype",
    )


class CreateBERArchetypes(Task, VisualizeMixin):
    """Create BER Archetypes.

    Args:
        Task (prefect.Task): see  https://docs.prefect.io/core/concepts/tasks.html
        VisualizeMixin (object): Mixin to add flow visualization method
    """

    def __init__(self, **kwargs: Any):
        """Initialise Task.

        Args:
            **kwargs (Any): see https://docs.prefect.io/core/concepts/tasks.html
        """
        self.flow = flow
        super().__init__(**kwargs)

    def run(self, input_filepath: Path, output_filepath: Path) -> pd.DataFrame:
        """Run Flow.

        Args:
            input_filepath (Path): Path to Clean BER Data
            output_filepath (Path): Path to BER archetypes
        """
        with raise_on_exception():
            state = self.flow.run(parameters=dict(ber_fpath=input_filepath))

        result = state.result[ber_archetypes].result

        result.to_parquet(output_filepath)


create_ber_archetypes = CreateBERArchetypes()
