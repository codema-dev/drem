from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from prefect import Flow
from prefect import Parameter
from prefect import Task
from prefect import task
from prefect.utilities.debug import raise_on_exception

import drem.utilities.dask_dataframe_tasks as ddt
import drem.utilities.pandas_tasks as pdt

from drem.utilities.visualize import VisualizeMixin


@task
def _bin_year_of_construction_as_in_census(
    ber: pd.DataFrame, target: str, result: str,
) -> pd.DataFrame:

    ber = ber.copy()

    year = ber[target].fillna(0).astype(int).to_numpy()

    conditions = [
        year <= 1919,
        year < 1946,
        year < 1961,
        year < 1971,
        year < 1981,
        year < 1991,
        year < 2001,
        year < 2010,
        year < 2025,
        year == 0,
    ]

    choices = [
        "before 1919",
        "1919 - 1945",
        "1946 - 1960",
        "1961 - 1970",
        "1971 - 1980",
        "1981 - 1990",
        "1991 - 2000",
        "2001 - 2010",
        "2011 or later",
        "not stated",
    ]

    ber.loc[:, result] = np.select(conditions, choices, default="ERROR")

    return ber


with Flow("Cleaning the BER Data...") as flow:

    ber_fpath = Parameter("ber_fpath")

    raw_ber = ddt.read_parquet(ber_fpath)

    get_dublin_rows = pdt.get_rows_where_column_contains_substring(
        raw_ber, target="CountyName", substring="Dublin",
    )
    raw_dublin_ber = ddt.compute(get_dublin_rows)

    rename_postcodes = pdt.rename(raw_dublin_ber, columns={"CountyName": "postcodes"})
    bin_year_built_into_census_categories = _bin_year_of_construction_as_in_census(
        rename_postcodes, target="Year_of_Construction", result="cso_period_built",
    )


class TransformBER(Task, VisualizeMixin):
    """Clean BER Data in a Prefect flow.

    Args:
        Task (prefect.Task): see https://docs.prefect.io/core/concepts/tasks.html
        VisualizeMixin (object): Mixin to add flow visualization method
    """

    def __init__(self, **kwargs: Any):
        """Initialise Task.

        Args:
            **kwargs (Any): see https://docs.prefect.io/core/concepts/tasks.html
        """
        self.flow = flow
        super().__init__(**kwargs)

    def run(self, input_filepath: Path, output_filepath: Path) -> None:
        """Run flow.

        Args:
            input_filepath (Path): Path to input data
            output_filepath (Path): Path to output data
        """
        with raise_on_exception():
            state = self.flow.run(parameters=dict(ber_fpath=input_filepath))

        result = state.result[bin_year_built_into_census_categories].result
        result.to_parquet(output_filepath)


transform_ber = TransformBER()
