from collections import defaultdict
from pathlib import Path
from typing import DefaultDict
from typing import List

import dask.dataframe as dd
import numpy as np
import pandas as pd
import seaborn as sns

from prefect import Flow
from prefect import Parameter
from prefect import mapped
from prefect import task
from prefect.engine.executors import DaskExecutor
from prefect.engine.results import LocalResult

from drem.filepaths import INTERIM_DIR
from drem.filepaths import PROCESSED_DIR
from drem.filepaths import ROUGHWORK_DIR


@task
def _read_parquet(filepath: Path) -> dd.DataFrame:

    return dd.read_parquet(filepath)


@task(
    target="{task_name}", checkpoint=True, result=LocalResult(dir=INTERIM_DIR),
)
def _get_unique_column_values(ddf: dd.DataFrame, on: str) -> pd.Series:

    return ddf[on].unique().compute()


@task
def _get_random_sample(series: pd.Series, size: int, seed: int) -> np.ndarray:
    """Get a random sample subset of values.

    Args:
        series (pd.Series): Data to be sampled
        size (int): Size of sample
        seed (int): see
            https://numpy.org/doc/stable/reference/random/generated/numpy.random.seed.html

    Returns:
        np.ndarray: A random sample of the data
    """
    np.random.seed(seed)
    return np.random.choice(series, size)


@task
def _extract_sample(ddf: dd.DataFrame, on: str, ids: int) -> pd.DataFrame:

    return ddf[ddf[on].isin(ids)].compute()


@task
def _calculate_relative_peak_demand(
    df: pd.DataFrame, group_on: str, target: str, size: int,
) -> int:

    return df.groupby(group_on)[target].agg(lambda arr: arr.sum() / size).max()


with Flow("Calculate Relative Peak Demand for Sample Size N") as flow:

    dirpath = Parameter("dirpath")
    elec_demands = _read_parquet(dirpath)

    unique_ids = _get_unique_column_values(elec_demands, on="id")
    sample_sizes = Parameter("sample_sizes")
    random_seed = Parameter("random_seed")

    sample_ids = _get_random_sample(
        unique_ids, size=mapped(sample_sizes), seed=random_seed,
    )
    sample = _extract_sample(elec_demands, on="id", ids=mapped(sample_ids))
    relative_peak_demands = _calculate_relative_peak_demand(
        mapped(sample), group_on="datetime", target="demand", size=mapped(sample_sizes),
    )


if __name__ == "__main__":

    data_dir = str(PROCESSED_DIR / "SM_electricity")
    plot_dir = str(ROUGHWORK_DIR / "diversity_curve")
    sample_sizes = [1, 2, 10, 20, 50, 100, 200, 500, 1000, 2000]
    number_of_simulations = 20

    executor = DaskExecutor()
    simulation_results: DefaultDict[int, List[int]] = defaultdict(list)
    for simulation_number in range(number_of_simulations):

        state = flow.run(
            executor=executor,
            parameters=dict(
                dirpath=data_dir,
                sample_sizes=sample_sizes,
                random_seed=simulation_number,
            ),
        )
        simulation_result = state.result[relative_peak_demands].result
        simulation_results[simulation_number] = simulation_result

    raw_demands = np.array(list(simulation_results.values()))
    demands_by_sample_size = raw_demands.T.flatten()
    number_of_repeats = len(demands_by_sample_size) / len(sample_sizes)
    sample_sizes_expanded = np.repeat(sample_sizes, number_of_repeats)

    clean_demands = pd.DataFrame(
        {"demands": demands_by_sample_size, "sample_size": sample_sizes_expanded},
    )
    sns.relplot(x="sample_size", y="demands", data=clean_demands).set(
        title="CRU Smart Meter Electricity Demand",
        ylabel="Peak Demand / Sample Size [kWh/HH]",
        xlabel="Sample Size [HH]",
    ).savefig(plot_dir)
