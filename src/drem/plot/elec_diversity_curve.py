from collections import defaultdict
from pathlib import Path
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

from drem.filepaths import PROCESSED_DIR
from drem.filepaths import ROUGHWORK_DIR


@task
def _read_parquet(filepath: Path) -> dd.DataFrame:

    return dd.read_parquet(filepath)


@task
def _get_unique_ids(elec_demands: dd.DataFrame, on: str) -> pd.Series:

    return elec_demands[on].unique().compute()


@task
def _get_sample_ids(
    unique_ids: pd.Series, sample_size: int, random_seed: int,
) -> np.ndarray:

    np.random.seed(random_seed)
    return np.random.choice(unique_ids, sample_size)


@task
def _generate_sample(
    elec_demands: dd.DataFrame, on: str, sample_ids: int,
) -> pd.DataFrame:

    return elec_demands[elec_demands[on].isin(sample_ids)].compute()


@task
def _calculate_relative_peak_demand(
    sample: pd.DataFrame, group_on: str, target: str, sample_size: int,
) -> int:

    return (
        sample.groupby(group_on)[target].agg(lambda arr: arr.sum() / sample_size).max()
    )


with Flow("Calculate Relative Peak Demand for Sample Size N") as flow:

    dirpath = Parameter("dirpath")
    elec_demands = _read_parquet(dirpath)

    unique_ids = _get_unique_ids(elec_demands, on="id")
    sample_sizes = Parameter("sample_sizes")
    random_seed = Parameter("random_seed")

    sample_ids = _get_sample_ids(
        unique_ids, sample_size=mapped(sample_sizes), random_seed=random_seed,
    )
    sample = _generate_sample(elec_demands, on="id", sample_ids=mapped(sample_ids))
    relative_peak_demands = _calculate_relative_peak_demand(
        mapped(sample),
        group_on="datetime",
        target="demand",
        sample_size=mapped(sample_sizes),
    )


if __name__ == "__main__":

    data_dir = PROCESSED_DIR / "SM_electricity"
    plot_dir = ROUGHWORK_DIR / "diversity_curve"
    sample_sizes = [1, 10, 100, 1000]
    number_of_simulations = 20

    executor = DaskExecutor()
    simulation_results: defaultdict[int, List[int]] = defaultdict(list)
    for simulation_number in range(number_of_simulations):

        state = flow.run(
            executor=executor,
            parameters=dict(
                dirpath=dirpath,
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
