import json

from collections import defaultdict
from pathlib import Path

import dask.dataframe as dd
import numpy as np
import pandas as pd
import seaborn as sns

from prefect import Flow
from prefect import Parameter
from prefect import task
from prefect.engine.results import LocalResult
from tqdm import tqdm
from tqdm import trange

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
    sample_size = Parameter("sample_size")
    random_seed = Parameter("random_seed")

    sample_ids = _get_random_sample(unique_ids, size=sample_size, seed=random_seed)
    sample = _extract_sample(elec_demands, on="id", ids=sample_ids)
    relative_peak_demands = _calculate_relative_peak_demand(
        sample, group_on="datetime", target="demand", size=sample_size,
    )


if __name__ == "__main__":

    data_dir = str(PROCESSED_DIR / "SM_electricity")
    sample_sizes = (1, 2, 10, 20, 50, 100, 200, 500, 1000, 2000)
    number_of_simulations = 20

    simulation_results = defaultdict(dict)

    for sample_sz in tqdm(sample_sizes):

        sample_size_results = defaultdict(list)
        for simulation_number in trange(number_of_simulations):

            state = flow.run(
                parameters=dict(
                    dirpath=data_dir,
                    sample_size=sample_sz,
                    random_seed=simulation_number,
                ),
            )
            simulation_result = state.result[relative_peak_demands].result
            sample_size_results[simulation_number] = simulation_result

        simulation_results[sample_sz] = sample_size_results

    filepath_to_simulation_results = str(ROUGHWORK_DIR / "sim_results.json")
    with open(filepath_to_simulation_results, "w") as file:
        json.dump(simulation_result, file)

    results_extracted_from_defaultdict = {
        key: list(value_list.values()) for key, value_list in simulation_results.items()
    }
    clean_demands = pd.DataFrame(results_extracted_from_defaultdict).T

    sns.relplot(data=clean_demands).set(
        title="CRU Smart Meter Electricity Demand",
        ylabel="Peak Demand / Sample Size [kWh/HH]",
        xlabel="Sample Size [HH]",
    ).savefig(ROUGHWORK_DIR / "elec_diversity_curve")
