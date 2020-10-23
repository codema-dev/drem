from collections import defaultdict
from os import path
from pathlib import Path

import pandas as pd

from prefect import task

from drem.filepaths import DATA_DIR


def _read_text_files_linking_benchmarks_to_vo_to_dataframe(
    dirpath: Path,
) -> pd.DataFrame:

    links = defaultdict(list)
    for filepath in dirpath.glob("*.txt"):
        with open(filepath, "r") as file:
            for line in file.read().splitlines():
                links[filepath.stem].append(line)

    return (
        pd.DataFrame.from_dict(links, orient="index")
        .reset_index()
        .melt(id_vars="index")
        .drop(columns=["variable"])
        .rename(columns={"index": "benchmark", "value": "vo_use"})
        .sort_values(by="benchmark")
        .dropna(subset=["vo_use"])
        .reset_index(drop=True)
    )


def _merge_benchmarks_with_values(
    benchmarks_linked_to_vo: pd.DataFrame, benchmark_values: pd.DataFrame,
) -> pd.DataFrame:
    return (
        benchmarks_linked_to_vo.merge(benchmark_values, on="benchmark")
        .drop_duplicates()
        .reset_index(drop=True)
    )


@task(
    name="""
        Convert files linking Commercial Benchmarks to Valuation Office
        uses files to a Pandas DataFrame
    """,
)
def transform_benchmarks(dirpath: Path) -> pd.DataFrame:
    """Transform Benchmarks into Tidy Data.

    Args:
        dirpath (Path): Path to benchmarks linked to VO uses

    Returns:
        pd.DataFrame: VO uses linked to benchmark energies & categories
    """
    benchmarks_linked_to_vo = _read_text_files_linking_benchmarks_to_vo_to_dataframe(
        dirpath,
    )

    benchmark_demands_filepath = path.join(dirpath, "benchmark_energy_demands.csv")
    benchmark_demands = pd.read_csv(benchmark_demands_filepath)

    return _merge_benchmarks_with_values(benchmarks_linked_to_vo, benchmark_demands)


if __name__ == "__main__":

    benchmarks = transform_benchmarks.run(DATA_DIR / "commercial_building_benchmarks")
