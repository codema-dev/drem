from collections import defaultdict
from pathlib import Path

import pandas as pd

from prefect import task


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


@task
def transform_benchmarks(
    dirpath_to_benchmarks_links: Path, filepath_to_benchmark_energies: Path,
) -> pd.DataFrame:
    """Transform Benchmarks into Tidy Data.

    Args:
        dirpath_to_benchmarks_links (Path): Path to text files linking benchmarks to VO
        uses
        filepath_to_benchmark_energies (Path): Path to csv file containing benchmark
        energy / floor area

    Returns:
        pd.DataFrame: VO uses linked to benchmark energies & categories
    """
    benchmarks_linked_to_vo = _read_text_files_linking_benchmarks_to_vo_to_dataframe(
        dirpath_to_benchmarks_links,
    )

    benchmark_energies = pd.read_csv(filepath_to_benchmark_energies)

    return _merge_benchmarks_with_values(benchmarks_linked_to_vo, benchmark_energies)
