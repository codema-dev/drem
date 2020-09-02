from collections import defaultdict
from pathlib import Path

import pandas as pd


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
        .rename(columns={"index": "cibse_benchmarks", "value": "vo_uses"})
        .sort_values(by="cibse_benchmarks")
        .dropna(subset=["vo_uses"])
        .reset_index(drop=True)
    )
