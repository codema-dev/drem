#!/usr/bin/env python

from pathlib import Path

import pandas as pd

from prefect import task

from drem.extract.download import download


CWD = Path.cwd()


@task(name="Download CSO 2016 Census Small Area Statistics")
def extract_sa_statistics(savedir: Path = CWD) -> pd.DataFrame:
    """Download CSO 2016 Census Small Area Statistics.

    Args:
        savedir (Path): Save directory for sa_statistics. Defaults to your
        current working directory (i.e. CWD)

    Returns:
        pd.DataFrame: Small Area Statistics data

    Examples:
        Download data to your current working directory:

        >>> import drem
        >>> from pathlib import Path
        >>> drem.extract_sa_statistics.run()
    """
    filepath = savedir / "sa_statistics.parquet"

    if not filepath.exists():

        download(
            url="https://www.cso.ie/en/media/csoie/census/census2016/census2016boundaryfiles/SAPS2016_SA2017.csv",
            filepath=filepath,
        )

        pd.read_csv(filepath.with_suffix(".csv")).to_parquet(filepath)

    return pd.read_parquet(filepath)


@task(name="Download CSO Small Area Statistics Glossary")
def extract_sa_glossary(savedir: Path = CWD) -> pd.DataFrame:
    """Download CSO 2016 Census Small Area Statistics Glossary.

    Args:
        savedir (Path): Save directory for sa_glossary.
        Defaults to your current working directory (i.e. CWD)

    Returns:
        pd.DataFrame: Small Area Statistics Glossary data

    Examples:
        Download data to your current working directory:

        >>> import drem
        >>> from pathlib import Path
        >>> drem.extract_sa_glossary.run()
    """
    filepath = savedir / "sa_glossary.parquet"

    if not filepath.exists():

        download(
            url="https://www.cso.ie/en/media/csoie/census/census2016/census2016boundaryfiles/SAPS_2016_Glossary.xlsx",
            filepath=filepath,
        )

        pd.read_excel(filepath.with_suffix(".xlsx"), engine="openpyxl").to_parquet(
            filepath,
        )

    return pd.read_parquet(filepath)
