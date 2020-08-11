#!/usr/bin/env python

from pathlib import Path
from typing import Optional

import pandas as pd

from prefect import task

from drem.extract.download import download


@task(name="Download CSO 2016 Census Small Area Statistics")
def extract_sa_statistics(statistics: Optional[Path] = None) -> pd.DataFrame:
    """Download CSO 2016 Census Small Area Statistics.

    Args:
        statistics (Path, optional): Save destination for statistics. Defaults to None which
        results in the Statistics being saved to your current working directory.

    Returns:
        pd.DataFrame: Small Area Statistics

    Examples:
        Download data to your current working directory:

        >>> import drem
        >>> from pathlib import Path
        >>> drem.extract_sa_statistics.run()
    """
    if statistics is None:
        statistics: Path = Path.cwd() / "sa_statistics"

    if not statistics.exists():

        download(
            url="https://www.cso.ie/en/media/csoie/census/census2016/census2016boundaryfiles/SAPS2016_SA2017.csv",
            filepath=statistics,
        )

    return pd.read_csv(statistics)


@task(name="Download CSO Small Area Statistics Glossary")
def extract_sa_glossary(glossary: Optional[Path] = None) -> pd.DataFrame:
    """Download CSO 2016 Census Small Area Statistics Glossary.

    Args:
        glossary (Path, optional): Save destination for statistics glossary. Defaults to None
        which results in the Glossary being saved to your current working directory.

    Returns:
        pd.DataFrame: Small Area Statistics Glossary

    Examples:
        Download data to your current working directory:

        >>> import drem
        >>> from pathlib import Path
        >>> drem.extract_sa_glossary.run()
    """
    if glossary is None:
        glossary: Path = Path.cwd() / "sa_glossary.xlsx"

    if not glossary.exists():

        download(
            url="https://www.cso.ie/en/media/csoie/census/census2016/census2016boundaryfiles/SAPS_2016_Glossary.xlsx",
            filepath=glossary,
        )

    return pd.read_excel(glossary, engine="openpyxl")
