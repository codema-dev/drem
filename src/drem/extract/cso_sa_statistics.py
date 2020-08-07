#!/usr/bin/env python

from pathlib import Path

from prefect import task

from drem._filepaths import EXTERNAL_DIR
from drem.extract.download import download


@task(name="Download CSO 2016 Census Small Area Statistics")
def extract_cso_sa_statistics() -> Path:
    """Download CSO 2016 Census Small Area Statistics & Glossary.

    Returns:
        Path: filepath to downloaded data
    """
    statistics: Path = EXTERNAL_DIR / "cso_sa_statistics.csv"

    if not statistics.exists():

        download(
            url="https://www.cso.ie/en/media/csoie/census/census2016/census2016boundaryfiles/SAPS2016_SA2017.csv",
            filepath=statistics,
        )

    return statistics


@task(name="Download CSO Small Area Statistics Glossary")
def extract_cso_sa_glossary() -> Path:
    """Download CSO 2016 Census Small Area Statistics Glossary.

    Returns:
        Path: filepath to downloaded data
    """
    glossary: Path = EXTERNAL_DIR / "cso_sa_glossary.xlsx"

    if not glossary.exists():

        download(
            url="https://www.cso.ie/en/media/csoie/census/census2016/census2016boundaryfiles/SAPS_2016_Glossary.xlsx",
            filepath=glossary,
        )

    return glossary
