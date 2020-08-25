#!/usr/bin/env python

import geopandas as gpd

from prefect import task

from drem.filepaths import PROCESSED_DIR


@task(name="Load Small Area Statistics to file")
def load_sa_statistics(statistics: gpd.GeoDataFrame) -> None:
    """Load transformed Dublin Small Area Statistics data to local file.

    Args:
        statistics (gpd.GeoDataFrame): Transformed Dublin Small Area Statistics data
    """
    savepath = PROCESSED_DIR / "sa_statistics.parquet"
    statistics.to_parquet(savepath)
