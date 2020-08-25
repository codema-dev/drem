#!/usr/bin/env python

from pathlib import Path

import geopandas as gpd

from prefect import task


@task(name="Load Small Area Statistics to file")
def load_sa_statistics(statistics: gpd.GeoDataFrame, savedir: Path) -> None:
    """Load transformed Dublin Small Area Statistics data to local file.

    Args:
        statistics (gpd.GeoDataFrame): Transformed Dublin Small Area Statistics data
        savedir (Path): Save directory for data
    """
    statistics.to_parquet(savedir / "sa_statistics.parquet")
