import re

from pathlib import Path

import geopandas as gpd

from prefect import Flow
from prefect import Parameter
from prefect import Task

import drem.utilities.geopandas_tasks as gpdt
import drem.utilities.pandas_tasks as pdt


with Flow("Clean Dublin Postcodes") as flow:

    fpath = Parameter("fpath")

    raw_postcodes = gpdt.read_parquet(fpath)
    postcodes_with_lat_long_crs = gpdt.to_crs(raw_postcodes, epsg=4326)
    postcodes_matched_to_co_dublin = pdt.replace_substring_in_column(
        postcodes_with_lat_long_crs,
        target="Yelp_postc",
        result="postcodes",
        pat="""
            ^               # at start of string
            (?!Dublin.*$)   # doesn't contain 'Dublin' substring
            .*              # Matches any word (greedy)
            """,
        repl="Co. Dublin",
        flags=re.VERBOSE,
    )
    postcodes_with_co_dublin_geometries_dissolved = gpdt.dissolve(
        postcodes_matched_to_co_dublin, by="postcodes",
    )
    clean_postcodes = pdt.get_columns(
        postcodes_with_co_dublin_geometries_dissolved,
        column_names=["postcodes", "geometry"],
    )


class TransformDublinPostcodes(Task):
    """Transform Dublin Postcodes via Prefect Flow.

    Args:
        Task (prefect.Task): see https://docs.prefect.io/core/concepts/tasks.html
    """

    def run(self, dirpath: Path, filename: str) -> gpd.GeoDataFrame:
        """Run module flow.

        Args:
            dirpath (Path): Directory where data is stored
            filename (str): Name of data

        Returns:
            gpd.GeoDataFrame: Clean Dublin Postcode Geometries
        """
        filepath = dirpath / f"{filename}.parquet"

        state = flow.run(fpath=filepath)
        return state.result[clean_postcodes].result


transform_dublin_postcodes = TransformDublinPostcodes()
