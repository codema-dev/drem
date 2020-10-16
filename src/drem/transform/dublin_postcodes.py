import re

from pathlib import Path
from typing import Any

from prefect import Flow
from prefect import Parameter
from prefect import Task

import drem.utilities.geopandas_tasks as gpdt
import drem.utilities.pandas_tasks as pdt

from drem.utilities.visualize import VisualizeMixin


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


class TransformDublinPostcodes(Task, VisualizeMixin):
    """Transform Dublin Postcodes via Prefect Flow.

    Args:
        Task (prefect.Task): see https://docs.prefect.io/core/concepts/tasks.html
        VisualizeMixin (object): Mixin to add flow visualization method
    """

    def __init__(self, **kwargs: Any):
        """Initialise Task.

        Args:
            **kwargs (Any): see https://docs.prefect.io/core/concepts/tasks.html
        """
        self.flow = flow
        super().__init__(**kwargs)

    def run(self, input_filepath: Path, output_filepath: Path) -> None:
        """Run module flow.

        Args:
            input_filepath (Path): Path to input data
            output_filepath (Path): Path to output data
        """
        state = self.flow.run(fpath=input_filepath)
        result = state.result[clean_postcodes].result
        result.to_parquet(output_filepath)


transform_dublin_postcodes = TransformDublinPostcodes()
