import json

from os import path

import dask.dataframe as dd
import prefect

from prefect import Task


class BerPublicSearchToDaskParquet(Task):
    """Create prefect.Task to Convert BERPublicsearch to Dask Parquet.

    Args:
        Task (prefect.Task): see  https://docs.prefect.io/core/concepts/tasks.html
    """

    def run(
        self, input_filepath: str, output_filepath: str, dtypes_filepath: str,
    ) -> None:
        """Convert csv file to parquet.

        Args:
            input_filepath (str): Path to input data
            output_filepath (str): Path to output data
            dtypes_filepath (str): Path to file containing dtypes
        """
        logger = prefect.context.get("logger")
        if path.exists(output_filepath):
            logger.info(f"{output_filepath} already exists")

        else:
            with open(dtypes_filepath, "r") as json_file:
                dtypes = json.load(json_file)

            csv = dd.read_csv(
                self.input_filepath,
                sep="\t",
                encoding="latin-1",
                error_bad_lines=False,
                low_memory=False,
                dtype=dtypes,
            )

            csv.to_parquet(output_filepath, schema="infer")


berpublicsearch = BerPublicSearchToDaskParquet(
    name="Convert BERPublicsearch from txt to dask parquet",
)
