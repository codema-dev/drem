from pathlib import Path

import pandas as pd

from prefect import Task


class ReadParquetToDataFrame(Task):
    """Create Prefect Task object to read a parquet file to a pandas DataFrame.

    Args:
        Task (Task): Prefect's Task class
    """

    def run(self, filepath: Path) -> pd.DataFrame:
        """Read a parquet file to a pandas DataFrame.

        Args:
            filepath (Path): Filepath to file of interest

        Returns:
            pd.DataFrame: DataFrame of file data
        """
        return pd.read_parquet(filepath)
