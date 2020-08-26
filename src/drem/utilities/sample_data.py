from pathlib import Path

import geopandas as gpd
import pandas as pd
import pyarrow.parquet as pq

from icontract import require


@require(lambda input_filepath: input_filepath.exists(), "Input File doesn't exist...")
@require(
    lambda input_filepath: input_filepath.suffix == ".parquet",
    "Only parquet file type supported",
)
def _create_sample_data(
    input_filepath: Path, output_filepath: Path, file_engine: str, sample_size: int,
) -> None:

    if file_engine == "pandas":
        pd.read_parquet(input_filepath).sample(sample_size).to_parquet(output_filepath)
    elif file_engine == "geopandas":
        gpd.read_parquet(input_filepath).sample(sample_size).to_parquet(output_filepath)
    else:
        raise ValueError("Only 'pandas' and 'geopandas' are currently supported!")


def create_test_data(
    input_dirpath: Path, output_dirpath: Path, sample_size: int = 100,
) -> None:
    """Sample N rows from input dir parquet files and save to output dir.

    Warning! Requires the input parquet files to first be initialised with metadata for
    file_engine as pandas or geopandas so know which engine to read data with...

    Args:
        input_dirpath (Path): Path to input dir containing parquet files with metadata
        output_dirpath (Path): Path to output dir
        sample_size (int): Number of rows to sample from each file.  Defaults to 100.

    Raises:
        IOError: If v'file_engine' metadata is not specified for input data files!
    """
    input_files = input_dirpath.glob("*.parquet")

    for file in input_files:

        try:
            file_engine = (
                pq.read_metadata(file).metadata[b"file_engine"].decode("utf-8")
            )
        except KeyError:
            raise IOError("No b'file_engine' metadata key found!")

        output_filepath = output_dirpath / file.name
        _create_sample_data(file, output_filepath, file_engine, sample_size)
