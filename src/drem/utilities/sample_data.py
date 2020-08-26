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
        input_data = pd.read_parquet(input_filepath)
    elif file_engine == "geopandas":
        input_data = gpd.read_parquet(input_filepath)
    else:
        raise ValueError("Only 'pandas' and 'geopandas' are currently supported!")

    if len(input_data) < sample_size:
        input_data.sample(len(input_data)).to_parquet(output_filepath)
    else:
        input_data.sample(sample_size).to_parquet(output_filepath)


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
    output_filenames = [file.name for file in output_dirpath.glob("*.parquet")]

    for file in input_dirpath.glob("*.parquet"):

        if file.name not in output_filenames:

            try:
                file_engine = (
                    pq.read_metadata(file).metadata[b"file_engine"].decode("utf-8")
                )
            except KeyError:
                raise IOError("No b'file_engine' metadata key found!")

            output_filepath = output_dirpath / file.name
            _create_sample_data(file, output_filepath, file_engine, sample_size)
