import warnings

from prefect import Flow
from prefect import Parameter

import drem

from drem.filepaths import DATA_DIR


warnings.filterwarnings("ignore", message=".*initial implementation of Parquet.*")

read_parquet_to_dataframe = drem.ReadParquetToDataFrame(
    name="Read Parquet file to DataFrame",
)
load_to_parquet = drem.LoadToParquet(name="Load Data to Parquet file")


with Flow("Extract, Transform & Load DREM Data") as flow:

    data_dir = Parameter("data_dir", default=DATA_DIR)

    external_dir = data_dir / "external"
    benchmarks_dir = data_dir / "commercial_building_benchmarks"
    raw_dir = data_dir / "raw"
    processed_dir = data_dir / "processed"

    vo_raw = read_parquet_to_dataframe(filepath=external_dir / "vo_dublin.parquet")

    benchmarks = drem.transform_benchmarks(
        benchmarks_dir, benchmarks_dir / "benchmark_energy_demands.csv",
    )
    vo_clean = drem.transform_vo(vo_raw, benchmarks, benchmarks_dir / "Unmatched.txt")

    load_to_parquet(vo_clean, processed_dir / "vo_dublin.parquet")
