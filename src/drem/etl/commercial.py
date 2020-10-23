import warnings

from prefect import Flow
from prefect import Parameter

import drem.utilities.pandas_tasks as pdt

from drem.filepaths import DATA_DIR
from drem.load.parquet import LoadToParquet
from drem.transform.benchmarks import transform_benchmarks
from drem.transform.vo import transform_vo


warnings.filterwarnings("ignore", message=".*initial implementation of Parquet.*")

load_to_parquet = LoadToParquet(name="Load Data to Parquet file")


with Flow("Extract, Transform & Load DREM Data") as flow:

    data_dir = Parameter("data_dir", default=DATA_DIR)

    external_dir = data_dir / "external"
    benchmarks_dir = data_dir / "commercial_building_benchmarks"
    raw_dir = data_dir / "raw"
    processed_dir = data_dir / "processed"

    vo_raw = pdt.read_parquet(filepath=external_dir / "vo_dublin.parquet")

    benchmarks = transform_benchmarks(benchmarks_dir)
    vo_clean = transform_vo(vo_raw, benchmarks, benchmarks_dir / "Unmatched.txt")

    load_to_parquet(vo_clean, processed_dir / "vo_dublin.parquet")
