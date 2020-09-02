import warnings

import prefect

from prefect import Flow
from prefect import Parameter
from prefect.tasks.secrets import PrefectSecret

import drem

from drem.filepaths import DATA_DIR


warnings.filterwarnings("ignore", message=".*initial implementation of Parquet.*")

# Enable checkpointing for pipeline-persisted results
prefect.config.flows.checkpointing = True
email_address = PrefectSecret("email_address")

read_parquet_to_dataframe = drem.ReadParquetToDataFrame(
    name="Read Parquet file to DataFrame",
)
load_to_parquet = drem.LoadToParquet(name="Load Data to Parquet file")


with Flow("Extract, Transform & Load DREM Data") as flow:

    data_dir = Parameter("data_dir", default=DATA_DIR)
    external_dir = data_dir / "external"
    processed_dir = data_dir / "processed"

    sa_geometries_raw = drem.extract_sa_geometries(external_dir)
    sa_statistics_raw = drem.extract_sa_statistics(external_dir)
    sa_statistics_glossary = drem.extract_sa_glossary(external_dir)
    dublin_postcodes_raw = drem.extract_dublin_postcodes(external_dir)
    ber_raw = drem.extract_ber(email_address, external_dir)

    vo_raw = read_parquet_to_dataframe(filepath=external_dir / "vo_dublin.parquet")

    sa_geometries_clean = drem.transform_sa_geometries(sa_geometries_raw)
    dublin_postcodes_clean = drem.transform_dublin_postcodes(dublin_postcodes_raw)
    ber_clean = drem.transform_ber(ber_raw)
    sa_statistics_clean = drem.transform_sa_statistics(
        sa_statistics_raw,
        sa_statistics_glossary,
        sa_geometries_clean,
        dublin_postcodes_clean,
        ber_clean,
    )

    vo_clean = drem.transform_vo(vo_raw)

    load_to_parquet(sa_geometries_clean, processed_dir / "sa_geometries.parquet")
    load_to_parquet(sa_statistics_clean, processed_dir / "sa_statistics.parquet")
