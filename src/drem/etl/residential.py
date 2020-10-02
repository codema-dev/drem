import warnings

import prefect

from prefect import Flow
from prefect import Parameter
from prefect.tasks.secrets import PrefectSecret

import drem

from drem.estimate.ber_archetypes import CreateBERArchetypes
from drem.estimate.sa_demand import EstimateSmallAreaDemand
from drem.filepaths import DATA_DIR
from drem.transform.ber import TransformBER
from drem.transform.sa_statistics import TransformSaStatistics


warnings.filterwarnings("ignore", message=".*initial implementation of Parquet.*")

# Enable checkpointing for pipeline-persisted results
prefect.config.flows.checkpointing = True

email_address = PrefectSecret("email_address")

read_parquet_to_dataframe = drem.ReadParquetToDataFrame(
    name="Read Parquet file to DataFrame",
)

estimate_sa_demand = EstimateSmallAreaDemand()
transform_sa_statistics = TransformSaStatistics()
transform_ber = TransformBER()
create_ber_archetypes = CreateBERArchetypes()

load_to_parquet = drem.LoadToParquet(name="Load Data to Parquet file")


with Flow("Extract, Transform & Load DREM Data") as flow:

    data_dir = Parameter("data_dir", default=DATA_DIR)
    external_dir = data_dir / "external"
    processed_dir = data_dir / "processed"
    raw_dir = data_dir / "raw"

    sa_geometries_raw = drem.extract_sa_geometries(external_dir)
    sa_statistics_raw = drem.extract_sa_statistics(external_dir)
    sa_statistics_glossary = drem.extract_sa_glossary(external_dir)
    dublin_postcodes_raw = drem.extract_dublin_postcodes(external_dir)
    ber_raw = drem.extract_ber(email_address, external_dir)

    ber_clean = transform_ber(ber_raw)
    ber_archetypes = create_ber_archetypes(ber_clean)

    sa_geometries_clean = drem.transform_sa_geometries(sa_geometries_raw)
    dublin_postcodes_clean = drem.transform_dublin_postcodes(dublin_postcodes_raw)
    sa_statistics_clean = transform_sa_statistics(
        raw_sa_statistics=sa_statistics_raw,
        raw_sa_glossary=sa_statistics_glossary,
        dublin_postcodes=dublin_postcodes_clean,
        dublin_sa_geometries=sa_geometries_clean,
    )

    sa_demand = estimate_sa_demand(
        sa_statistics_clean, ber_archetypes, sa_geometries_clean,
    )

    load_to_parquet(ber_clean, processed_dir / "ber.parquet")
    load_to_parquet(
        sa_statistics_clean["period_built"], processed_dir / "sa_period_built.parquet",
    )
    load_to_parquet(
        sa_statistics_clean["boiler_type"], processed_dir / "sa_boiler_type.parquet",
    )
    load_to_parquet(sa_demand, processed_dir / "sa_demand.parquet")
