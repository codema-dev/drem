import warnings

from os import path
from typing import Optional

from prefect import Flow
from prefect import Task
from prefect.engine.state import State
from prefect.tasks.secrets import PrefectSecret

from drem import convert
from drem.download.ber import DownloadBER
from drem.estimate.ber_archetypes import create_ber_archetypes
from drem.estimate.sa_demand import estimate_sa_demand
from drem.filepaths import VISUALIZATION_DIR
from drem.load.parquet import LoadToParquet
from drem.transform.ber import transform_ber
from drem.transform.cso_gas import transform_cso_gas
from drem.transform.dublin_postcodes import transform_dublin_postcodes
from drem.transform.sa_geometries import transform_sa_geometries
from drem.transform.sa_statistics import transform_sa_statistics
from drem.utilities import convert as convert_util
from drem.utilities.download import Download
from drem.utilities.filepath_tasks import get_filepath
from drem.utilities.get_data_dir import get_data_dir
from drem.utilities.visualize import VisualizeMixin
from drem.utilities.zip import unzip as unzip_util


small_area_statistics_filename = "small_area_statistics_2016"
small_area_glossary_filename = "small_area_glossary_2016"
small_area_geometries_filename = "small_area_geometries_2016"
dublin_postcode_geometries_filename = "dublin_postcodes"
ber_filename = "BERPublicsearch"
cso_gas_filename = "cso_gas_2019"

warnings.filterwarnings("ignore", message=".*initial implementation of Parquet.*")

email_address = PrefectSecret("email_address")

download_sa_statistics = Download(
    name="Download Small Area Statistics",
    url="https://www.cso.ie/en/media/csoie/census/census2016/census2016boundaryfiles/SAPS2016_SA2017.csv",
)
download_sa_glossary = Download(
    name="Download Small Area Glossary",
    url="https://www.cso.ie/en/media/csoie/census/census2016/census2016boundaryfiles/SAPS_2016_Glossary.xlsx",
)
download_sa_geometries = Download(
    name="Download Small Area Geometries",
    url="http://data-osi.opendata.arcgis.com/datasets/c85e610da1464178a2cd84a88020c8e2_3.zip",
)
download_dublin_postcode_geometries = Download(
    name="Download Dublin Postcode Geometries",
    url="https://github.com/rdmolony/dublin-postcode-shapefiles/archive/master.zip",
)
download_ber = DownloadBER(name="Download Ireland BER Data")
download_cso_gas = Download(
    name="Download CSO 2019 Postcode Annual Network Gas Consumption",
    url="https://www.cso.ie/en/releasesandpublications/er/ngc/networkedgasconsumption2019/",
)

load_to_parquet = LoadToParquet(name="Load Data to Parquet file")

data_dir = get_data_dir()
external_dir = path.join(data_dir, "external")
interim_dir = path.join(data_dir, "interim")
processed_dir = path.join(data_dir, "processed")
dtypes_dir = path.join(data_dir, "dtypes")

with Flow("Extract, Transform & Load DREM Data") as flow:

    # Download all data
    # -----------------
    sa_statistics_downloaded = download_sa_statistics(
        filepath=get_filepath(external_dir, small_area_statistics_filename, ".csv"),
    )
    sa_glossary_downloaded = download_sa_glossary(
        filepath=get_filepath(external_dir, small_area_glossary_filename, ".xlsx"),
    )
    sa_geometries_downloaded = download_sa_geometries(
        filepath=get_filepath(external_dir, small_area_geometries_filename, ".zip"),
    )
    dublin_postcodes_downloaded = download_dublin_postcode_geometries(
        filepath=get_filepath(
            external_dir, dublin_postcode_geometries_filename, ".zip",
        ),
    )
    ber_downloaded = download_ber(
        email_address=email_address,
        filepath=get_filepath(external_dir, ber_filename, ".zip"),
    )
    cso_gas_downloaded = download_cso_gas(
        filepath=get_filepath(external_dir, cso_gas_filename, ".html"),
    )

    # Unzip all zipped data folders
    # -----------------------------
    sa_geometries_unzipped = unzip_util(
        input_filepath=get_filepath(
            external_dir, small_area_geometries_filename, ".zip",
        ),
        output_filepath=get_filepath(external_dir, small_area_geometries_filename, ""),
    )
    dublin_postcodes_unzipped = unzip_util(
        input_filepath=get_filepath(
            external_dir, dublin_postcode_geometries_filename, ".zip",
        ),
        output_filepath=get_filepath(
            external_dir, dublin_postcode_geometries_filename, "",
        ),
    )
    ber_unzipped = unzip_util(
        input_filepath=get_filepath(external_dir, ber_filename, ".zip"),
        output_filepath=get_filepath(external_dir, ber_filename, ""),
    )

    # Convert all data to parquet for faster io
    # -----------------------------------------
    sa_statistics_converted = convert_util.csv_to_parquet(
        input_filepath=get_filepath(
            external_dir, small_area_statistics_filename, ".csv",
        ),
        output_filepath=get_filepath(
            external_dir, small_area_statistics_filename, ".parquet",
        ),
    )
    sa_glossary_converted = convert_util.excel_to_parquet(
        input_filepath=get_filepath(
            external_dir, small_area_glossary_filename, ".xlsx",
        ),
        output_filepath=get_filepath(
            external_dir, small_area_glossary_filename, ".parquet",
        ),
    )
    sa_geometries_converted = convert_util.shapefile_to_parquet(
        input_filepath=get_filepath(external_dir, small_area_geometries_filename, ""),
        output_filepath=get_filepath(
            external_dir, small_area_geometries_filename, ".parquet",
        ),
    )
    dublin_postcodes_converted = convert_util.shapefile_to_parquet(
        input_filepath=get_filepath(
            external_dir,
            dublin_postcode_geometries_filename,
            "/dublin-postcode-shapefiles-master/Postcode_dissolve",
        ),
        output_filepath=get_filepath(
            external_dir, dublin_postcode_geometries_filename, ".parquet",
        ),
    )
    ber_converted = convert.berpublicsearch(
        input_filepath=path.join(external_dir, f"{ber_filename}.txt"),
        output_filepath=path.join(interim_dir, f"{ber_filename}.parquet"),
        dtypes_filepath=path.join(dtypes_dir, f"{ber_filename}.json"),
    )

    # Clean data
    # ----------
    ber_clean = transform_ber(
        input_filepath=get_filepath(external_dir, ber_filename, ".parquet"),
        output_filepath=get_filepath(processed_dir, ber_filename, ".parquet"),
    )
    sa_geometries_clean = transform_sa_geometries(
        input_filepath=get_filepath(
            external_dir, small_area_geometries_filename, ".parquet",
        ),
        output_filepath=get_filepath(
            processed_dir, small_area_geometries_filename, ".parquet",
        ),
    )
    dublin_postcodes_clean = transform_dublin_postcodes(
        input_filepath=get_filepath(
            external_dir, dublin_postcode_geometries_filename, ".parquet",
        ),
        output_filepath=get_filepath(
            processed_dir, dublin_postcode_geometries_filename, ".parquet",
        ),
    )
    sa_statistics_clean = transform_sa_statistics(
        input_filepath=get_filepath(
            external_dir, small_area_statistics_filename, ".parquet",
        ),
        sa_glossary_filepath=get_filepath(
            external_dir, small_area_glossary_filename, ".parquet",
        ),
        postcodes_filepath=get_filepath(
            processed_dir, dublin_postcode_geometries_filename, ".parquet",
        ),
        sa_geometries_filepath=get_filepath(
            processed_dir, small_area_geometries_filename, ".parquet",
        ),
        output_filepath_period_built=get_filepath(
            processed_dir, "small_area_period_built", ".parquet",
        ),
        output_filepath_boilers=get_filepath(
            processed_dir, "small_area_boilers", ".parquet",
        ),
    )
    cso_gas_clean = transform_cso_gas(
        input_filepath=get_filepath(external_dir, cso_gas_filename, ".html"),
        postcodes_filepath=get_filepath(
            processed_dir, dublin_postcode_geometries_filename, ".parquet",
        ),
        small_area_boilers_filepath=get_filepath(
            processed_dir, "small_area_boilers", ".parquet",
        ),
        output_filepath_residential_gas=get_filepath(
            processed_dir, "residential_postcode_gas", ".parquet",
        ),
        output_filepath_non_residential_gas=get_filepath(
            processed_dir, "non_residential_postcode_gas", ".parquet",
        ),
    )

    ber_archetypes = create_ber_archetypes(
        input_filepath=get_filepath(processed_dir, ber_filename, ".parquet"),
        output_filepath=get_filepath(processed_dir, "ber_archetypes", ".parquet"),
    )
    sa_demand = estimate_sa_demand(
        small_area_period_built_filepath=get_filepath(
            processed_dir, "small_area_period_built", ".parquet",
        ),
        ber_archetypes_filepath=get_filepath(
            processed_dir, "ber_archetypes", ".parquet",
        ),
        small_area_geometries_filepath=get_filepath(
            processed_dir, small_area_geometries_filename, ".parquet",
        ),
        output_filepath=get_filepath(
            processed_dir, "small_area_heat_demand_estimate", ".parquet",
        ),
    )

    # Define dependencies
    # -------------------
    sa_geometries_unzipped.set_upstream(sa_geometries_downloaded)
    dublin_postcodes_unzipped.set_upstream(dublin_postcodes_downloaded)
    ber_unzipped.set_upstream(ber_downloaded)

    sa_statistics_converted.set_upstream(sa_statistics_downloaded)
    sa_glossary_converted.set_upstream(sa_glossary_downloaded)
    sa_geometries_converted.set_upstream(sa_geometries_unzipped)
    dublin_postcodes_converted.set_upstream(dublin_postcodes_unzipped)
    ber_converted.set_upstream(ber_unzipped)

    ber_clean.set_upstream(ber_converted)
    dublin_postcodes_clean.set_upstream(dublin_postcodes_converted)
    sa_geometries_clean.set_upstream(sa_geometries_converted)

    sa_statistics_clean.set_upstream(sa_statistics_converted)
    sa_statistics_clean.set_upstream(sa_glossary_converted)
    sa_statistics_clean.set_upstream(sa_geometries_clean)
    sa_statistics_clean.set_upstream(dublin_postcodes_clean)

    cso_gas_clean.set_upstream(cso_gas_downloaded)
    cso_gas_clean.set_upstream(dublin_postcodes_clean)
    cso_gas_clean.set_upstream(sa_statistics_clean)

    ber_archetypes.set_upstream(ber_clean)

    sa_demand.set_upstream(sa_statistics_clean)
    sa_demand.set_upstream(ber_archetypes)
    sa_demand.set_upstream(sa_geometries_clean)


class ResidentialETL(Task, VisualizeMixin):
    """Create Residential ETL Task.

    Args:
        Task (Task): see https://docs.prefect.io/core/concepts/tasks.html
        VisualizeMixin (object): Mixin to add flow visualization method
    """

    def run(self) -> State:
        """Run Residential ETL Flow.

        Returns:
            State: see https://docs.prefect.io/core/concepts/results.html#result-objects
        """
        return flow.run()


residential_etl = ResidentialETL()


def visualize_subflows() -> None:
    """Create flow visualizations for each subflow."""
    transform_ber.save_flow_visualization_to_file(
        savepath=VISUALIZATION_DIR / "transform" / ber_filename,
        flow=transform_ber.flow,
    )
    transform_dublin_postcodes.save_flow_visualization_to_file(
        savepath=VISUALIZATION_DIR / "transform" / dublin_postcode_geometries_filename,
        flow=transform_dublin_postcodes.flow,
    )
    transform_sa_statistics.save_flow_visualization_to_file(
        savepath=VISUALIZATION_DIR / "transform" / small_area_statistics_filename,
        flow=transform_sa_statistics.flow,
    )
    transform_cso_gas.save_flow_visualization_to_file(
        savepath=VISUALIZATION_DIR / "transform" / cso_gas_filename,
        flow=transform_cso_gas.flow,
    )

    create_ber_archetypes.save_flow_visualization_to_file(
        savepath=VISUALIZATION_DIR / "estimate" / "ber_archetypes",
        flow=create_ber_archetypes.flow,
    )


def visualize_flow(flow_to_viz: Flow, flow_state: Optional[State] = None) -> None:
    """Visualize ETL flow.

    Args:
        flow_to_viz (Flow): Flow to be visualized
        flow_state (State, optional): Flow State result. Defaults to None.
    """
    residential_etl.save_flow_visualization_to_file(
        savepath=VISUALIZATION_DIR / "etl" / "residential",
        flow=flow_to_viz,
        flow_state=flow_state,
    )
