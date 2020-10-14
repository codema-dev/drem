import warnings

from prefect import Flow
from prefect import Parameter
from prefect import Task
from prefect.engine.state import State
from prefect.tasks.secrets import PrefectSecret

from drem.download.ber import DownloadBER
from drem.estimate.ber_archetypes import create_ber_archetypes
from drem.estimate.sa_demand import estimate_sa_demand
from drem.filepaths import EXTERNAL_DIR
from drem.filepaths import PROCESSED_DIR
from drem.filepaths import VISUALIZATION_DIR
from drem.load.parquet import LoadToParquet
from drem.transform.ber import transform_ber
from drem.transform.cso_gas import transform_cso_gas
from drem.transform.dublin_postcodes import transform_dublin_postcodes
from drem.transform.sa_geometries import transform_sa_geometries
from drem.transform.sa_statistics import transform_sa_statistics
from drem.utilities import convert
from drem.utilities.download import Download
from drem.utilities.visualize import VisualizeMixin
from drem.utilities.zip import unzip


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


with Flow("Extract, Transform & Load DREM Data") as flow:

    external_dir = Parameter("external_dir", default=EXTERNAL_DIR)

    # Download all data
    # -----------------
    sa_statistics_downloaded = download_sa_statistics(
        savedir=external_dir,
        filename=small_area_statistics_filename,
        file_extension="csv",
    )
    sa_glossary_downloaded = download_sa_glossary(
        savedir=external_dir,
        filename=small_area_glossary_filename,
        file_extension="xlsx",
    )
    sa_geometries_downloaded = download_sa_geometries(
        savedir=external_dir,
        filename=small_area_geometries_filename,
        file_extension="zip",
    )
    dublin_postcodes_downloaded = download_dublin_postcode_geometries(
        savedir=external_dir,
        filename=dublin_postcode_geometries_filename,
        file_extension="zip",
    )
    ber_downloaded = download_ber(
        email_address=email_address, savedir=external_dir, filename=ber_filename,
    )
    cso_gas_downloaded = download_cso_gas(
        savedir=external_dir, filename=cso_gas_filename, file_extension="html",
    )

    # Unzip all zipped data folders
    # -----------------------------
    sa_geometries_unzipped = unzip(
        dirpath=external_dir, filename=small_area_geometries_filename,
    )
    dublin_postcodes_unzipped = unzip(
        dirpath=external_dir, filename=dublin_postcode_geometries_filename,
    )
    ber_unzipped = unzip(dirpath=external_dir, filename=ber_filename)

    # Convert all data to parquet for faster io
    # -----------------------------------------
    sa_statistics_converted = convert.csv_to_parquet(
        input_dirpath=external_dir,
        output_dirpath=external_dir,
        filename=small_area_statistics_filename,
    )
    sa_glossary_converted = convert.excel_to_parquet(
        input_dirpath=external_dir,
        output_dirpath=external_dir,
        filename=small_area_glossary_filename,
    )
    sa_geometries_converted = convert.shapefile_to_parquet(
        input_dirpath=external_dir,
        output_dirpath=external_dir,
        filename=small_area_geometries_filename,
    )
    dublin_postcodes_converted = convert.shapefile_to_parquet(
        input_dirpath=external_dir,
        output_dirpath=external_dir,
        filename=dublin_postcode_geometries_filename,
        path_to_shapefile="dublin-postcode-shapefiles-master/Postcode_dissolve",
    )
    ber_converted = convert.csv_to_parquet(
        input_dirpath=external_dir / ber_filename,
        output_dirpath=external_dir,
        filename=ber_filename,
        file_extension="txt",
        sep="\t",
        encoding="latin-1",
        error_bad_lines=False,
        low_memory=False,
    )

    # Clean data
    # ----------
    ber_clean = transform_ber(dirpath=external_dir, filename=ber_filename)
    ber_archetypes = create_ber_archetypes(ber_clean)

    sa_geometries_clean = transform_sa_geometries(
        dirpath=external_dir, filename=small_area_geometries_filename,
    )
    dublin_postcodes_clean = transform_dublin_postcodes(
        dirpath=external_dir, filename=dublin_postcode_geometries_filename,
    )
    sa_statistics_clean = transform_sa_statistics(
        dirpath=external_dir,
        sa_statistics_filename=small_area_statistics_filename,
        sa_glossary_filename=small_area_glossary_filename,
        dublin_postcodes=dublin_postcodes_clean,
        dublin_sa_geometries=sa_geometries_clean,
    )
    cso_gas_clean = transform_cso_gas(
        dirpath=external_dir,
        filename=cso_gas_filename,
        dublin_postcodes=dublin_postcodes_clean,
        small_area_boiler_statistics=sa_statistics_clean["boiler_type"],
    )

    sa_demand = estimate_sa_demand(
        sa_statistics_clean, ber_archetypes, sa_geometries_clean,
    )

    load_to_parquet(ber_clean, PROCESSED_DIR / "ber.parquet")
    load_to_parquet(
        sa_statistics_clean["period_built"], PROCESSED_DIR / "sa_period_built.parquet",
    )
    load_to_parquet(
        sa_statistics_clean["boiler_type"], PROCESSED_DIR / "sa_boiler_type.parquet",
    )
    load_to_parquet(
        cso_gas_clean["Residential"], PROCESSED_DIR / "cso_gas_residential.parquet",
    )
    load_to_parquet(
        cso_gas_clean["Non-Residential"],
        PROCESSED_DIR / "cso_gas_non_residential.parquet",
    )
    load_to_parquet(sa_demand, PROCESSED_DIR / "sa_demand.parquet")

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

    sa_statistics_clean.set_upstream(sa_statistics_converted)
    sa_statistics_clean.set_upstream(sa_glossary_converted)
    sa_geometries_clean.set_upstream(sa_geometries_converted)
    dublin_postcodes_clean.set_upstream(dublin_postcodes_converted)
    ber_clean.set_upstream(ber_converted)
    cso_gas_clean.set_upstream(cso_gas_downloaded)


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


if __name__ == "__main__":

    state = residential_etl.run()
    residential_etl.save_flow_visualization_to_file(
        savepath=VISUALIZATION_DIR / "etl" / "residential", flow=flow, flow_state=state,
    )
