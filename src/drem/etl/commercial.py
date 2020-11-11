import warnings

from os import path

from prefect import Flow

from drem.download.vo import DownloadValuationOffice
from drem.transform.benchmarks import transform_benchmarks
from drem.transform.vo import transform_vo
from drem.utilities.get_data_dir import get_data_dir


data_dir = get_data_dir()
external_dir = path.join(data_dir, "external")
processed_dir = path.join(data_dir, "processed")

benchmarks_dir = path.join(data_dir, "commercial_building_benchmarks")

warnings.filterwarnings("ignore", message=".*initial implementation of Parquet.*")

download_valuation_office = DownloadValuationOffice()

with Flow("Extract, Transform & Load DREM Data") as flow:

    valuation_office_downloaded = download_valuation_office(
        dirpath=external_dir,
        local_authorities=[
            "FINGAL COUNTY COUNCIL",
            "DUN LAOGHAIRE RATHDOWN CO CO",
            "DUBLIN CITY COUNCIL",
            "SOUTH DUBLIN COUNTY COUNCIL",
        ],
    )

    benchmarks = transform_benchmarks(benchmarks_dir)
    vo_clean = transform_vo(
        input_dirpath=path.join(external_dir, "vo"),
        output_filepath=path.join(processed_dir, "vo.parquet"),
        benchmarks_dirpath=benchmarks_dir,
        unmatched_filepath=path.join(benchmarks_dir, "Unmatched.txt"),
    )

    vo_clean.set_upstream(valuation_office_downloaded)
    vo_clean.set_upstream(benchmarks)
