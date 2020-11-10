from os import path
from pathlib import Path
from types import MappingProxyType

from drem.utilities.get_data_dir import get_data_dir


BASE_DIR = Path(__file__).parents[3]
DATA_DIR = get_data_dir()


INTERIM_DIR = path.join(DATA_DIR, "interim")
EXTERNAL_DIR = path.join(DATA_DIR, "external")
PROCESSED_DIR = path.join(DATA_DIR, "processed")
RAW_DIR = path.join(DATA_DIR, "raw")
ROUGHWORK_DIR = path.join(DATA_DIR, "roughwork")


COMMERCIAL_BENCHMARKS_DIR = str(BASE_DIR / "data" / "commercial_building_benchmarks")
DTYPES_DIR = str(BASE_DIR / "data" / "dtypes")
REQUESTS_DIR = str(BASE_DIR / "data" / "requests")


FTEST_EXTERNAL_DIR = str(BASE_DIR / "tests" / "functional" / "data" / "external")


DTYPES = MappingProxyType(
    {"BERPublicsearch_json": path.join(DTYPES_DIR, "BERPublicsearch.json")},
)


EXTERNAL = MappingProxyType(
    {
        "small_area_statistics_2016_csv": path.join(
            EXTERNAL_DIR, "small_area_statistics_2016.csv",
        ),
        "small_area_statistics_2016_parquet": path.join(
            EXTERNAL_DIR, "small_area_statistics_2016.parquet",
        ),
        "small_area_glossary_2016_xlsx": path.join(
            EXTERNAL_DIR, "small_area_glossary_2016.xlsx",
        ),
        "small_area_glossary_2016_parquet": path.join(
            EXTERNAL_DIR, "small_area_glossary_2016.parquet",
        ),
        "small_area_geometries_2016_zip": path.join(
            EXTERNAL_DIR, "small_area_geometries_2016.zip",
        ),
        "small_area_geometries_2016_shp": path.join(
            EXTERNAL_DIR, "small_area_geometries_2016",
        ),
        "small_area_geometries_2016_parquet": path.join(
            EXTERNAL_DIR, "small_area_geometries_2016_parquet",
        ),
        "dublin_postcode_geometries_zip": path.join(
            EXTERNAL_DIR, "dublin_postcodes.zip",
        ),
        "dublin_postcode_geometries_shp": path.join(
            EXTERNAL_DIR,
            "dublin_postcodes/dublin-postcode-shapefiles-master/Postcode_dissolve",
        ),
        "dublin_postcode_geometries_parquet": path.join(
            EXTERNAL_DIR, "dublin_postcodes.parquet",
        ),
        "cso_gas_2019_html": path.join(EXTERNAL_DIR, "cso_gas_2019.html"),
        "BERPublicsearch_zip": path.join(EXTERNAL_DIR, "BERPublicsearch.zip"),
        "BERPublicsearch_txt": path.join(
            EXTERNAL_DIR, "BERPublicsearch/BERPublicsearch.txt",
        ),
        "vo": path.join(EXTERNAL_DIR, "vo"),
        "commercial_benchmarks": path.join(DATA_DIR, "commercial_building_benchmarks"),
    },
)


INTERIM = MappingProxyType(
    {"BERPublicsearch_parquet": path.join(INTERIM_DIR, "BERPublicsearch.parquet")},
)


PROCESSED = MappingProxyType(
    {
        "small_area_statistics_2016_parquet": path.join(
            PROCESSED_DIR, "small_area_statistics_2016.parquet",
        ),
        "small_area_geometries_2016_parquet": path.join(
            PROCESSED_DIR, "small_area_geometries_2016_parquet",
        ),
        "dublin_postcode_geometries_parquet": path.join(
            PROCESSED_DIR, "dublin_postcodes.parquet",
        ),
        "BERPublicsearch_parquet": path.join(PROCESSED_DIR, "BERPublicsearch.parquet"),
    },
)
