from os import path
from types import MappingProxyType

from drem.utilities.get_data_dir import get_data_dir


DATA_DIR = get_data_dir()
EXTERNAL_DIR = path.join(DATA_DIR, "external")
PROCESSED_DIR = path.join(DATA_DIR, "processed")

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
        "BERPublicsearch_parquet": path.join(EXTERNAL_DIR, "BERPublicsearch.parquet"),
    },
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
