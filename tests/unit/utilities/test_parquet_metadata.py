from pathlib import Path

import geopandas as gpd
import pandas as pd
import pyarrow.parquet as pq

from drem.utilities.parquet_metadata import add_file_engine_metadata_to_parquet_file


def test_add_pandas_engine_metadata_to_parquet_file(tmp_path) -> None:
    """Pandas engine metadata added.

    Args:
        tmp_path ([type]): Pytest temporary path plugin
    """
    filepath = tmp_path / "test_meta.parquet"
    pd.DataFrame().to_parquet(filepath)
    add_file_engine_metadata_to_parquet_file(filepath, "pandas")

    assert pq.read_metadata(filepath).metadata[b"file_engine"] == b"pandas"


def test_add_geopandas_engine_metadata_to_parquet_file(tmp_path: Path) -> None:
    """Geopandas engine metadata added.

    Args:
        tmp_path (Path): Pytest temporary path plugin
    """
    filepath = tmp_path / "test_meta.parquet"
    gpd.GeoDataFrame().to_parquet(filepath)
    add_file_engine_metadata_to_parquet_file(filepath, "geopandas")

    assert pq.read_metadata(filepath).metadata[b"file_engine"] == b"geopandas"
