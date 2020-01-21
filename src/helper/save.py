import geopandas as gpd
from pathlib import Path
import os
import shutil


def save_geodataframe(save_path: Path, gdf: gpd.GeoDataFrame) -> None:

    save_directory = Path(save_path).parent

    if os.path.exists(save_directory):
        shutil.rmtree(save_directory)
        os.mkdir(save_directory)
    else:
        os.mkdir(save_directory)

    gdf.to_file(save_path)
