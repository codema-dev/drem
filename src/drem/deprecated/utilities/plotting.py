import geopandas as gpd
import logging
import codema_drem
from codema_drem.utilities.logging import create_logger
from pathlib import Path


def plot_geodf_to_file(
    gdf: gpd.GeoDataFrame,
    save_path: Path,
    logger: logging.Logger = None,
    column: str = None,
    categorical: bool = False,
):

    gdf = gdf.copy()

    if logger:
        logger.info(f"Saving plot to: {save_path}")

    if column:
        (
            gdf.plot(
                column=column,
                legend=True,
                categorical=categorical,
                missing_kwds={
                    "color": "lightgrey",
                    "edgecolor": "red",
                    "hatch": "///",
                    "label": "Missing values",
                },
            )
            .get_figure()
            .savefig(save_path)
        )
    else:
        (gdf.plot(color="white", edgecolor="black",).get_figure().savefig(save_path))


# Plot pcode-ed join result in ipy
# eds.plot(column='Postcodes', categorical=True, legend=True,
# figsize=(10,20), legend_kwds={'bbox_to_anchor':(-2,1.05)}).get_figure()
# .savefig(BASE_DIR/'plots'/'eds_linked.png')
