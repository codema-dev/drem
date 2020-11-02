import geopandas as gpd

from drem.filepaths import PROCESSED_DIR
from drem.filepaths import ROUGHWORK_DIR


sa_geoms = gpd.read_parquet(PROCESSED_DIR / "small_area_geometries_2016.parquet")
pcodes = gpd.read_parquet(PROCESSED_DIR / "dublin_postcodes.parquet")
resi_gas = gpd.read_parquet(PROCESSED_DIR / "residential_postcode_gas.parquet")
ber_sa_estimate = gpd.read_parquet(
    PROCESSED_DIR / "small_area_heat_demand_estimate.parquet",
)

ber_pcode = gpd.sjoin(pcodes, ber_sa_estimate)
ber_pcode_est = (
    ber_pcode[["postcodes", "total_heat_demand_per_sa_kwh"]]
    .groupby("postcodes")["total_heat_demand_per_sa_kwh"]
    .sum()
)

pcodes_ber_est = pcodes.merge(ber_pcode_est)

resi_gas.to_file(ROUGHWORK_DIR / "cso_resi_pcode_gas")
pcodes_ber_est.to_file(ROUGHWORK_DIR / "ber_pcode_heat")
