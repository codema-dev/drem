import geopandas as gpd

from drem.filepaths import PROCESSED_DIR
from drem.filepaths import ROUGHWORK_DIR


sa_geoms = gpd.read_parquet(PROCESSED_DIR / "small_area_geometries_2016.parquet")
pcodes = gpd.read_parquet(PROCESSED_DIR / "dublin_postcodes.parquet")
vo = gpd.read_parquet(PROCESSED_DIR / "vo.parquet")
non_resi_gas = gpd.read_parquet(PROCESSED_DIR / "non_residential_postcode_gas.parquet")

vo_pcode = gpd.sjoin(pcodes, vo)
vo_sas = gpd.sjoin(sa_geoms, vo)

vo_pcode_elec_demand = (
    vo_pcode.groupby("postcodes")["estimated_electricity_kwh"].sum().reset_index()
)
vo_pcode_ff_demand = (
    vo_pcode.groupby("postcodes")["estimated_fossil_fuel_kwh"].sum().reset_index()
)
vo_sa_elec_demand = (
    vo_sas.groupby("small_area")["estimated_electricity_kwh"].sum().reset_index()
)
vo_sa_ff_demand = (
    vo_sas.groupby("small_area")["estimated_fossil_fuel_kwh"].sum().reset_index()
)

pcodes_elec_demand = pcodes.merge(vo_pcode_elec_demand)
pcodes_ff_demand = pcodes.merge(vo_pcode_ff_demand)
sas_elec_demand = sa_geoms.merge(vo_sa_elec_demand)
sas_ff_demand = sa_geoms.merge(vo_sa_ff_demand)

pcodes_elec_demand.to_file(ROUGHWORK_DIR / "vo_pcode_elec_demand")
pcodes_ff_demand.to_file(ROUGHWORK_DIR / "vo_pcode_ff_demand")
sas_elec_demand.to_file(ROUGHWORK_DIR / "vo_sa_elec_demand")
sas_ff_demand.to_file(ROUGHWORK_DIR / "vo_sa_ff_demand")
non_resi_gas.to_file(ROUGHWORK_DIR / "cso_non_resi_pcode_gas")
