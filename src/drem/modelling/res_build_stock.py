"""
This script will create postcode-level estimates for residential energy consumption.

Our methodology is to divide each SA into several building types,
using the Split-Apply-Combine pandas transform. These ratios will
then be applied to outputs from EnergyPlus to generate a first-pass
estimate for residential energy demand in Dublin.

"""
import geopandas as gpd
import numpy as np
import pandas as pd

from prefect import Flow
from prefect import task

from drem.filepaths import PROCESSED_DIR
from drem.filepaths import RAW_DIR
from drem.filepaths import ROUGHWORK_DIR


@task
def _read_sa_parquet(input_filepath: str) -> pd.DataFrame:

    return pd.read_parquet(input_filepath).drop_duplicates()


@task
def _read_sa_geometries(input_filepath: str) -> gpd.GeoDataFrame:

    return gpd.read_parquet(input_filepath)


@task
def _read_ed_geometries(input_filepath: str) -> pd.DataFrame:

    return gpd.read_file(input_filepath)


@task
def _read_csv(input_filepath: str) -> pd.DataFrame:

    return pd.read_csv(input_filepath, encoding="unicode_escape").drop_duplicates()


@task
def _read_energy_csv(input_filepath: str) -> pd.DataFrame:

    return pd.read_csv(input_filepath)


@task
def _extract_ber_dublin(df: pd.DataFrame, on: str, contains: str) -> pd.DataFrame:

    return df[df[on].str.contains(contains)]


@task
def _set_postcodes_to_lowercase(df: pd.DataFrame, new: str, old: str) -> pd.DataFrame:

    df[new] = df[old].str.lower()

    return df


@task
def _merge_ber_sa(
    left: pd.DataFrame, right: pd.DataFrame, left_on: str, right_on: str, **kwargs,
) -> pd.DataFrame:

    return left.merge(right, left_on=left_on, right_on=right_on, **kwargs)


@task
def _merge_cso_ed(
    left: pd.DataFrame, right: pd.DataFrame, left_on: str, right_on: str, **kwargs,
) -> pd.DataFrame:

    return left.merge(right, left_on=left_on, right_on=right_on, **kwargs)


@task
def _extract_cso_columns(
    df: pd.DataFrame, ed: str, res: str, geometry: str,
) -> pd.DataFrame:

    return df[[ed, res, geometry]]


@task
def _split_ber_by_year_of_construction(
    df: pd.DataFrame, condition: str,
) -> pd.DataFrame:

    return df.query(condition).drop_duplicates()


@task
def _spatial_join(
    left: gpd.GeoDataFrame, right: pd.DataFrame, **kwargs,
) -> gpd.GeoDataFrame:

    right = gpd.GeoDataFrame(right)
    right = right.to_crs(epsg="4326")

    return gpd.sjoin(left, right, **kwargs)


@task
def _drop_ed_duplicates(df: pd.DataFrame, on: str) -> pd.DataFrame:

    return df.drop_duplicates(subset=[on])


@task
def _assign_building_type(df: pd.DataFrame, on: str, equiv: list) -> pd.DataFrame:

    return df.replace({on: equiv})


@task
def _assign_energy_rating_to_numerical(
    df: pd.DataFrame, number: str, rating: str, equiv: list,
) -> pd.DataFrame:

    df[number] = df[rating].map(equiv)

    return df


@task
def _count_resi_buildings_in_each_postcode_on_column(
    gdf: gpd.GeoDataFrame, on: str, renamed: str,
) -> pd.DataFrame:

    return gdf[[on]].value_counts().rename(renamed).reset_index()


@task
def _apply_ber_rating_split(
    df: pd.DataFrame,
    small_area: str,
    year_of_construction: str,
    split: str,
    year: int,
    post: str,
    pre: str,
    pre_post: str,
) -> pd.DataFrame:

    df = df.groupby([small_area, year_of_construction])
    df = df.head(300000)
    df[year_of_construction] = df[year_of_construction].astype(int)
    df[split] = np.where(df[year_of_construction] <= year, post, pre)
    df = df.groupby(small_area)[split].value_counts(normalize=True)
    df = df.to_frame()
    df = df.rename(columns={split: pre_post})

    return df.reset_index()


@task
def _count_buildings_by_ed(
    df: pd.DataFrame, by: str, on: str, renamed: str,
) -> pd.DataFrame:

    return df.groupby(by)[on].sum().rename(renamed).reset_index()


@task
def _count_buildings_by_pcode(df: pd.DataFrame, by: str, on: str) -> pd.DataFrame:

    return df.groupby(by)[on].sum().reset_index()


@task
def _count_buildings_by_sa(
    df: pd.DataFrame, by: str, on: str, renamed: str,
) -> pd.DataFrame:

    return df.groupby(by)[on].value_counts(normalize=True).rename(renamed).reset_index()


@task
def _calculate_split_buildings_per_postcode(
    df: pd.DataFrame, total: str, count: str, ratio: str,
) -> pd.DataFrame:

    df[total] = df[count] * df[ratio]

    return df


@task
def _calculate_demand_per_postcode(
    df: pd.DataFrame, total: str, count: str, ratio: str,
) -> pd.DataFrame:

    df[total] = df[count] * df[ratio]

    return df


@task
def _match_building_description_to_energyplus(
    df: pd.DataFrame, new: str, left: str, right: str,
) -> pd.DataFrame:

    df[new] = df[left] + df[right]

    return df


@task
def _calculate_energy_by_postcode(
    df: pd.DataFrame, by: str, on: str, renamed: str,
) -> pd.DataFrame:

    return df.groupby(by)[on].sum().rename(renamed).reset_index()


@task
def _merge_postcode_geometries(
    left: pd.DataFrame, right: pd.DataFrame, left_on: str, right_on: str, **kwargs,
) -> pd.DataFrame:

    return left.merge(right, left_on=left_on, right_on=right_on, **kwargs)


@task
def _extract_plotting_columns(
    df: pd.DataFrame, postcode: str, energy: str, geometry: str,
) -> pd.DataFrame:

    return df[[postcode, energy, geometry]]


@task
def _merge_result(
    energy: pd.DataFrame,
    elec: pd.DataFrame,
    heat: pd.DataFrame,
    peak_elec: pd.DataFrame,
    on: str,
) -> pd.DataFrame:

    return energy.merge(elec, on=on).merge(heat, on=on).merge(peak_elec, on=on)


@task
def _create_shapefile(df: pd.DataFrame, driver: str, path: str) -> gpd.GeoDataFrame:

    gdf = gpd.GeoDataFrame(df)

    return gdf.to_file(driver=driver, filename=path)


@task
def _convert_postcode_to_sa(df: pd.DataFrame) -> pd.DataFrame:

    df_loc = df.iloc[:, 310:339]
    df_loc["sum"] = df_loc.sum(axis=1)
    df = pd.DataFrame(df["GEOGID"])
    df_sum = pd.concat([df, df_loc], axis=1)
    df_tot = df_sum[["GEOGID", "sum"]]
    df_tot["GEOGID"] = df_tot["GEOGID"].str.replace("SA2017_", "")

    return df_tot


@task
def _small_area_centroid_join(
    df: pd.DataFrame, pcode: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:

    gdf = gpd.GeoDataFrame(df)
    gdf["centroids"] = gdf.geometry.centroid
    df_ext = gdf[["GEOGID", "sum", "centroids"]]
    df_ext = df_ext.rename(columns={"centroids": "geometry"})
    gdf = gpd.GeoDataFrame(df_ext)
    gdf = gdf.set_crs(epsg="4326")
    gdf2 = gpd.GeoDataFrame(pcode)

    return gpd.sjoin(gdf, gdf2, op="within")


@task
def _compute_sa_demands(df: pd.DataFrame) -> pd.DataFrame:

    df = df.groupby("postcodes")["sum"].sum()

    return pd.DataFrame(df).reset_index()


@task
def _link_sa_pcode_count(df: pd.DataFrame) -> pd.DataFrame:

    df["portion"] = df["sum_x"] / df["sum_y"]
    df["sa_demand_kwh"] = df["energy_per_postcode_kwh"] * df["portion"]

    return df[["GEOGID", "sa_demand_kwh", "geometry"]]


@task
def _link_sa_centroids_geom(demand: pd.DataFrame, geom: pd.DataFrame) -> pd.DataFrame:

    df = demand.merge(geom, left_on="GEOGID", right_on="small_area", how="inner")
    df = df.rename(columns={"geometry_y": "geometry"})

    return df[["GEOGID", "sa_demand_kwh", "geometry"]]


with Flow("Create synthetic residential building stock") as flow:

    dublin_post = _read_sa_parquet(PROCESSED_DIR / "dublin_postcodes.parquet")
    post_geom = _read_sa_geometries(PROCESSED_DIR / "dublin_postcodes.parquet")
    ber = _read_csv(RAW_DIR / "BER.09.06.2020.csv")
    ed_geom = _read_ed_geometries(RAW_DIR / "cso_ed/dublin_ed_geometries_cso.shp")
    cso_crosstab = _read_csv(RAW_DIR / "census2016_ed_crosstab.csv")
    ber_dublin = _extract_ber_dublin(ber, on="CountyName2", contains="DUBLIN")
    ber_dub_lower = _set_postcodes_to_lowercase(
        ber_dublin, new="postcode", old="CountyName2",
    )
    dublin_post_lower = _set_postcodes_to_lowercase(
        dublin_post, new="postcode", old="postcodes",
    )
    post_geom_lower = _set_postcodes_to_lowercase(
        post_geom, new="postcode", old="postcodes",
    )
    cso_geom_total = _count_buildings_by_ed(
        cso_crosstab, by="ed_2016", on="value_2016_inferred", renamed="total_resi_ed",
    )
    ed_lower = _set_postcodes_to_lowercase(ed_geom, new="ed_lower", old="ED_ENGLISH")
    ed_merged = _merge_cso_ed(
        left=cso_geom_total,
        right=ed_lower,
        left_on="ed_2016",
        right_on="ed_lower",
        how="inner",
    )
    cso_extracted = _extract_cso_columns(
        ed_merged, ed="ed_lower", res="total_resi_ed", geometry="geometry",
    )
    cso_postcode = _spatial_join(left=post_geom, right=cso_extracted, how="right")
    cso_dropped = _drop_ed_duplicates(cso_postcode, on="ed_lower")
    cso_final = _count_buildings_by_ed(
        cso_dropped, by="postcode", on="total_resi_ed", renamed="res_per_postcode",
    )
    ber_postcode = _merge_ber_sa(
        left=dublin_post_lower,
        right=ber_dub_lower,
        left_on="postcode",
        right_on="postcode",
        how="inner",
        indicator=True,
    )
    ber_numbered = _assign_energy_rating_to_numerical(
        ber_postcode,
        number="Energy_Number",
        rating="Energy Rating",
        equiv={
            "A1": 1,
            "A2": 2,
            "A3": 3,
            "B1": 4,
            "B2": 5,
            "B3": 6,
            "C1": 7,
            "C2": 8,
            "C3": 9,
            "D1": 10,
            "D2": 11,
            "E1": 12,
            "E2": 13,
            "F": 14,
            "G": 15,
        },
    )
    energyplus = _read_energy_csv(PROCESSED_DIR / "energy_demand_by_building_type.csv")
    ber_assigned = _assign_building_type(
        ber_numbered,
        on="Dwelling type description",
        equiv={
            "Mid floor apt.": "Apartment",
            "Top-floor apt.": "Apartment",
            "Apt.": "Apartment",
            "Maisonette": "Apartment",
            "Grnd floor apt.": "Apartment",
            "Semi-det. house": "Semi detatched house",
            "House": "Semi detatched house",
            "Det. house": "Detatched house",
            "Mid terrc house": "Terraced house",
            "End terrc house": "Terraced house",
            "None": "Not stated",
        },
    )
    ber_split = _split_ber_by_year_of_construction(
        df=ber_assigned, condition="`Energy_Number`<6",
    )
    ber_rating_split = _apply_ber_rating_split(
        df=ber_assigned,
        small_area="postcode",
        year_of_construction="Energy_Number",
        split="er_split",
        year=6,
        post="post",
        pre="pre",
        pre_post="pre_post",
    )
    ber_grouped = _count_buildings_by_sa(
        ber_split,
        by="postcode",
        on="Dwelling type description",
        renamed="Dwelling Percentage",
    )
    joined = _merge_ber_sa(
        left=cso_final,
        right=ber_grouped,
        left_on="postcode",
        right_on="postcode",
        how="inner",
    )
    output = _calculate_split_buildings_per_postcode(
        joined,
        total="total_buildings_per_postcode",
        count="res_per_postcode",
        ratio="Dwelling Percentage",
    )
    output_split = _merge_ber_sa(
        left=ber_rating_split,
        right=output,
        left_on="postcode",
        right_on="postcode",
        how="inner",
    )
    output_dataframe = _calculate_split_buildings_per_postcode(
        output_split,
        total="total_sa_final",
        count="total_buildings_per_postcode",
        ratio="pre_post",
    )
    output_added = _match_building_description_to_energyplus(
        df=output_dataframe,
        new="building_energyplus",
        left="Dwelling type description",
        right="er_split",
    )
    output_merged = _merge_ber_sa(
        left=output_added,
        right=energyplus,
        left_on="building_energyplus",
        right_on="dwelling",
        how="inner",
    )
    output_energy = _calculate_demand_per_postcode(
        df=output_merged,
        total="energy_kwh",
        count="total_sa_final",
        ratio="total_site_energy_kwh",
    )
    output_elec = _calculate_demand_per_postcode(
        df=output_energy,
        total="elec_kwh",
        count="total_sa_final",
        ratio="total_site_elec_kwh",
    )
    output_heat = _calculate_demand_per_postcode(
        df=output_elec,
        total="heat_kwh",
        count="total_sa_final",
        ratio="total_site_heat_kwh",
    )
    output_peak_elec = _calculate_demand_per_postcode(
        df=output_elec,
        total="peak_elec_kw",
        count="total_sa_final",
        ratio="peak_elec_demand_kw",
    )
    energy_post = _calculate_energy_by_postcode(
        df=output_energy,
        by="postcode",
        on="energy_kwh",
        renamed="energy_per_postcode_kwh",
    )
    elec_post = _calculate_energy_by_postcode(
        df=output_elec, by="postcode", on="elec_kwh", renamed="elec_per_postcode_kwh",
    )
    heat_post = _calculate_energy_by_postcode(
        df=output_heat, by="postcode", on="heat_kwh", renamed="heat_per_postcode_kwh",
    )
    peak_elec_post = _calculate_energy_by_postcode(
        df=output_peak_elec,
        by="postcode",
        on="peak_elec_kw",
        renamed="peak_elec_per_postcode_kw",
    )
    postcode_final = _merge_result(
        energy=energy_post,
        elec=elec_post,
        heat=heat_post,
        peak_elec=peak_elec_post,
        on="postcode",
    )
    energy_plot = _merge_postcode_geometries(
        left=energy_post,
        right=post_geom_lower,
        left_on="postcode",
        right_on="postcode",
        how="inner",
    )
    energy_plot_final = _extract_plotting_columns(
        energy_plot,
        postcode="postcodes",
        energy="energy_per_postcode_kwh",
        geometry="geometry",
    )
    output_shape = _create_shapefile(
        energy_plot_final,
        driver="ESRI Shapefile",
        path=PROCESSED_DIR / "resi_demand_shape",
    )
    saps = _read_energy_csv(ROUGHWORK_DIR / "SAPS2016_SA2017.csv")
    sa = _read_sa_geometries(PROCESSED_DIR / "small_area_geometries_2016.parquet")
    pcode_count = _convert_postcode_to_sa(saps)
    count_merge = _merge_ber_sa(
        left=pcode_count,
        right=sa,
        left_on="GEOGID",
        right_on="small_area",
        how="inner",
    )
    sa_pcode = _small_area_centroid_join(count_merge, energy_plot_final)
    total_pcode = _count_buildings_by_pcode(sa_pcode, by="postcodes", on="sum")
    pcode_count = _merge_ber_sa(
        left=sa_pcode,
        right=total_pcode,
        left_on="postcodes",
        right_on="postcodes",
        how="inner",
        indicator=True,
    )
    sa_demand = _link_sa_pcode_count(pcode_count)
    sa_dem_geom = _link_sa_centroids_geom(sa_demand, sa)

if __name__ == "__main__":
    state = flow.run()
    flow.visualize(flow_state=state, filename=ROUGHWORK_DIR / "eplus-resi-model")
