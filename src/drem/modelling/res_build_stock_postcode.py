"""
This script will create a synthetic residential building stock.

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
def _read_csv(input_filepath: str) -> pd.DataFrame:

    return pd.read_csv(input_filepath, encoding="unicode_escape").drop_duplicates()


@task
def _read_energy_csv(input_filepath: str) -> pd.DataFrame:

    return pd.read_csv(input_filepath)


@task
def _extract_ber_dublin(df: pd.DataFrame, on: str, contains: str) -> pd.DataFrame:

    return df[df[on].str.contains(contains)]


@task
def _lowercase_postcode(df: pd.DataFrame, new: str, old: str) -> pd.DataFrame:

    df[new] = df[old].str.lower()

    return df


@task
def _merge_ber_sa(
    left: pd.DataFrame, right: pd.DataFrame, left_on: str, right_on: str, **kwargs,
) -> pd.DataFrame:

    return left.merge(right, left_on=left_on, right_on=right_on, **kwargs)


@task
def _split_ber_construction(df: pd.DataFrame, condition: str) -> pd.DataFrame:

    return df.query(condition).drop_duplicates()


@task
def _extract_res(df: pd.DataFrame, on: str, value: str) -> pd.DataFrame:

    return df.loc[df[on] == value].drop_duplicates().reset_index(drop=True)


@task
def _transform_res(df: pd.DataFrame, lon: str, lat: str) -> gpd.GeoDataFrame:

    return gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(lon, lat))


@task
def _spatial_join(
    left: gpd.GeoDataFrame, right: gpd.GeoDataFrame, **kwargs,
) -> gpd.GeoDataFrame:

    right = right.set_crs(epsg="4326")

    return gpd.sjoin(left, right)


@task
def _assign_building_type(df: pd.DataFrame, on: str, equiv: list) -> pd.DataFrame:

    return df.replace({on: equiv})


@task
def _total_res_buildings_by_sa(
    gdf: gpd.GeoDataFrame, on: str, renamed: str,
) -> pd.DataFrame:

    return gdf[[on]].value_counts().rename(renamed).reset_index()


@task
def _apply_period_built_split(
    df: pd.DataFrame,
    sa: str,
    yc: str,
    split: str,
    year: int,
    post: str,
    pre: str,
    pre_post: str,
) -> pd.DataFrame:

    df = df.groupby([sa, yc])
    df = df.head(300000)
    df[yc] = df[yc].astype(int)
    df[split] = np.where(df[yc] >= year, post, pre)
    df = df.groupby(sa)[split].value_counts(normalize=True)
    df = df.to_frame()
    df = df.rename(columns={split: pre_post})

    return df.reset_index()


@task
def _count_buildings_by_sa(
    df: pd.DataFrame, by: str, on: str, renamed: str,
) -> pd.DataFrame:

    return df.groupby(by)[on].value_counts(normalize=True).rename(renamed).reset_index()


@task
def _final_count(df: pd.DataFrame, total: str, count: str, ratio: str) -> pd.DataFrame:

    df[total] = df[count] * df[ratio]

    return df


@task
def _add_energy_description(
    df: pd.DataFrame, new: str, left: str, right: str,
) -> pd.DataFrame:

    df[new] = df[left] + df[right]

    return df


@task
def _calculate_energy_by_sa(
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


with Flow("Create synthetic residential building stock") as flow:

    dublin_post = _read_sa_parquet(PROCESSED_DIR / "dublin_postcodes.parquet")
    post_geom = _read_sa_geometries(PROCESSED_DIR / "dublin_postcodes.parquet")
    ber = _read_csv(RAW_DIR / "BER.09.06.2020.csv")
    ber_dublin = _extract_ber_dublin(ber, on="CountyName2", contains="DUBLIN")
    ber_dub_lower = _lowercase_postcode(ber_dublin, new="postcode", old="CountyName2")
    dublin_post_lower = _lowercase_postcode(
        dublin_post, new="postcode", old="postcodes",
    )
    post_geom_lower = _lowercase_postcode(post_geom, new="postcode", old="postcodes")
    ber_postcode = _merge_ber_sa(
        left=dublin_post_lower,
        right=ber_dub_lower,
        left_on="postcode",
        right_on="postcode",
        how="inner",
        indicator=True,
    )
    energyplus = _read_energy_csv(PROCESSED_DIR / "energy_demand_by_building_type.csv")
    ber_assigned = _assign_building_type(
        ber_postcode,
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
    total_res = _total_res_buildings_by_sa(
        ber_assigned, on="postcode", renamed="total_dwellings",
    )
    ber_split = _split_ber_construction(
        df=ber_assigned, condition="`Year of construction`<1973",
    )
    period_built_split = _apply_period_built_split(
        df=ber_assigned,
        sa="postcode",
        yc="Year of construction",
        split="yc_split",
        year=1973,
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
        left=total_res,
        right=ber_grouped,
        left_on="postcode",
        right_on="postcode",
        how="inner",
    )
    output = _final_count(
        joined,
        total="total_buildings_per_postcode",
        count="total_dwellings",
        ratio="Dwelling Percentage",
    )
    output_split = _merge_ber_sa(
        left=period_built_split,
        right=output,
        left_on="postcode",
        right_on="postcode",
        how="inner",
    )
    output_dataframe = _final_count(
        output_split,
        total="total_sa_final",
        count="total_buildings_per_postcode",
        ratio="pre_post",
    )
    output_added = _add_energy_description(
        df=output_dataframe,
        new="building_energyplus",
        left="Dwelling type description",
        right="yc_split",
    )
    output_merged = _merge_ber_sa(
        left=output_added,
        right=energyplus,
        left_on="building_energyplus",
        right_on="dwelling",
        how="inner",
    )
    output_energy = _final_count(
        df=output_merged,
        total="energy_kwh",
        count="total_sa_final",
        ratio="total_site_energy_kwh",
    )
    energy_post = _calculate_energy_by_sa(
        df=output_energy,
        by="postcode",
        on="energy_kwh",
        renamed="energy_per_postcode_kwh",
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

if __name__ == "__main__":
    state = flow.run()
    flow.visualize(flow_state=state, filename=ROUGHWORK_DIR / "eplus-resi-model")