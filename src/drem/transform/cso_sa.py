import pandas as pd
import geopandas as gpd
from drem.filepaths import RAW_DIR

# Spatially linking BER dataset to Geodir on SA level
ber = pd.read_parquet(RAW_DIR / "BER_Closed.parquet")
ber_dub = ber[ber["CountyName2"].str.contains("DUBLIN")]
ber_dub = ber_dub.drop_duplicates()
ber_extracted = ber_dub[
    [
        "cso_small_area",
        "CountyName2",
        "Year of construction",
        "Year of construction range",
        "Dwelling type description",
        "Energy Rating",
    ]
]
ber_renamed = ber_extracted.rename(columns={"CountyName2": "Dublin Postcode"})

census = gpd.read_file(RAW_DIR / "Census2011_Small_Areas_generalised20m.shp")
census_extracted = census[["SMALL_AREA", "geometry"]]
census_renamed = census_extracted.rename(columns={"SMALL_AREA": "cso_small_area"})

ber_sa = census_renamed.merge(ber_renamed, on="cso_small_area", how="inner")
ber_sa = ber_sa.sort_values("cso_small_area")

ber_crs = gpd.GeoDataFrame(ber_sa)
ber_crs = ber_crs.to_crs(epsg=32629)

# df_output from geodir.py
geodir_in_ber = gpd.sjoin(ber_crs, df_output, op="contains")
geo_dropped = geodir_in_ber.drop_duplicates(subset="Name", keep="last")
# The construction period that's here is invalid and needs reassigning

geo_dropped["Dwelling Group"] = geo_dropped["Dwelling type description"].map(
    {
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
    }
)

dwelling = ["Apartment", "Semi detatched house", "Detatched house", "Terraced"]

geo_dwelling = geo_dropped[
    geo_dropped["Dwelling Group"].str.contains("Apartment", na=False)
]
geo_dwelling.groupby(["Dublin Postcode"])[["Year of construction"]].median()
