from drem._filepaths import DATA_DIR

CSO_RESIDENTIAL_DATA = DATA_DIR / "external" / "SAPS2016_SA2017.csv"
CSO_RESIDENTIAL_GLOSSARY = DATA_DIR / "external" / "SAPS_2016_Glossary.xlsx"


def _extract_year_built(data: pd.DataFrame, glossary: pd.DataFrame,) -> pd.DataFrame:

    start_row = (
        glossary.query(
            "`Tables Within Themes` == 'Permanent private households by year built '"
        ).index.item()
        - 1
    )  # Need to subtract 1 as the relevant rows start one row above the text used to search

    end_row = (
        glossary.query(
            "`Tables Within Themes` == 'Permanent private households by type of occupancy '"
        ).index.item()
        - 1
    )

    year_built_glossary = (
        glossary.iloc[start_row:end_row][["Column Names", "Description of Field"]]
        .set_index("Column Names")
        .to_dict()["Description of Field"]
    )

    return (
        data.copy()
        .loc[:, ["GEOGID"] + list(year_built_glossary.keys())]
        .rename(columns=year_built_glossary)
    )


def _melt_year_built_columns(df: pd.DataFrame) -> pd.DataFrame:

    hh_columns = [col for col in df.columns if "households" in col]
    person_columns = [col for col in df.columns if "persons" in col]

    hh = pd.melt(
        df,
        value_vars=hh_columns,
        id_vars="GEOGID",
        var_name="period_built",
        value_name="households",
    )
    persons = pd.melt(
        df, value_vars=person_columns, id_vars="GEOGID", value_name="people"
    ).drop(columns=["GEOGID", "variable"])

    return pd.concat([hh, persons], axis=1)


def _clean_year_built_columns(df: pd.DataFrame) -> pd.DataFrame:

    return (
        df.copy()
        .assign(small_areas=lambda x: x["GEOGID"].str.replace(r"SA2017_", ""))
        .assign(
            period_built=lambda x: x["period_built"].str.replace(
                r" \(No. of households\)", ""
            )
        )
        .drop(columns="GEOGID")
    )


# def _extract_dublin_small_areas(df: pd.DataFrame) -> pd.DataFrame:


@prefect.task
def cso_residential(data_path: Path, glossary_path: Path):

    glossary = pd.read_excel(glossary_path)
    data = pd.read_csv(data_path)

    return (
        data.pipe(_extract_year_built, glossary)
        .pipe(_melt_year_built_columns)
        .pipe(_clean_year_built_columns)
    )

