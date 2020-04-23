from typing import Union

import geopandas as gpd
import numpy as np
import pandas as pd
from icontract import ensure, require, ViolationError
from pipeop import pipes
from prefect import Flow, task
import prefect

from codema_drem.utilities.flow import run_flow
from codema_drem.utilities.icontract import (
    columns_are_in_dataframe,
    error_rows_lost_in_merge,
    no_null_values_in_result_dataframe,
    no_null_values_in_result_dataframe_columns,
    error_null_values_in_result_dataframe,
    merge_column_dtypes_are_equivalent,
    no_empty_values_in_merge_columns,
)
from codema_drem._filepaths import (
    DATA_DIR,
    PLOT_DIR,
    BASE_DIR,
    CENSUS_CLEAN,
    BER_CLEAN,
    DUBLIN_EDS_CLEAN,
    SYN_RESID_RESULTS,
    SYN_RESID_FLOW,
)
from codema_drem._globals import (
    RANDOM_SEED,
)  # Used for random number generation by NumPy
from codema_drem.plot.plot_synthetic_residential import (
    _plot_sr_alldublin_energy_ratings,
)

# TODO: run 100 simulations using 100 different RANDOM_SEEDS & plot energy ratings


@pipes
def synthetic_residential_model_flow(save=True) -> Flow:

    with Flow("Create synthetic residential building stock") as flow:

        eds = _load_eds()
        census = _load_census2016()
        ber = _load_ber()

        census_with_postcodes = _link_census_to_postcodes(census, eds)
        expanded_census = _expand_census_to_individual_buildings(census_with_postcodes)

        postcode_archetype_totals = _extract_postcode_archetype_totals(ber)
        alldublin_archetype_totals = _extract_alldublin_archetype_totals(ber)

        ber_with_archetype_ids = (
            _add_column_postcode_archetype_id(ber)
            >> _add_column_alldublin_archetype_id
            >> _convert_merge_column_categoricals_to_strings
        )

        census_with_archetype_totals = (
            _link_to_ber_postcode_archetype_totals(
                expanded_census, postcode_archetype_totals
            )
            >> _link_to_ber_alldublin_archetype_totals(alldublin_archetype_totals)
            >> _flag_small_archetype_sample_sizes
        )

        census_with_postcode_archetypes = (
            _remove_postcode_archetypes_with_small_sample_sizes(
                census_with_archetype_totals
            )
            >> _select_a_random_matching_postcode_archetype
            >> _link_to_matching_postcode_archetypes(ber_with_archetype_ids)
        )

        census_with_alldublin_archetypes = (
            _remove_postcode_archetypes_with_large_sample_sizes(
                census_with_archetype_totals
            )
            >> _select_a_random_matching_alldublin_archetype
            >> _link_to_matching_alldublin_archetypes(ber_with_archetype_ids)
        )

        census_with_archetypes = _concatenate_archetypes(
            census_with_postcode_archetypes, census_with_alldublin_archetypes
        )

        if save:
            _save_dataframe_result(census_with_archetypes)

        # _plot_sr_alldublin_energy_ratings(census_with_archetypes)

    return flow


# NOTE: run_flow(flow_function=synthetic_residential_model_flow, viz_path=SYN_RESID_FLOW)


# ---------------------------------------------------------------------------


@task
@ensure(lambda result: columns_are_in_dataframe({"eds", "postcodes"}, result))
def _load_eds() -> gpd.GeoDataFrame:

    return gpd.read_file(DUBLIN_EDS_CLEAN)


@task
@ensure(
    lambda result: columns_are_in_dataframe(
        {"eds", "dwelling_type", "period_built", "total_hh"}, result
    )
)
def _load_census2016() -> pd.DataFrame:

    return pd.read_parquet(CENSUS_CLEAN)


@task
def _load_ber() -> pd.DataFrame:

    return pd.read_parquet(BER_CLEAN)


@task
@ensure(lambda census, result: len(census) == len(result))
@ensure(lambda result: no_null_values_in_result_dataframe(result))
def _link_census_to_postcodes(
    census: pd.DataFrame, eds: gpd.GeoDataFrame,
) -> pd.DataFrame:

    return census.merge(eds[["eds", "postcodes"]])


@task
@ensure(lambda census, result: census.total_hh.sum() == len(result))
@ensure(lambda census, result: np.setdiff1d(census.eds, result.eds).size == 0)
def _expand_census_to_individual_buildings(census: pd.DataFrame,) -> pd.DataFrame:
    """Expands census out to a DataFrame with one row per building

        Parameters
        ----------
        census : pd.DataFrame

        Returns
        -------
        pd.DataFrame
        """

    """NOTE
        https://stackoverflow.com/questions/32792263/duplicate-row-based-on-value-in-different-column"""

    return (
        census.loc[census.index.repeat(census.total_hh)]
        .drop(columns="total_hh")
        .reset_index(drop=True)
    )


@task
@ensure(lambda census, result: census.total_hh.sum() == len(result))
def _expand_census_to_individual_buildings(census: pd.DataFrame,) -> pd.DataFrame:
    """Expands census out to a DataFrame with one row per building

        Parameters
        ----------
        census : pd.DataFrame

        Returns
        -------
        pd.DataFrame
        """

    """NOTE
        https://stackoverflow.com/questions/32792263/duplicate-row-based-on-value-in-different-column"""

    expanded_census = (
        census.loc[census.index.repeat(census.total_hh)]
        .drop(columns="total_hh")
        .reset_index(drop=True)
    )

    eds_lost = np.setdiff1d(census.eds, expanded_census.eds)

    logger = prefect.context.get("logger")
    logger.info(f"\n\nFollowing Electoral Districts have been lost: {eds_lost}\n")

    return expanded_census


@task
def _extract_alldublin_archetype_totals(ber: pd.DataFrame) -> pd.DataFrame:
    """Aggregates BER data on dwelling_type | period_built columns to create
    all-of-Dublin BER archetypes
    
    Example; for archetype partments built between 1919 - 1945 calculates the 
    total number of households in the BER dataset matching this description
    in Dublin 

    Parameters
    ----------
    ber : pd.DataFrame

    Returns
    -------
    pd.DataFrame
        ber aggregated to alldublin archetype totals
    """
    return (
        ber.groupby(["dwelling_type", "period_built"], sort=False)["dwelling_type"]
        .count()
        .reset_index(name="alldublin_archetype_total")
    )


@task
def _extract_postcode_archetype_totals(ber: pd.DataFrame) -> pd.DataFrame:
    """Aggregates BER data on dwelling_type | period_built columns to create
    all-of-Dublin BER archetypes

    Example; for archetype partments built between 1919 - 1945 calculates the
    total number of households in the BER dataset matching this description
    in Dublin

    Parameters
    ----------
    ber : pd.DataFrame

    Returns
    -------
    pd.DataFrame
        ber aggregated to alldublin archetype totals
    """
    return (
        ber.groupby(["postcodes", "dwelling_type", "period_built"], sort=False)[
            "dwelling_type"
        ]
        .count()
        .reset_index(name="postcode_archetype_total")
    )


@task
def _add_column_postcode_archetype_id(ber: pd.DataFrame) -> pd.DataFrame:
    """Use pandas.DataFrame.groupby.cumcount to track the number of
        occurances of each archetype.
        Parameters
        ----------
        ber : pd.DataFrame

        Returns
        -------
        pd.DataFrame
            ber dataset with a column cumulatively counting the number of
            occurances of each archetype
        """

    """NOTE
        https://stackoverflow.com/questions/53912388/pandas-force-one-to-one-merge-on-column-containing-duplicate-keys"""
    return ber.assign(
        postcode_archetype_id=(
            ber.groupby(["postcodes", "dwelling_type", "period_built"])
            .cumcount()
            .apply(lambda x: x + 1)  # add 1 to each row as cumcount starts at 0
        )
    )


@task
def _add_column_alldublin_archetype_id(ber: pd.DataFrame) -> pd.DataFrame:
    """Use pandas.DataFrame.groupby.cumcount to track the number of
        occurances of each archetype.
        Parameters
        ----------
        ber : pd.DataFrame

        Returns
        -------
        pd.DataFrame
            ber dataset with a column cumulatively counting the number of
            occurances of each archetype
        """

    """NOTE
        https://stackoverflow.com/questions/53912388/pandas-force-one-to-one-merge-on-column-containing-duplicate-keys"""
    return ber.assign(
        alldublin_archetype_id=(
            ber.groupby(["dwelling_type", "period_built"])
            .cumcount()
            .apply(lambda x: x + 1)  # add 1 to each row as cumcount starts at 0
        )
    )


@task
def _convert_merge_column_categoricals_to_strings(ber: pd.DataFrame) -> pd.DataFrame:

    merge_columns = [
        "postcodes",
        "dwelling_type",
        "period_built",
    ]

    ber.loc[:, merge_columns] = ber[merge_columns].astype(str)

    return ber


@task
@require(
    lambda postcode_archetype_totals: "postcode_archetype_total"
    in postcode_archetype_totals.columns
)
def _link_to_ber_postcode_archetype_totals(
    census: pd.DataFrame, postcode_archetype_totals: pd.DataFrame,
) -> pd.DataFrame:
    """Where possible links ber postcode archetype totals to each census HH.
    This enables random sampling of postcode archetypes from the ber dataset...

    Note! 
    The BER dataset doesn't necessarily contain at least one unique 
    building for each postcode > dwelling_type > period_built.  For example; 
    Dublin 11 may not have any apartments built in 1919-1945.  Therefore need
    to merge ber separately with alldublin archetypes as the alldublin will 
    fill in the missing HH... 
    
    Parameters
    ----------
    census : pd.DataFrame
    postcode_archetype_totals: pd.DataFrame

    Returns
    -------
    pd.DataFrame
        A census DataFrame with each buildings linked to its corresponding
        postcode archetype total
    """

    return pd.merge(
        left=census,
        right=postcode_archetype_totals,
        on=["postcodes", "dwelling_type", "period_built"],
        how="left",
    )


@task
@require(
    lambda alldublin_archetype_totals: "alldublin_archetype_total"
    in alldublin_archetype_totals.columns
)
@ensure(
    lambda result: no_null_values_in_result_dataframe_columns(
        "alldublin_archetype_total", result
    )
)
def _link_to_ber_alldublin_archetype_totals(
    census: pd.DataFrame, alldublin_archetype_totals: pd.DataFrame,
) -> pd.DataFrame:
    """Links ber alldublin archetype totals to each census HH.
    This enables random sampling of alldublin archetypes from the ber dataset.
    The alldublin archetype is used for all buildings with a small sample (or
    none) representation in the postcode archetype
    
    Parameters
    ----------
    census : pd.DataFrame
    ber_postcode_archetype_totals: pd.DataFrame

    Returns
    -------
    pd.DataFrame
        A census DataFrame with each buildings linked to its corresponding
        postcode archetype total
    """

    return pd.merge(
        left=census,
        right=alldublin_archetype_totals,
        on=["dwelling_type", "period_built"],
        how="left",
    )


@task
def _flag_small_archetype_sample_sizes(census: pd.DataFrame) -> pd.DataFrame:
    """Flag all archetypes with a sample size < 30 so that a sample from
        all of Dublin can be used instead of an archetype sample

        Parameters
        ----------
        census : pd.DataFrame

        Returns
        -------
        pd.DataFrame
            census data with small archetype samples flagged
        """

    cutoff_point = 30

    return census.assign(
        postcode_archetype_sample_too_small=(
            census["postcode_archetype_total"] < cutoff_point
        )
    )


@task
@require(lambda census: "postcode_archetype_sample_too_small" in census.columns)
def _remove_postcode_archetypes_with_small_sample_sizes(
    census: pd.DataFrame,
) -> pd.DataFrame:

    return census[~census["postcode_archetype_sample_too_small"]]


@task
@ensure(
    lambda result: no_null_values_in_result_dataframe_columns(
        "postcode_archetype_id", result
    )
)
def _select_a_random_matching_postcode_archetype(census: pd.DataFrame) -> pd.DataFrame:

    # pick a building id at random between 1 and archetype_total
    random_number_generator = np.random.default_rng(seed=RANDOM_SEED)
    building_ids = random_number_generator.integers(
        low=1, high=census["postcode_archetype_total"].values,
    )
    census["postcode_archetype_id"] = building_ids

    return census


@task
@require(
    lambda census, ber: merge_column_dtypes_are_equivalent(
        census,
        ber,
        merge_columns=[
            "postcodes",
            "dwelling_type",
            "period_built",
            "postcode_archetype_id",
        ],
    ),
)
@ensure(lambda result: not result.empty)
def _link_to_matching_postcode_archetypes(
    census: pd.DataFrame, ber: pd.DataFrame,
) -> pd.DataFrame:

    merge_columns = [
        "postcodes",
        "dwelling_type",
        "period_built",
        "postcode_archetype_id",
    ]

    return pd.merge(census, ber, on=merge_columns)


@task
@require(lambda census: "postcode_archetype_sample_too_small" in census.columns)
def _remove_postcode_archetypes_with_large_sample_sizes(
    census: pd.DataFrame,
) -> pd.DataFrame:

    return census[census["postcode_archetype_sample_too_small"]]


@task
@ensure(lambda result: "alldublin_archetype_id" in result.columns)
@ensure(
    lambda result: no_null_values_in_result_dataframe_columns(
        "alldublin_archetype_id", result
    )
)
def _select_a_random_matching_alldublin_archetype(census: pd.DataFrame) -> pd.DataFrame:

    # pick a building id at random between 1 and archetype_total
    rng = np.random.default_rng(seed=1)  # random number generator
    building_ids = rng.integers(low=1, high=census["alldublin_archetype_total"].values,)
    census["alldublin_archetype_id"] = building_ids

    return census


@task
@require(
    lambda census, ber: merge_column_dtypes_are_equivalent(
        census,
        ber,
        merge_columns=[
            "postcodes",
            "dwelling_type",
            "period_built",
            "alldublin_archetype_id",
        ],
    ),
)
@ensure(lambda result: not result.empty)
def _link_to_matching_alldublin_archetypes(
    census: pd.DataFrame, ber: pd.DataFrame,
) -> pd.DataFrame:

    merge_columns = [
        "postcodes",
        "dwelling_type",
        "period_built",
        "alldublin_archetype_id",
    ]

    return pd.merge(census, ber, on=merge_columns,)


@task
def _concatenate_archetypes(postcode_archetypes, alldublin_archetypes):

    return pd.concat([postcode_archetypes, alldublin_archetypes])


@task
def _save_dataframe_result(df: Union[pd.DataFrame, gpd.GeoDataFrame]) -> None:

    df.to_parquet(SYN_RESID_RESULTS)
