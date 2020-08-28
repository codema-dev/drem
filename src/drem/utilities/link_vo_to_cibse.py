from pathlib import Path
from typing import Dict

import pandas as pd
import yaml


def _convert_dataframe_to_dict_of_lists(vo_cibse_link: pd.DataFrame) -> Dict[str, str]:
    """Convert pandas DataFrame into a Dictionary of Lists.

    Args:
        vo_cibse_link (pd.DataFrame): Data to be converted

    Returns:
        Dict[str, str]: Converted output

    Example output:
        {"General Retail (TM:46)": ["KIOSK CLOTHES SHOP", ...]}
    """
    return {
        idx: group["Uses"].drop_duplicates().tolist()
        for idx, group in vo_cibse_link.groupby("CATEGORY")
    }


def _save_dict_to_yamls(vo_by_category: Dict[str, str], savedir: Path) -> None:

    for key in vo_by_category.keys():

        with open(savedir / "".join([key, ".yaml"]), "w") as file:
            yaml.dump({key: vo_by_category[key]}, file)


def copy_vo_cibse_link_from_excel_to_yamls(filepath: Path, savedir: Path) -> None:
    """Copy Valuation-Office-to-CIBSE-Benchmark links from Excel to yamls.

    Where each yaml represents a different CIBSE category... Can add new categories to
    these yamls in the future as they are added to the Valuation Office data...

    Args:
        filepath (Path): Filepath to Excel file containing labels
        savedir (Path): Path to directory where data will be saved
    """
    vo_cibse_link: pd.DataFrame = pd.read_excel(filepath, engine="openpyxl")

    vo_cibse_link.loc[:, "CATEGORY"] = vo_cibse_link["CATEGORY"].str.replace(
        "/", " or ", regex=True,
    )

    vo_by_category: Dict[str, str] = _convert_dataframe_to_dict_of_lists(vo_cibse_link)
    _save_dict_to_yamls(vo_by_category, savedir)
