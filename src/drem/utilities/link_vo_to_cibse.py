from pathlib import Path
from typing import Dict
from typing import List

import pandas as pd

import drem


def _convert_dataframe_to_dict_of_lists(
    vo_cibse_link: pd.DataFrame, category_col: str, uses_col: str,
) -> Dict[str, str]:
    """Convert pandas DataFrame into a Dictionary of Lists.

    Args:
        vo_cibse_link (pd.DataFrame): Data to be converted
        category_col (str): Column name of CIBSE 'category' column
        uses_col (str): Column name of Valuation Office 'uses' column

    Returns:
        Dict[str, str]: Converted output

    Example output:
        {"General Retail (TM:46)": ["KIOSK CLOTHES SHOP", ...]}
    """
    return {
        idx: group[uses_col].drop_duplicates().tolist()
        for idx, group in vo_cibse_link.groupby(category_col)
    }


def _save_dict_to_text_file(vo_by_category: Dict[str, str], savedir: Path) -> None:

    for key in vo_by_category.keys():

        filepath: Path = savedir / "".join([key, ".txt"])

        data: List[str] = ["".join([str(item), "\n"]) for item in vo_by_category[key]]

        with open(filepath, "a") as text_file:
            text_file.writelines(data)


def copy_vo_cibse_link_from_excel_to_text_files(
    filepath: Path, savedir: Path, category_col: str, uses_col: str,
) -> None:
    """Copy Valuation-Office-to-CIBSE-Benchmark links from Excel to yamls.

    Where each yaml represents a different CIBSE category... Can add new categories to
    these yamls in the future as they are added to the Valuation Office data...

    Args:
        filepath (Path): Filepath to Excel file containing labels
        savedir (Path): Path to directory where data will be saved
        category_col (str): Column name of CIBSE 'category' column
        uses_col (str): Column name of Valuation Office 'uses' column
    """
    vo_cibse_link: pd.DataFrame = pd.read_excel(filepath, engine="openpyxl")

    vo_cibse_link.loc[:, category_col] = vo_cibse_link[category_col].str.replace(
        "/", " or ", regex=True,
    )

    vo_by_category: Dict[str, str] = _convert_dataframe_to_dict_of_lists(
        vo_cibse_link, category_col, uses_col,
    )
    _save_dict_to_text_file(vo_by_category, savedir)


if __name__ == "__main__":

    copy_vo_cibse_link_from_excel_to_text_files(
        drem.filepaths.RAW_DIR / "cibse-vo.xlsx",
        drem.filepaths.LINKS_DIR,
        category_col="Reference Benchmark Used",
        uses_col="Property Use",
    )
    copy_vo_cibse_link_from_excel_to_text_files(
        drem.filepaths.ROUGHWORK_DIR / "vo-rows-not-caught-by-cibse.xlsx",
        drem.filepaths.LINKS_DIR,
        category_col="CATEGORY",
        uses_col="Uses",
    )
