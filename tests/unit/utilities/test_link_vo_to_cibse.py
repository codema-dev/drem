from os import listdir
from pathlib import Path
from typing import List

import numpy as np
import pandas as pd
import pytest

from drem.utilities.link_vo_to_cibse import copy_vo_cibse_link_from_excel_to_text_files


@pytest.fixture
def vo_cibse_link_excel(tmp_path: Path) -> Path:
    """Create dummy data of vo - cibse link.

    Args:
        tmp_path (Path): A Pytest plugin to create a temporary path

    Returns:
        Path: Path to dummy excel file location
    """
    vo_cibse_link: pd.DataFrame = pd.DataFrame(
        {
            "Uses": [
                "KIOSK CLOTHES SHOP",
                "COFFEE SHOP STORE",
                "MISCELLANEOUS SURGERY",
                np.nan,
            ],
            "CATEGORY": [
                "General Retail (TM:46)",
                "Retail: Small Food Shop",
                "Clinic: Health center/Clinic/Surgery (TM46)",
                "Clinic: Health center/Clinic/Surgery (TM46)",
            ],
        },
    )

    vo_cibse_link_excel: Path = tmp_path / "link-vo-to-cibse.xlsx"

    vo_cibse_link.to_excel(vo_cibse_link_excel, engine="openpyxl")

    return vo_cibse_link_excel


@pytest.fixture
def vo_cibse_link_excel_new(tmp_path: Path) -> Path:
    """Create new dummy data of vo - cibse link.

    Args:
        tmp_path (Path): A Pytest plugin to create a temporary path

    Returns:
        Path: Path to dummy excel file location
    """
    vo_cibse_link_new: pd.DataFrame = pd.DataFrame(
        {"Uses": ["COFFEE SHOP"], "CATEGORY": ["Retail: Small Food Shop"]},
    )

    vo_cibse_link_excel_new: Path = tmp_path / "link-vo-to-cibse-new.xlsx"

    vo_cibse_link_new.to_excel(vo_cibse_link_excel_new, engine="openpyxl")

    return vo_cibse_link_excel_new


@pytest.fixture
def dir_with_text_files(vo_cibse_link_excel: Path, tmp_path: Path) -> Path:
    """Path to directory containing vo-cibse link yamls.

    Args:
        vo_cibse_link_excel (Path): Path to dummy vo-cibse link excel data
        tmp_path (Path): A Pytest plugin to create a temporary path

    Returns:
        Path: Path to directory containing dummy text files
    """
    copy_vo_cibse_link_from_excel_to_text_files(
        vo_cibse_link_excel, tmp_path, category_col="CATEGORY", uses_col="Uses",
    )

    return tmp_path


def test_copy_vo_cibse_link_from_excel_to_text_files(dir_with_text_files: Path) -> None:
    """Copy VO-to-CIBSE links from excel to new text file.

    Where each file is named after a category and contains corresponding uses.

    Args:
        dir_with_text_files (Path): Path to directory containing dummy text files
    """
    assert "General Retail (TM:46).txt" in listdir(dir_with_text_files)


def test_copy_vo_cibse_link_from_excel_to_text_files_appends(
    vo_cibse_link_excel_new: Path, dir_with_text_files: Path,
) -> None:
    """Append VO-to-CIBSE links from Excel to existing text file.

    Where each file is named after a category and contains corresponding uses.

    Args:
        vo_cibse_link_excel_new (Path): Path to new vo-cibse-link dummy data
        dir_with_text_files (Path): A Pytest plugin to create a temporary path
    """
    retail_text_file: Path = dir_with_text_files / "Retail: Small Food Shop.txt"

    copy_vo_cibse_link_from_excel_to_text_files(
        vo_cibse_link_excel_new,
        dir_with_text_files,
        category_col="CATEGORY",
        uses_col="Uses",
    )

    with open(retail_text_file, "r") as text_file:
        retail_values: List[str] = [item.strip() for item in text_file.readlines()]

    assert set(retail_values) == {"COFFEE SHOP STORE", "COFFEE SHOP"}
