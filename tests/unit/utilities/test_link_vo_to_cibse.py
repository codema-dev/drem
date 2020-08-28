from os import listdir
from pathlib import Path

import pandas as pd
import pytest

from drem.utilities.link_vo_to_cibse import copy_vo_cibse_link_from_excel_to_yamls


@pytest.fixture
def path_to_vo_cibse_link(tmp_path: Path) -> Path:
    """Create dummy data of vo to cibse link.

    Args:
        tmp_path (Path): A Pytest plugin to create a temporary path

    Returns:
        Path: Path to dummy data file location
    """
    vo_cibse_link: pd.DataFrame = pd.DataFrame(
        {
            "Uses": [
                "KIOSK CLOTHES SHOP",
                "COFFEE SHOP STORE",
                "MISCELLANEOUS SURGERY",
            ],
            "CATEGORY": [
                "General Retail (TM:46)",
                "Retail: Small Food Shop",
                "Clinic: Health center/Clinic/Surgery (TM46)",
            ],
        },
    )

    path_to_vo_cibse_link: Path = tmp_path / "link-vo-to-cibse.xlsx"

    vo_cibse_link.to_excel(path_to_vo_cibse_link, engine="openpyxl")

    return path_to_vo_cibse_link


def test_copy_vo_cibse_link_from_excel_to_yamls(
    path_to_vo_cibse_link: Path, tmp_path: Path,
) -> None:
    """Copy VO-to-CIBSE links to a yaml named after each category containing uses.

    Args:
        path_to_vo_cibse_link (Path): Path to vo-cibse-link dummy data
        tmp_path (Path): A Pytest plugin to create a temporary path
    """
    copy_vo_cibse_link_from_excel_to_yamls(path_to_vo_cibse_link, tmp_path)

    assert "General Retail (TM:46).yaml" in listdir(tmp_path)
