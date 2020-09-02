from pathlib import Path

import pandas as pd
import pytest

from pandas.testing import assert_frame_equal

from drem.transform.benchmarks import (
    _read_text_files_linking_benchmarks_to_vo_to_dataframe,
)


@pytest.fixture
def text_files_linking_benchmarks_to_vo(tmp_path: Path) -> Path:
    """Generate a directory containing text files linking benchmarks to vo categories.

    Args:
        tmp_path (Path): A pytest plugin to generate a temporary path

    Returns:
        Path: A path to a directory containing text files linking benchmarks to vo
          categories.
    """
    cibse_category_links = {
        "Bar, Pub or Licensed Club (TM:46)": ["PUB\n"],
        "Catering: Fast Food Restaurant": ["TAKE AWAY\n", "RESTAURANT TAKE AWAY\n"],
    }
    for category, vo_uses in cibse_category_links.items():
        with open(tmp_path / "".join([category, ".txt"]), "w") as file:
            file.writelines(vo_uses)

    return tmp_path


@pytest.fixture
def benchmarks_linked_to_vo() -> pd.DataFrame:
    """Generate a DataFrame which links benchmarks to vo categories.

    Returns:
        [pd.DataFrame]: DataFrame which links benchmarks to vo categories
    """
    return pd.DataFrame(
        {
            "cibse_benchmarks": [
                "Bar, Pub or Licensed Club (TM:46)",
                "Catering: Fast Food Restaurant",
                "Catering: Fast Food Restaurant",
            ],
            "vo_uses": ["PUB", "TAKE AWAY", "RESTAURANT TAKE AWAY"],
        },
    )


def test_read_text_files_linking_benchmarks_to_vo_to_dataframe(
    text_files_linking_benchmarks_to_vo: Path, benchmarks_linked_to_vo: pd.DataFrame,
) -> None:
    """Read text files to dataframe.

    Args:
        text_files_linking_benchmarks_to_vo (Path): A path to a directory containing
          text files linking benchmarks to vo categories.
        benchmarks_linked_to_vo (pd.DataFrame): DataFrame which links benchmarks to vo
          categories
    """
    output: pd.DataFrame = _read_text_files_linking_benchmarks_to_vo_to_dataframe(
        text_files_linking_benchmarks_to_vo,
    )

    expected_output = benchmarks_linked_to_vo

    assert_frame_equal(output, expected_output)


# def test_merge_benchmark_links_with_benchmark_values()
