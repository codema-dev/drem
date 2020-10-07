import pandas as pd

from pandas.testing import assert_frame_equal

from drem.transform.sa_geometries import extract_dublin_local_authorities


def test_extract_dublin_local_authorities() -> None:
    """Extracted DataFrame contains only Dublin local authorities."""
    geometries = pd.DataFrame(
        {
            "COUNTYNAME": [
                "Kildare",
                "Dun Laoghaire-Rathdown",
                "Fingal",
                "South Dublin",
                "Dublin City",
            ],
        },
    )

    expected_output = pd.DataFrame(
        {
            "COUNTYNAME": [
                "Dun Laoghaire-Rathdown",
                "Fingal",
                "South Dublin",
                "Dublin City",
            ],
        },
    )

    output = extract_dublin_local_authorities(geometries).reset_index(drop=True)

    assert_frame_equal(
        output, expected_output,
    )
