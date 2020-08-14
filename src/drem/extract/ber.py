from os import remove
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

from icontract import require
from prefect import task
from validate_email import validate_email

from drem.extract.download import download_file_from_response
from drem.extract.read_json import read_json
from drem.extract.unzip import unzip_file
from drem.filepaths import REQUESTS_DIR


CWD: Path = Path.cwd()


def _download_ber(email_address: str, filepath: Path) -> None:

    # Get forms stored in same directory as this script
    ber_form_data = read_json(REQUESTS_DIR / "ber_forms.json")

    with requests.Session() as session:

        # Register login email address in form
        ber_form_data["login"][
            "ctl00$DefaultContent$Register$dfRegister$Name"
        ] = email_address

        # Login to BER Research Tool using email address
        session.post(
            url="https://ndber.seai.ie/BERResearchTool/Register/Register.aspx",
            headers=ber_form_data["headers"],
            data=ber_form_data["login"],
        )

        # Download Ber data via a post request
        with session.post(
            url="https://ndber.seai.ie/BERResearchTool/ber/search.aspx",
            headers=ber_form_data["headers"],
            data=ber_form_data["download_all_data"],
            stream=True,
        ) as response:

            response.raise_for_status()
            download_file_from_response(response, filepath)

    unzip_file(filepath)


@task
@require(
    lambda email_address: validate_email(email_address), "Email address is invalid!",
)
def extract_ber(email_address: str, savedir: Optional[Path] = CWD) -> pd.DataFrame:
    """Download SEAI's BER Public search data set from their website.

    Warning! You must first register your email address with SEAI to use this function

    Args:
        email_address (str): address that has been egistered with SEAI
        savedir (Path, optional): save directory for data. Defaults to your current
        working directory (i.e. CWD)

    Returns:
        pd.DataFrame: Downloaded raw ber data
    """
    filepath: Path = savedir / "BERPublicsearch.parquet"

    if not filepath.exists():

        _download_ber(email_address, filepath.with_suffix(".zip"))

        ber_raw: pd.DataFrame = pd.read_csv(
            filepath.with_suffix(".txt"),
            sep="\t",
            encoding="latin-1",
            error_bad_lines=False,
            low_memory=False,
        )

        remove(filepath.with_suffix(".zip"))
        remove(filepath.with_suffix(".txt"))
        remove(filepath.with_name("ReadMe.txt"))

        ber_raw.to_parquet(filepath)

    return pd.read_parquet(filepath)
