import json
from os import path

from pathlib import Path

import requests

from icontract import require
from prefect import Task
from validate_email import validate_email

from drem.filepaths import REQUESTS_DIR
from drem.utilities.download import download_file_from_response


CWD: Path = Path.cwd()


class DownloadBER(Task):
    """Download BER via Prefect.

    Args:
        Task (prefect.Task): see https://docs.prefect.io/core/concepts/tasks.html
    """

    @require(
        lambda email_address: validate_email(email_address),
        "Email address is invalid!",
    )
    def run(self, email_address: str, filepath: str) -> None:
        """Login & Download BER data.

        Warning:
            Email address must first be registered with SEAI at
                https://ndber.seai.ie/BERResearchTool/Register/Register.aspx

        Args:
            email_address (str): Registered Email address with SEAI
            filepath (str): Path to data
        """
        if path.exists(filepath):
            self.logger.info(f"Skipping download as {filepath} already exists!")

        else:
            with open(REQUESTS_DIR / "ber_forms.json", "r") as json_file:
                ber_form_data = json.load(json_file)

            # Register login email address in form
            ber_form_data["login"][
                "ctl00$DefaultContent$Register$dfRegister$Name"
            ] = email_address

            with requests.Session() as session:

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
