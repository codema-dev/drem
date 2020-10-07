from pathlib import Path

import requests

from icontract import require
from prefect import Task
from validate_email import validate_email

from drem.download.download import download_file_from_response
from drem.extract.read_json import read_json
from drem.filepaths import REQUESTS_DIR


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
    def run(
        self, email_address: str, savedir: Path, filename: str, file_extension: str,
    ) -> None:
        """Login & Download BER data.

        Warning:
            Email address must first be registered with SEAI at
                https://ndber.seai.ie/BERResearchTool/Register/Register.aspx

        Args:
            email_address (str): Registered Email address with SEAI
            savedir (Path): Save directory
            filename (str): File name
            file_extension (str): File extension (such as csv)
        """
        savepath = savedir / f"{filename}.{file_extension}"
        ber_form_data = read_json(REQUESTS_DIR / "ber_forms.json")

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
                download_file_from_response(response, savepath)