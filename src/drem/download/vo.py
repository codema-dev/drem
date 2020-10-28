from os import mkdir
from os import path
from typing import List

import requests

from loguru import logger
from prefect import Task

from drem.filepaths import EXTERNAL_DIR
from drem.utilities.download import download


class DownloadValuationOffice(Task):
    """Download Valuation Office Data via Prefect.

    Args:
        Task (prefect.Task): see https://docs.prefect.io/core/concepts/tasks.html
    """

    def run(self, dirpath: str, local_authorities: List[str]):
        """Download Local Authority Valuation Office Data category-by-category.

        Args:
            dirpath (str): Path to directory where data will be saved
            local_authorities (List[str]): Names of local authorities to be queried
        """

        savedir = path.join(dirpath, "vo")
        mkdir(savedir)

        categories = [
            "OFFICE",
            "FUEL/DEPOT",
            "LEISURE",
            "INDUSTRIAL USES",
            "HEALTH",
            "HOSPITALITY",
            "MINERALS",
            "MISCELLANEOUS",
            "RETAIL (SHOPS)",
            "UTILITY",
            "RETAIL (WAREHOUSE)",
            "NO CATEGORY SELECTED",
            "CENTRAL VALUATION LIST",
            "CHECK CATEGORY",
            "NON-LIST",
            "NON-LIST EXEMPT",
        ]

        for local_authority in local_authorities:
            for category in categories:
                category_without_slashes = category.replace("/", " or ")
                try:
                    download(
                        url=f"https://api.valoff.ie/api/Property/GetProperties?Fields=*&LocalAuthority={local_authority}&CategorySelected={category}&Format=csv&Download=true",
                        filepath=path.join(
                            savedir,
                            f"{local_authority} - {category_without_slashes}.csv",
                        ),
                    )

                except requests.HTTPError as error:
                    logger.info(error)


if __name__ == "__main__":
    download_valuation_office = DownloadValuationOffice()
    download_valuation_office.run(
        dirpath=EXTERNAL_DIR,
        local_authorities=[
            "FINGAL COUNTY COUNCIL",
            "DUN LAOGHAIRE RATHDOWN CO CO",
            "DUBLIN CITY COUNCIL",
            "SOUTH DUBLIN COUNTY COUNCIL",
        ],
    )
