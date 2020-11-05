# Dublin Region Energy Masterplan (drem)

![PyPI - License](https://img.shields.io/pypi/l/drem)
![build](https://github.com/codema-dev/drem/workflows/build/badge.svg)
[![Codecov](https://codecov.io/gh/codema-dev/drem/branch/master/graph/badge.svg)](https://codecov.io/gh/codema-dev/drem)
[![DeepSource](https://deepsource.io/gh/codema-dev/drem.svg/?label=active+issues&show_trend=true)](https://deepsource.io/gh/codema-dev/drem/?ref=repository-badge)
[![wemake-python-styleguide](https://img.shields.io/badge/style-wemake-000000.svg)](https://github.com/wemake-services/wemake-python-styleguide)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)


The goal of `drem` is to:

- Download Dublin energy-related data (from SEAI, the CSO etc.)
- Transform this data into a bottom-up energy demand model for Dublin

`drem` uses open-source software and open-access data to enable:
- Reproducibility
- Usage of 'Live'/up-to-date data sources


## Setup

- Download `drem` locally by clicking 'Clone or download' (or by running `git clone https://github.com/codema-dev/drem`)

- To install `drem`:

    - Install [docker](https://docs.docker.com/docker-for-windows/install/)

    - Install [Microsoft Visual Studio Code (VSCode)](https://code.visualstudio.com/)

    - Open the drem folder in Visual Studio Code

    - Install the “Remote - Containers” extension in VSCode from the Extensions: Marketplace (or directly from [here](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers))

    - Reopen the drem folder in a container by opening the Command Palette (via View > Command Palette or by Ctrl + Shft + P) and searching “Remote-Containers: Reopen in Container”

- To run `drem` (and consequently download & transform all `drem` related data):
    - Launch `Jupyter Notebook`:
        - Enter `jnbook` on the Command Line
            ```bash
            > /drem on master
            jnbook
            ```
        - Copy and paste the resulting URL to your browser (or Ctrl + Click)

            > It should look like http://127.0.0.1:8888/?token=aa69433d1370ab87a15436c27cd3f6948f77539a6bbeb6ee

    - Open `run-drem.ipynb` and ...
        - Enter your email address
        - Run all cells by selecting Cell > Run (or by manually running each cell via the Run button or by clicking each cell followed by Shft + Enter)

> (Optional) Set your VSCode Python Interpreter to your `poetry` `virtualenv` Python to enable `black`, `flake8`, `mypy`, `pre-commit`...
    - On the zsh command line enter:
        ```bash
        poetry shell
        which python
        ```

> The `drem` `Dockerfile` fully encapsulates all `drem` project dependencies (libraries, Operating System etc.) in a `docker` container.  Thanks to this encapsulation software developed in a docker container should run in the same manner on any computer with `docker` installed.

> For more information see [Developing inside a Container guide](https://code.visualstudio.com/docs/remote/containers)


---

## 'drem' relies on

Software:

- `prefect` to orchestrate all `drem` tasks via a data pipeline
- `pandas` to transform columnar data
- `geopandas` to transform columnar geospatial data
- `requests` to download data
- `pypostal` to standardise and parse address string columns
- `docker` to create a reproducible build environment that runs on Windows, OSX and Linux
- `git` to track code changes


Data:

- Residential buildings:

    - SEAI's 2016 Census Small Area [Statistics](https://www.cso.ie/en/media/csoie/census/census2016/census2016boundaryfiles/SAPS2016_SA2017.csv), [Geometries](https://data.gov.ie/dataset/small-areas-ungeneralised-osi-national-statistical-boundaries-2015) & [Glossary](https://www.cso.ie/en/media/csoie/census/census2016/census2016boundaryfiles/SAPS_2016_Glossary.xlsx).

    - SEAI's [BER Public Search](https://ndber.seai.ie/BERResearchTool/Register/Register.aspx): dwelling fabric information.

    - [Dublin Postcodes Geometries](https://github.com/rdmolony/dublin-postcode-shapefiles) (created by Shane McGuinness of Trinity College Dublin).

    - [CRU Smart Meter Trials 2009-10](https://www.ucd.ie/issda/data/commissionforenergyregulationcer/): 15-minute resolution demands & participant surveys _... available upon request_.

- Commercial:

    - [Valuation Office data](https://www.valoff.ie/en/open-data/api/): commercial building floor areas etc.

    - [CIBSE Energy Benchmarks](https://www.cibse.org/Knowledge/knowledge-items/detail?id=a0q20000008I7evAAC)

    - [SEAI Dublin Measurement & Verification data](https://www.seai.ie/): annual gas/electricity demands for Public sector buildings _... available upon request_.

> See [energy-modelling-ireland/energy-data-sources](https://github.com/energy-modelling-ireland/energy-data-sources) for more Irish-specific energy sources.


---


## Directory structure

Here's a brief overview of what each file and directory in `drem` does:
```
│
├── .github                 <- Scripts to run Github Actions CI
├── src                     <- Source code for use in this project.
│   ├── extract/            <- Scripts to download data
│   ├── transform/          <- ""         clean data
│   ├── load/               <- ""         load data to files or databases
│   └── etl.py              <- Orchestrates Extract, Transform, Load via prefect
│
├── tests                   <- Scripts to test src code via pytest
│
├── data
│   ├── external            <- Data from third party sources
│   ├── interim             <- Intermediate data that has been transformed
│   ├── processed           <- Final, canonical data sets for modeling
│   └── raw                 <- Original, immutable data dump (closed source)
│
├── externals               <- External libraries used by drem
├── .gitignore              <- Specifies files and folders to be ignored by source control
├── .pre-commit-config.yaml <- pre-commit hooks
├── LICENSE                 <- Terms & conditions for library usage etc.
├── README.md               <- Executive Summary of library
├── poetry.lock             <- Used by Poetry to store dependencies
├── pyproject.toml          <- ""             to setup library
├── Dockerfile              <- Used by docker to create the drem development environment
└── setup.cfg               <- Used by flake8 for linting style
```

> Inspired by [cookiecutter-data-science](https://github.com/drivendata/cookiecutter-data-science)

For more information see:
- [Extract, Transform, Load with Prefect](https://docs.prefect.io/core/tutorial/02-etl-flow.html)
- [Pytest](https://docs.pytest.org/en/latest/)
- [Github Actions](https://github.com/actions/setup-python) for Continuous Integration (CI)
- [Pre-commit hooks](https://pre-commit.com/)
- [Poetry](https://python-poetry.org/) for library setup
- [flake8](https://flake8.pycqa.org/en/latest/), [we-make-python-style-guide](https://wemake-python-stylegui.de/en/latest/pages/usage/violations/index.html) for checking style issues
- externals
    - [libpostal](https://github.com/openvenues/libpostal) enables fuzzy address matching
    - [nominatim-docker](https://github.com/mediagis/nominatim-docker) enables creation of local Nominatim server for geocoding at scale via OpenStreetMaps
