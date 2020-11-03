# Dublin Region Energy Masterplan (drem)

![PyPI - License](https://img.shields.io/pypi/l/drem)
![build](https://github.com/codema-dev/drem/workflows/build/badge.svg)
[![Codecov](https://codecov.io/gh/codema-dev/drem/branch/master/graph/badge.svg)](https://codecov.io/gh/codema-dev/drem)
[![DeepSource](https://deepsource.io/gh/codema-dev/drem.svg/?label=active+issues&show_trend=true)](https://deepsource.io/gh/codema-dev/drem/?ref=repository-badge)
[![wemake-python-styleguide](https://img.shields.io/badge/style-wemake-000000.svg)](https://github.com/wemake-services/wemake-python-styleguide)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)


The goal of `drem` is to automate:

- Downloading of data from various sources
- Transforming this raw data into a usable format for the creation of a bottom-up energy model for Dublin

... to enable:
- Reproducibility
- Usage of 'Live'/up-to-date data sources

> This process is also known as [__Extract, Transform, Load__](https://en.wikipedia.org/wiki/Extract,_transform,_load) or etl.  Under the hood `drem` uses `prefect` to [chain Python functions in a `prefect` flow into a data pipeline](https://docs.prefect.io/core/tutorial/02-etl-flow.html).

You may also find `drem` useful to __download any data sets used in `drem` using simple Python commands__ rather than manually downloading them yourself from source (see [Basic Usage](#basic-usage)).

`drem` currently uses the following data sets:

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

`drem` uses:
- `prefect` to orchestrate the data pipeline using Python functions.
- `pandas` to clean columnar data
- `geopandas` to clean columnar geospatial data and geocode.
- `requests` to extract and download data via HTTP requests
- `pypostal` to standardise and parse address string columns



## Installation

__Recommended__: Via [`conda`](https://conda.io/en/latest/)

```bash
git clone https://github.com/codema-dev/drem
cd drem
conda env create --file=environment.yaml
conda activate drem-env
pip install -e .
```

> `drem` depends upon `GeoPandas` for geospatial analysis which depends on several low-level libraries which can be a challenge to install. It overcomes this barrier by using the [`conda`](https://conda.io/en/latest/) package manager.  This can be obtained by installing the [Anaconda Distribution](https://www.anaconda.com/products/individual) (a free Python distribution for data science), or through [miniconda](https://docs.conda.io/en/latest/miniconda.html) (minimal distribution only containing Python and the [`conda`](https://conda.io/en/latest/) package manager).


## Basic usage

To view & run all currently implemented functions run the following in [`iPython`](https://ipython.readthedocs.io/en/stable/): or in a [Jupyter Notebook](https://jupyter.org/)

```python
from pathlib import Path
from drem.etl import residential
residential.<TAB>   # to see available functions & classes
drem.download_ber?  # ? to get help with usage of individual functions
drem.download_ber.run(
        email_address="your-email-address", # WARNING: must register your email first at https://ndber.seai.ie/BERResearchTool/Register/Register.aspx
        savedir=Path.cwd(), # set your current-working-directory as your save directory
        filename="ber",
)
```

---

## Advanced Usage

To run the `drem` residential etl flow:

- Download this repository to your local hard-drive by clicking the green `Code` button and selecting `Download ZIP` or via `git clone https://github.com/codema-dev/drem`

- [Register your email address with SEAI](https://ndber.seai.ie/BERResearchTool/Register/Register.aspx)

- Save your SEAI-registered email address as an environmental variable to register it as a local [`prefect` secret](https://docs.prefect.io/core/concepts/secrets.html#overview) in bash/zsh

    > To run `drem` on `Windows` you'll have to manually install all non-Python dependencies yourself such as Visual Studio C++ build tools (see [here](https://mingw-w64.org/doku.php) or [here](https://github.com/felixrieseberg/windows-build-tools)), GDAL (see [here](https://www.lfd.uci.edu/~gohlke/pythonlibs/)) etc. or alternatively you could [Setup a Local Docker Development environment using Visual Studio Code](#setup-a-local-development-environment-using-visual-studio-code).

    ```bash
    export PREFECT__CONTEXT__SECRETS__email_address=your-email-address@some-domain.ie
    ```
- Run the flow in a python shell
    ```python
    from drem.etl.residential import flow

    state = flow.run()
    ```


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

---

## Setup a Local `drem` Development environment

- Download the `drem` repository locally by clicking 'Clone or download' or by running `git clone https://github.com/codema-dev/drem`


### Via `conda` only

1. Install the [Anaconda Distribution](https://www.anaconda.com/products/individual) (a free Python distribution for data science) or [miniconda](https://docs.conda.io/en/latest/miniconda.html) (minimal distribution only containing Python and the [`conda`](https://conda.io/en/latest/) package manager).

2. Install `drem` via `conda` using the `Anaconda` (or `miniconda`) prompt

    ```bash
    conda env create --file=environment.yaml
    conda activate drem-env
    pip install -e .
    ```

---

### Via `docker`

The `drem` `Dockerfile` fully encapsulates all `drem` project dependencies (libraries, Operating System etc.) in a `docker` container.  Thanks to this encapsulation software developed in a docker container should run in the same manner on any computer with `docker` installed.


1. Download [Microsoft Visual Studio Code (VSCode)](https://code.visualstudio.com/)

2. Reopen the `drem` folder within the `drem` `Dockerfile` container by following the instructions at:
    - [Developing inside a Container guide](https://code.visualstudio.com/docs/remote/containers)
    - [Reopen the `drem` folder in a container](https://code.visualstudio.com/docs/remote/create-dev-container)

3. Set your VSCode Python Interpreter to your `conda` virtualenv Python to enable `black`, `flake8`, `mypy`, `pre-commit`...
    - On the zsh command line enter:
        ```bash
        poetry shell
        which python
        ```

---

### [Optional] Develop in Windows Subsystem for Linux 2 (WSL2)

Another work-around is WSL2 if you work in Windows and wish to create your own development environment in Linux.  See [Install Windows Subsystem for Linux (WSL) in VSCode](https://code.visualstudio.com/docs/remote/wsl) for more information.
