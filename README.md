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

- To install `drem`:

    > __Warning__: you must accept all permissions-related requests made by `VSCode` and docker (to unblock your firewall and grant access to your C-Drive)

    - Install [`docker`](https://docs.docker.com/docker-for-windows/install/)

        > `drem` needs `Docker Desktop` to be running.  You should see a small docker whale in your toolbar after installation!

        > If you have trouble installing `docker` please see [FAQ](#FAQ)

        ![Install `docker`](images/docker-whale.png)

    - Install [Microsoft Visual Studio Code (`VSCode`)](https://code.visualstudio.com/)

    - Download `drem` as a ZIP (by selecting the green `Code` button at the top of the `drem` github page) and unzip it

        ![Download `drem`](images/download-drem.PNG)

        > If you are familiar with `git` see [Setup development environment](#setup-development-environment)

    - Open the `drem` folder in `VSCode` by selecting 'File > Open Folder'

    - Select 'Extensions' on the side-bar and install the “Remote - Containers” extension (or install directly from [here](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers))

        ![Containers Extension](images/containers-extension.PNG)

    - Reopen the `drem` folder in a container by selecting 'Reopen in Container' in the popup menu or by opening the Command Palette (via View > Command Palette or by Ctrl + Shft + P) and searching 'Remote-Containers: Reopen in Container'

        > If you want to view the installation progress click on 'Starting Dev Container (show log)' and once the installation is complete (files will appear in the left sidebar) click the + symbol in your Terminal window.

        ![Show log](images/show-log.PNG)


- To run `drem` (and consequently download & transform all `drem` related data):

    - Launch `Jupyter Notebook`:

        - Enter `jnbook` in the `VSCode` Terminal

            ![Launch Jupyter Notebook](images/launch-notebook.PNG)

        - Copy and paste the resulting URL to your browser (or Ctrl + Click)
            > It should look like http://127.0.0.1:8888/?token=aa69433d1370ab87a15436c27cd3f6948f77539a6bbeb6ee

    - Open `run-drem.ipynb` and ...

        - Register your email address with SEAI at  https://ndber.seai.ie/BERResearchTool/Register/Register.aspx
        - Fill in your email address in the notebook
        - Run all cells by selecting Cell > Run (or by manually running each cell via the Run button or by clicking each cell followed by Shft + Enter)

            ![Run `drem`](images/run-drem.PNG)

- To update your local `drem` code:

    - Re-download `drem` as a ZIP
    - (Optional) Copy & paste the files in drem/data/external across from the old version to the new version to skip redownloading of files

---

## `drem` relies on

### Open-source software

- `prefect` to orchestrate all `drem` tasks via a data pipeline
- `pandas` to transform columnar data
- `geopandas` to transform columnar geospatial data
- `requests` to download data
- `pypostal` to standardise and parse address string columns
- `docker` to create a reproducible build environment that runs on Windows, OSX and Linux
- `git` to track code changes


### Open-access data (unless specified!)

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


---


## Setup development environment

> If you've never contributed to an open-source project before checkout (or are new to `git`) [first-contributions](https://github.com/firstcontributions/first-contributions).  They even have a [`VSCode`-specific section](https://github.com/firstcontributions/first-contributions/blob/master/gui-tool-tutorials/github-windows-vs-code-tutorial.md)

- Clone `drem` instead of downloading a zip file as `git` lets you update `drem` without having to manually redownload it:
    - via `git clone https://github.com/codema-dev/drem`

    - or via `VSCode`:
        - Select 'Source Control' on the side-bar
        - Select 'Clone Repository' and search `codema-dev/drem`

        ![Clone `drem`](images/clone-drem.PNG)

- Set your local `VSCode` Python Interpretor to your local `poetry` `virtualenv` Python
    - Run `poetry shell` (to enter`virtualenv`) followed by `which python` (to get the filepath to `virtualenv` Python)
    - Set your Python Interpretor by opening the Command Palette (via View > Command Palette or by Ctrl + Shft + P), searching “Python: Select Interpreter” and copying & pasting the resulting filepath

- Change your local `VSCode` settings to `codema-dev` by creating a local `.vscode` folder and copying & pasting [this settings.json](https://github.com/codema-dev/codema-dev-dotfiles/tree/master/.vscode)

- Install local dev dependencies via `poetry install`

- To update your local `drem` with the latest code:

    > If you change any file `git pull` may result in merge conflicts, see[first-contributions](https://github.com/firstcontributions/first-contributions/blob/master/additional-material/git_workflow_scenarios/resolving-merge-conflicts.md) or [the `VSCode` docs](https://code.visualstudio.com/Docs/editor/versioncontrol)

    - via `git pull https://github.com/codema-dev/drem`

    - or via `VSCode`
        - Select 'Source Control' in the side-bar
        - Select the 3 dots in the top right hand corner of the popup
        - Select 'Pull' to merge in the latest changes

        ![Merge in latest changes](images/git-pull.png)

> For more information see [Developing inside a Container guide](https://code.visualstudio.com/docs/remote/containers)


---


## FAQ

- Install WSL2 for `Docker Desktop` on Windows:

    As of 10/11/2020 `Docker Desktop` raises the following pop-up on Windows:

    ![WSL2 not installed](images/wsl2-not-installed.PNG)

    - Following the pop-up instructions:

        - Launch Powershell as an Administrator search your files for 'Powershell' and either select or right-click 'Run as Administrator':

            ![Launch Powershell as Administrator](images/launch-powershell-as-admin.png)

        - Copy & paste the suggested command `Enable-WindowsOptionalFeature -Online -FeatureName $("VirtualMachinePlatform", "Microsoft-Windows-Subsystem-Linux")` and ENTER

            ![Run Powershell command](images/install-wsl2.PNG)

    - Restart `docker`

        ![Restart `docker`](images/restart-docker.png)

    - 'Docker Desktop' should raise the following pop-up, follow link to complete installation

        ![WSL2 install incomplete](images/wsl2-install-incomplete.PNG)

    - The 'Docker Desktop' whale should indicate that 'Docker Desktop' is running next time you launch it :smiley:
