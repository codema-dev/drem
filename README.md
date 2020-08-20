# Dublin Region Energy Masterplan (drem)

[![Codecov](https://codecov.io/gh/codema-dev/drem/branch/master/graph/badge.svg)](https://codecov.io/gh/codema-dev/drem)
[![PyPI](https://img.shields.io/pypi/v/drem.svg)](https://pypi.org/project/drem/)
![build](https://github.com/codema-dev/drem/workflows/build/badge.svg)
[![wemake-python-styleguide](https://img.shields.io/badge/style-wemake-000000.svg)](https://github.com/wemake-services/wemake-python-styleguide)

The `drem` library currently automates a number of [__Extract, Transform, Load__](https://en.wikipedia.org/wiki/Extract,_transform,_load) (etl) tasks to generate data for Dublin-specific bottom-up energy modelling.

`drem` enables etl for the following data sets:

- Residential:

    - SEAI's 2016 Census Small Area [Statistics](https://www.cso.ie/en/media/csoie/census/census2016/census2016boundaryfiles/SAPS2016_SA2017.csv), [Geometries](https://data.gov.ie/dataset/small-areas-ungeneralised-osi-national-statistical-boundaries-2015) & [Glossary](https://www.cso.ie/en/media/csoie/census/census2016/census2016boundaryfiles/SAPS_2016_Glossary.xlsx).

    - SEAI's [BER Public Search](https://ndber.seai.ie/BERResearchTool/Register/Register.aspx).
    
    - [Dublin Postcodes Geometries](https://github.com/rdmolony/dublin-postcode-shapefiles) created by Shane McGuinness of Trinity College Dublin.

`drem` provides a simple API to call __extract__, __transform__ and __load__ functions from the Command Line or alternatively to [chain functions in a `prefect` flow into a data pipeline](https://docs.prefect.io/core/tutorial/02-etl-flow.html).
    
`drem` uses:
- `prefect` to orchestrate the data pipeline using Python functions.
- `pandas` to clean columnar data and `geopandas` to clean columnar geospatial data.
- `requests` to extract and download data via HTTP requests



## Installation

```bash
pip install drem
```

> Warning! Installing via `pip` enables only basic usage of the `drem` library. It currently does not enable usage of externals such as the C-library `libpostal`

## Basic usage

### Running individual functions

To view all currently implemented functions run the following in iPython:

```python
[1]: import drem
[2]: drem.<TAB>
```

To get help with usage of individual functions run:

```python
[1]: import drem
[2]: drem.<name-of-method>?
```

For example; to download individual raw data files and tidy them for Dublin run:

```python
[1]: import drem
[2]: sa_statistics_raw = drem.extract_sa_statistics.run()
[3]: sa_statistics_dublin = drem.transform_sa_statistics.run(sa_statistics_raw)
```

Currently `drem` calls the above functions in a `prefect` data pipeline (or flow) Python script that can be found at `src/drem/extract_transform_load.py`, for more information on creating and running `prefect` flows see the [`prefect` documentation](https://docs.prefect.io/core/concepts/flows.html#functional-api)

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

## Contributing to `drem`

### Glossary of terms

- __script__ = a text file containing functions and classes - Python scripts have .py file endings.  These files must be run via the command line (See section below for more details).

- __module__ = an individual python file containing a bunch of functions or classes.

- __library__ = a collection of modules with helpful functions and classes.

---

### Setup a Local Development environment using Visual Studio Code

1. Download the `drem` repository by clicking 'Clone or download'

2. Download [Microsoft Visual Studio Code](https://code.visualstudio.com/)

3. Install the [Python extension](https://marketplace.visualstudio.com/items?itemName=ms-python.python)

4. Launch Visual Studio Code in your local `drem` folder

5. [Install Poetry](https://python-poetry.org/docs/) and run `poetry install` on the Command Line to setup your development environment

---

### [Optional] Setup Windows Subsystem for Linux 2 (WSL2)

A lot of Python modules require tedious workarounds to run properly on Windows.  One work-around is to use WSL2.  If you're having install issues with the previous setup it may be worthwhile switching to WSL2.

1. [Install Windows Subsystem for Linux (WSL) in VSCode](https://code.visualstudio.com/docs/remote/wsl)

2. Set your VSCode default terminal to WSL2:
    - Press `CONTROL-SHIFT-P`
    - Type `Terminal: Select Default Shell`
    - Select WSL Bash.

3. Launch your folder in WSL2

---

### Using existing libraries

```Python
import pandas
```

Now all of the pandas functions and classes can be accessed with a `.` operator:

```Python
# Create a DataFrame containing the list [1 2 3]
data = pandas.DataFrame([1 2 3])
```

Typically use `import pandas as pd` instead as short-hand so:

```Python
data = pd.DataFrame([1 2 3])
```


---

### Why not use Jupyter Notebooks?

Jupyter Notebooks are great for prototyping ideas with single-use code but not so good for writing reusable code.  Scripts are preferable for this purpose as they make the following possible:

- __Refactoring__ = re-implementing code to speed it up or clean it up.
- __Testing__ = write test scripts to make sure the code does what it says on the tin (trademark: Ronsill??)
- __Logging__ = store outputs of code as it runs in external files so can see what's going on inside the file
- __data pipeline__ = specifies the process of data transformation (where it comes from, where it goes, what's done to it) - typically a helper module such as [Luigi](https://luigi.readthedocs.io/en/stable/) is used for this.

---

### How to run scripts?

As scripts are run using the command line it is necessary to be familiar with a few commands (surprisingly few are needed)

- `cd` = change directory
- `cd ..` = go back a directory
- `ls` = list names of files and folders in directory
- `pwd` = print current directory
- `cd <name-of-file-or-folder>` = go to file/folder

See [Command Line Crash Course](https://learnpythonthehardway.org/book/appendixa.html) from Learn Python The Hard Way (the entire pdf is on the Share Drive) for intros/examples of maybe 10 commands that are used all the time.

---

### [iPython](https://ipython.readthedocs.io/en/stable/)

The command line tool iPython can be used to run scripts and try out code interactively.

For more information:__ See jakevdp's chapter on iPython at [Python Data Science Handbook](https://jakevdp.github.io/PythonDataScienceHandbook/).
