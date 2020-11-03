# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## [Unreleased]


---


## [0.4.0] - 2020-11-03

### Added

- Roughly estimate (not in a prefect flow yet...) & map Small Area energy demand for
    - Residential using SEAI BER archetypes, CSO 2016 Census HH statistics
    - Commercial using VO Floor areas and CIBSE 2009 / Dublin LA derived benchmarks
    - Map using CSO 2016 Census Small Areas and Shane McGuinness Postcodes

- Clean cso 2019 postcode network gas consumption (residential & non-residential) and link these demands to postcode households statistics (Census 2016 CSO) and to postcode geometries.  These demands can be used as a 'ground-truth' for district level heating demand.

- Roughly adapt BER hh fabric info, CSO Small Area stats & geodirectory hh stats via scripting for input to CityEnergyAnalyst

- Download VO data via their API LA by LA

- Add generic dask_dataframe_tasks to wrap generic dask dataframe methods for prefect

- Add filepath_tasks so can use functions to find ROOT_DIR which can be easily mocked out for the prefect pipeline dummy flows...

- Add immutable dicts to utilities.filepaths to store filepaths accross files - WIP

- Add flow visualization mixin that can be inherited by transform tasks to easily visualize each task in the form of a flow chart so that non-programmers can follow the transform pipeline at a glance...

### Changed

- Rename commercial benchmark filenames to remove irrelevant strings (s.a. Table X.X) and move unmatched benchmarks (to VO uses) to the appropriate corresponding benchmark

- Skip download ber if file exists

- Refactor all residential & commercial etl flow tasks to read/write to files instead of passing DataFrames & GeoDataFrames.  Consequently once a flow each step of the pipeline is now checkpointed.  This enables the running of each transform file independent of flow context.  It also enables Excel, Tableau or QGIS users to visualize these intermediate steps provided that the data is checkpointed in a compatible file format.

- Refactor testing of residential etl to mock out DATA_DIR rather than replacing it via prefect flow parameter.  Consequently the prefect flow visualisation for this etl is much cleaner as DATA_DIR is no longer split into 10s of div bubbles.  Also, it is now possible to use prefect's built-in file checkpointing in place of read/write file explicitely in each etl task as this DATA_DIR variable can be passed outside of flow context during the Task instantiation step at the top of the etl file.  It's still possible to run transform tasks independently of flow provided that a default read-from-filepath argument is set for each data file.

- Refactor `drem.transform.dublin_postcodes` into a `prefect` flow & dissolve all Co Dublin postcodes into one multipolygon geometry using `geopandas` dissolve.

- Pull generalisable `pandas` and `geopandas` prefect tasks into `drem.utilities.pandas_tasks` and `drem.utilities.geopandas_tasks` respectively.  Only unit tested if functionality differs from `pandas` or `geopandas` implementation...

### Removed

---


## [0.3.1] - 2020-10-07

### Added

- Create generic `Download` Task class that can be called to create download tasks to get data direct from any url.  Unit test this generic module using `responses` to mock http requests.

- Clean electricity demands in closed-access [CRU Smart Meter Trials Data](https://www.ucd.ie/issda/data/commissionforenergyregulationcer/) in `drem.transform.cru_electricity` into a usable tabular format via `dask.dataframe`

- Create an electricity diversity curve using closed-access [CRU Smart Meter Trials Data](https://www.ucd.ie/issda/data/commissionforenergyregulationcer/) electricity data

### Changed

- Replace `drem.extract` with separate `download`, `unzip` and `convert` tasks so that each task is doing one thing and one thing only (more modular code) & read-in files of raw data in `transform`:

    - use of `great_expectations` at each stage of the process to ensure expectations are met s.a. downloaded data is as expected (column names, missing values etc.) and each `transform` task is cleaning in the specified manner.  This replaces the previous implementation of fragile functional tests on each `transform` task which would break upon a simple column name change.  `great_expectations` are quicker to whip up as they are automated...

    - Rewrite `drem.utilities.ftest_data` so that instead of creating sample parquet files of raw data to be run through the functional flow test (which skips download, unzip & convert) it creates data in the same file format as the actual raw data used in non-test flows (so now only download tasks are skipped in the functional/end-to-end tests)

    - Create a module `drem.utilities.convert` to transform any file format (xlsx, csv, shp) into parquet in the etl flow (previously this was done during catch-all extract tasks)

    - Create a module `drem.utilities.zip` to unzip zipped folders prior to conversion to parquet.

    - Refactor `drem.extract.ber` into `drem.download.ber` which merely logs into BER Public search and downloads the data (so leaves unzipping & converting to parquet to other tasks...)

    - Rewrite entire `drem.etl.residential` using generic `Download` tasks (defining url for each in task initialisation @ top of module), unzip zipped folders, convert to parquet & transform task using a filepath input (rather than passing a DataFrame).

- Pull generic pandas tasks into `drem.utilities.pandas_tasks` and run `import drem.utilities.pandas_tasks as pdt` to call em within any flow

- Refactor `drem.transform.ber` into `drem.estimate.ber_archetypes` and `drem.transform.ber` with transform only performing cleaning/filtering operations.  This enables generic command line operations on clean ber Dublin data with all 204 columns as archetype generation is no longer a barrier...

- Refactor all transform tasks into `prefect` sub-flows so:

    - transform tasks log their progress step-by-step so that it is obvious at which step a transform task fails and why

    - enables use of prefect [flow visualization](https://docs.prefect.io/core/advanced_tutorials/visualization.html#flow-visualization) for transform tasks as well as etl flows so that non-programmers can easily see what steps are being performed on the data.

- Refactor Small Area Statitistics so can query any table in glossary excel file by copying and pasting the table name in the `Table Within Themes` column to the `drem.transform.sa_statistics._extract_rows_from_glossary` function's `target` argument.  Previously, this function was hard-coded to extract only the period built table from glossary.

### Removed

- Remove all `drem.*` namespace tasks and import them directly via `from drem.transform.ber import transform_ber`

- Delete all non-etl functional tests (previously had a functional test for each transform task) as am replacing em all with `great_expectations` tasks.  Previous implementation was too fragile as it broke every time a single column was changed.  Expecations on the other hand are designed to be dynamic and easily updatable...

- Remove all `tdda` related code as has been replaced by `great_expectations` for functional tests and `pytest` for individual task tests...  This includes removing all unit test data from source control...


---


## [0.3.0] - 2020-10-07

- Same as 0.3.1 except CHANGELOG was incomplete


---


## [0.2.0] - 2020-09-17

### Added

- Add Plot modules to `drem.plot.*` for:

    - Dublin Small Area Residential heat demand via `drem.plot.sa_statistics`

    - Dublin Small Area Commercial heat demand via `drem.plot.sa_commercial`

    - Create a  Diversity curve of electricity demands (Peak Demand per sample size vs sample size) to check whether or not an aggregated demand profile can be used for Small Area electricity demand (i.e. 80-120 homes) via `drem.plot.elec_diversity_curve`

- Add tasks to `drem.extract_*`

    - Download BER data from SEAI via `drem.extract_ber("<seai-registered-email-address>)`

- Add module to `drem.etl.*`

    - ETL Dublin Residential data and estimate energy usage using archetypes based on BER-estimated heat demands; call via `drem.etl.residential`

    - ETL Dublin Commercial data and estimate energy usage based on Benchmarks; call via `drem.etl.commercial`

- Add modules to `drem.transform.*`

    - Transform closed-access Measurement & Verification (M&R) data:

        - Clean MPRN and GPRN via `pandas`

        - Merge MPRN and GPRN data using [`pypostal`](https://github.com/openvenues/pypostal) to standardise addresses prior to merging

        - Parse M&R addresses using `pypostal` into categories (such as House Number, Road ...)

        - Standardise Valuation Office (VO) addresses via [`pypostal`]

        - Parse (VO) addresses via [`pypostal`] into categories

    - Transform closed-access CRU Smart Meter electricity data:

        - Clean via `dask` (as too big to load into memory for `pandas`)
            ```
            From:   1392  19503   0.14

            To:     id      datetime                demand
                    1392    2009-07-15 01:30:00     0.14
            ```

    - Transform Valuation Office (VO) data:
        ```
        From:   Property Number  ...   Area
                5014911           ...  30.8

        To:  Property Number   ...     Area     typical_fossil_fuel_demand  geometry
                315108            ...     30.8     12320.0                     POINT (-6.26747 53.28973)
        ```
        - Extract Use from Uses using regex via `pandas`
        - Convert Latitiude and Longitude to Points via `geopandas`
        - Link VO to benchmarks (demand per unit floor area) according to 'Usage' specified in VO

    - Setup pipeline end-to-end functional tests with dummy data for ETLs

    - Setup a `docker` dev-environment `Dockerfile` to automate the `drem` development environment setup using [`vscode-docker-dev`](https://code.visualstudio.com/docs/remote/containers)

### Changed

- Split etl into Residential and Commercial

- Adapt existing `drem.transform_*` tasks

    - `drem.transform_ber`

        - Get mean total heat demand __per household__ archetype instead of total heat demand for all BER buildings in archetype SO can apply this per household average directly to Census data.

    - `drem.transform_sa_statistics`

        - Link Small Areas to Postcodes

        - Link Small Area Statistics to BER archetypes via Postcodes

        - Estimate Small Area heat demand using BER archetype heat estimates



### Removed

- Explicit load Prefect tasks such as `drem.load_sa_geometries` have been removed; generic load tasks can be created via [instantiation of custom load Prefect tasks](https://docs.prefect.io/core/concepts/tasks.html#tasks) such as `drem.LoadToParquet`


---


## [0.1.2] - 2020-08-20

### Added

- Add [Github Actions](https://docs.github.com/en/actions/guides/building-and-testing-python) as a Continuous Integration platform to run on each pull request:

    - Style check with `black`

    - Functional & Unit Tests with `Pytest`

- Add [Deepsource](https://deepsource.io/) to "discover and fix bug risks, anti-patterns, performance issues, and security flaws — before they land in production."

- Add [Codecov](https://codecov.io/) to measure testing coverage on each Pull Request and display as a tag in README

- Add README intro with code examples

- Add tags for [`black`](https://black.readthedocs.io/en/stable/?badge=stable), [`Wemake-python-styleguide`](https://wemake-python-stylegui.de/en/latest/pages/usage/violations/index.html), `PyPi`, `deepsource`, `MIT License`, `Github Actions`


---


## [0.1.1] - 2020-08-20

### Added
- Add functions `drem.extract_*` to download data from URLs:

    - Dublin Postcodes geometries

    - Ireland Small Area Statistics from 2016 Census

    - Ireland Small Area Statitics geometries

- Add functions `drem.transform_*`

    - Building Energy Rating (BER)

        - Aggregate BER data into archetype subgroups for Postcode, Period Built with corresponding BER-estimated Heat Demands via `pandas`

    - Ireland Small Area (SA) geometries

    - Ireland SA glossary

        - Extract keys for Period Built information

    - Ireland SA Statistics

        - Extract Dublin Period Built information via the Small Area Glossary file

        - Link Small Area

        - Extract Dublin Small areas

- Add module `drem.etl` to orchestrate an Extract, Transform, Load pipeline of above tasks via `prefect`

- Replace `setup.py`, `requirements.txt` etc with `poetry`

- Copy `drem` code from `rdmolony` to `codema-dev` and restart Git History from there...

[Unreleased]: https://github.com/codema-dev/drem/compare/v0.4.0...HEAD
[0.4.0]: https://github.com/codema-dev/drem/compare/v0.3.1...v0.4.0
[0.3.1]: https://github.com/codema-dev/drem/compare/v0.3.1...v0.3.0
[0.3.0]: https://github.com/codema-dev/drem/compare/v0.3.0...v0.2.0
[0.2.0]: https://github.com/codema-dev/drem/compare/v0.2.0...v0.1.2
[0.1.2]: https://github.com/codema-dev/drem/compare/v0.1.2...v0.1.1
[0.1.1]: https://github.com/codema-dev/drem/releases/tag/v0.1.1
