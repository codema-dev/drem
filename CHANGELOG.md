# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## [Unreleased]
- Remove all `drem.*` namespace tasks and import them directly via `from drem.transform.ber import transform_ber`
- Replace `drem.extract` with separate `download`, `unzip` and `convert` tasks so that each task is doing one thing and one thing only (more modular code) & read-in files of raw data in `transform`:
    - use of `great_expectations` at each stage of the process to ensure expectations are met s.a. downloaded data is as expected (column names, missing values etc.) and each `transform` task is cleaning in the specified manner.  This replaces the previous implementation of fragile functional tests on each `transform` task which would break upon a simple column name change.  `great_expectations` are quicker to whip up as they are automated...
- Create generic `Download` Task class that can be called to create download tasks to get data direct from any url

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


## [0.1.2] - 2020-08-20

### Added

- Add [Github Actions](https://docs.github.com/en/actions/guides/building-and-testing-python) as a Continuous Integration platform to run on each pull request:

    - Style check with `black`

    - Functional & Unit Tests with `Pytest`

- Add [Deepsource](https://deepsource.io/) to "discover and fix bug risks, anti-patterns, performance issues, and security flaws — before they land in production."

- Add [Codecov](https://codecov.io/) to measure testing coverage on each Pull Request and display as a tag in README

- Add README intro with code examples

- Add tags for [`black`](https://black.readthedocs.io/en/stable/?badge=stable), [`Wemake-python-styleguide`](https://wemake-python-stylegui.de/en/latest/pages/usage/violations/index.html), `PyPi`, `deepsource`, `MIT License`, `Github Actions`



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

[Unreleased]: https://github.com/codema-dev/drem/compare/v0.2.0...HEAD
[0.1.2]: https://github.com/codema-dev/drem/compare/v0.1.2...v0.1.1
[0.1.1]: https://github.com/codema-dev/drem/releases/tag/v0.1.1
