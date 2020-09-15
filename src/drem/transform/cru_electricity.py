from pathlib import Path
from typing import Any
from typing import Iterable
from typing import Tuple

import dask.dataframe as dpd

from prefect import Flow
from prefect import Task
from prefect import task
from prefect.engine.state import State

import drem


@task
def _read_txt_files(dirpath: Path) -> Iterable[dpd.DataFrame]:

    return [
        dpd.read_csv(
            filepath,
            sep=" ",
            header=None,
            names=["id", "timeid", "demand"],
            dtype={"id": "int16", "timeid": "string", "demand": "float32"},
        )
        for filepath in dirpath.glob("*.txt")
    ]


@task
def _concat_ddfs(ddfs: Iterable[dpd.DataFrame]) -> dpd.DataFrame:

    return dpd.concat(ddfs)


@task
def _slice_timeid_column(ddf: dpd.DataFrame) -> dpd.DataFrame:

    ddf["day"] = ddf["timeid"].str.slice(0, 3).astype("int16")
    ddf["halfhourly_id"] = ddf["timeid"].str.slice(3, 5).astype("int8")

    return ddf.drop(columns=["timeid"])


@task
def _convert_dayid_to_datetime(ddf: dpd.DataFrame) -> dpd.DataFrame:

    ddf["datetime"] = dpd.to_datetime(
        ddf["day"], origin="01/01/2009", unit="D",
    ) + dpd.to_timedelta(ddf["halfhourly_id"] / 2, unit="h")

    return ddf.drop(columns=["day", "halfhourly_id"])


@task
def _to_parquet(ddf: dpd.DataFrame, savepath: Path):

    return ddf.to_parquet(savepath)


class CleanCRUElecDemand(Task):
    """Clean CRU Electricity Demands.

    From:

    1392  19503   0.14

    To:

    id      datetime                demand
    1392    2009-07-15 01:30:00     0.14

    Args:
        Task (prefect.Task): see https://docs.prefect.io/core/concepts/tasks.html
    """

    def __init__(
        self, dirpath: Path, savepath: Path, **kwargs: Any,
    ):
        """Initialise class with paths and flow executor.

        Args:
            dirpath (Path): Path to directory containing raw data files
            savepath (Path): Path where clean data will be saved
            **kwargs (Any): Keyword arguments that will be passed to the Task
                constructor Task, see https://docs.prefect.io/api/latest/core/task.html
        """
        self.dirpath = dirpath
        self.savepath = savepath

        super().__init__(name="Clean CRU Electricity Demands", **kwargs)

    def flow(self) -> Tuple[Flow, Task]:
        """Instantiate prefect task flow.

        Returns:
            Tuple[Flow, Task]: see https://docs.prefect.io/core/concepts/flows.html
        """
        with Flow("Transform CRU Smart Meter Data") as fw:

            ddfs = _read_txt_files(self.dirpath)
            demand_raw = _concat_ddfs(ddfs)
            demand_with_times = _slice_timeid_column(demand_raw)
            demand_with_datetimes = _convert_dayid_to_datetime(demand_with_times)
            _to_parquet(demand_with_datetimes, self.savepath)

        return fw, demand_with_datetimes

    def run(self) -> State:
        """Run prefect flow.

        Returns:
            State: See https://docs.prefect.io/core/concepts/flows.html#running-a-flow
        """
        flow, result_task = self.flow()
        state = flow.run()
        return state.result[result_task].result


if __name__ == "__main__":

    clean_cru_elec_demand = CleanCRUElecDemand(
        dirpath=drem.filepaths.RAW_DIR / "SM_electricity",
        savepath=drem.filepaths.PROCESSED_DIR / "SM_electricity",
    )
    clean_cru_elec_demand.run()
