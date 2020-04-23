from pathlib import Path
from typing import Callable, List, Tuple, Union
from prefect import Flow
from prefect.core.task import Task

from prefect.engine.state import State
from prefect.engine.executors import DaskExecutor, LocalExecutor
from prefect.utilities.debug import raise_on_exception


def run_flow(
    flow_function: Callable[[None], Flow], viz_path: Path = None, parallel: bool = True,
) -> Tuple[State, List[Task]]:
    """Runs all preprocessing prefect tasks and returns the flow state &
    a list of all tasks run

    For how to setup an etl flow see:
    https://docs.prefect.io/core/examples/etl.html#etl-flow

    For using a prefect flow state to view task results see:
    https://docs.prefect.io/core/advanced_tutorials/using-results.html#using-results

    Returns
    -------
    Tuple:
        State
            A prefect state object.  A state can be either 'Success' or 
            'Failed' depending on whether or not all of the tasks run in the 
            flow succeed.  This object allows access to: 
            (1) Data before and after each task in the flow
            (2) Task states (success/failure) 
        List[Task]
            A list of tasks containing task references that can be passed to a
            prefect state flow_state to view data before and after a specific task
            a prefect flow state 
    """

    flow = flow_function()
    tasks = flow.get_tasks()

    if parallel:
        executor = DaskExecutor()
    else:
        executor = LocalExecutor()

    with raise_on_exception():
        state = flow.run(executor=executor)

    if viz_path:
        flow.visualize(flow_state=state, filename=viz_path)

    return state, tasks
