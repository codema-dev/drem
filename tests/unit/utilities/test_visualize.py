from os import listdir
from pathlib import Path
from typing import Any

from prefect import Flow
from prefect import Task
from prefect import task
from prefect.engine.state import State

from drem.utilities.visualize import VisualizeMixin


@task
def do_complicated_mathematics() -> int:
    """Do complicated mathematics.

    Returns:
        int: The result
    """
    return 1 + 1


with Flow("Generic flow to test flow visualization") as flow:

    do_complicated_mathematics()


class GenericFlowRunTask(Task, VisualizeMixin):
    """Create generic prefect Task to test flow visualization.

    Args:
        Task (prefect.Task): see https://docs.prefect.io/core/concepts/tasks.html
        VisualizeMixin (object): Mixin to add flow visualization method
    """

    def __init__(self, **kwargs: Any):
        """Initialise Task.

        Args:
            **kwargs (Any): see https://docs.prefect.io/core/concepts/tasks.html
        """
        self.flow = flow
        super().__init__(**kwargs)

    def run(self) -> State:
        """Run module flow.

        Returns:
            State: see
        """
        return self.flow.run()


generic_task = GenericFlowRunTask()


def test_visualize_mixin_creates_pdf_flow_visualization(tmp_path: Path) -> None:
    """Test visualize mixin creates a pdf flow visualization.

    Args:
        tmp_path (Path): see https://docs.pytest.org/en/stable/tmpdir.html
    """
    filename = "generic_task"
    generic_task.save_flow_visualization_to_file(
        savepath=tmp_path / filename, flow=flow,
    )

    assert f"{filename}.pdf" in listdir(tmp_path)


def test_visualize_mixin_creates_pdf_flow_visualization_with_state(
    tmp_path: Path,
) -> None:
    """Test visualize mixin creates a pdf flow visualization.

    Args:
        tmp_path (Path): see https://docs.pytest.org/en/stable/tmpdir.html
    """
    filename = "generic_task"
    state = generic_task.run()
    generic_task.save_flow_visualization_to_file(
        savepath=tmp_path / filename, flow=flow, flow_state=state,
    )

    assert f"{filename}.pdf" in listdir(tmp_path)
