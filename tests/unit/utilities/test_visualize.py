from os import listdir
from pathlib import Path

from prefect import Flow
from prefect import Task
from prefect import task

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
        VisualizeMixin ([type]): Mixin to add flow visualization method
    """

    def run(self) -> None:
        """Run module flow."""
        flow.run()


generic_task = GenericFlowRunTask()


def test_visualize_mixin_creates_pdf_flow_visualization(tmp_path: Path) -> None:
    """Test visualize mixin creates a pdf flow visualization.

    Args:
        tmp_path (Path): see https://docs.pytest.org/en/stable/tmpdir.html
    """
    filename = "generic_task"
    generic_task.save_flow_visualization_to_file(
        dirpath=tmp_path, filename=filename, flow=flow,
    )

    assert f"{filename}.pdf" in listdir(tmp_path)
