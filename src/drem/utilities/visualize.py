from pathlib import Path
from typing import Optional

from prefect import Flow
from prefect.engine.state import State


class VisualizeMixin(object):
    """Visualize Prefect Flows."""

    def save_flow_visualization_to_file(
        self, savepath: Path, flow: Flow, flow_state: Optional[State] = None,
    ) -> None:
        """Save Prefect Flow visualization to file.

        Args:
            savepath (Path): Path to save file
            flow (Flow): Prefect flow to visualize
            flow_state (Optional[State], optional): see https://docs.prefect.io/core/concepts/results.html#result-objects
        """
        if flow_state:
            flow.visualize(flow_state=flow_state, filename=savepath)
        else:
            flow.visualize(filename=savepath)
