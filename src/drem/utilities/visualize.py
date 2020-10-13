from pathlib import Path

from prefect import Flow


class VisualizeMixin(object):
    """Visualize Prefect Flows."""

    def save_flow_visualization_to_file(
        self, dirpath: Path, filename: str, flow: Flow,
    ) -> None:
        """Save Prefect Flow visualization to file.

        Args:
            dirpath (Path): Path to save directory
            filename (str): Name of file
            flow (Flow): Prefect flow to visualize
        """
        savepath = dirpath / filename
        flow.visualize(filename=savepath)
