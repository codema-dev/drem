from typing import Any

from prefect import task


@task
def flow_breakpoint(**kwargs: Any) -> None:
    """Drop in a prefect.Flow to inspect local tasks.

    Example:
        ```python
        from prefect import Flow
        from prefect import task

        @task
        def create_data():
            return [1,2,3]

        with Flow("Example") as flow:
            data = create_data()
            flow_breakpoint(data=data)
        ```
        Can now access this object within `pdb` with kwargs["data"]   
    """
    breakpoint()  # noqa
