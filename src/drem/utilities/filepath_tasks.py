from functools import wraps
from pathlib import Path

from prefect import task

@task(name="Get Filepath")
def get_filepath(data_dir: Path, filename: str, file_extension: str) -> str:

    return str(data_dir / f"{filename}{file_extension}")


def task_with_name_equal_to_path(func):
    parameters = []
    @wraps(func)
    def wrapper(*args, **kwargs):
        breakpoint()
        parameters.extend(locals().get("args"))
        parameters.extend(locals().get("kwargs").values())
        parameters = [str(item) for item in parameters]
        return func(*args, **kwargs)
    
    wrapped_func = task(wrapper, name=f"{str(parameters[0])} / {parameters[1]}{parameters[2]}")
    breakpoint()
    return wrapped_func