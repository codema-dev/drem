from pathlib import Path

from drem.utilities.filepath_tasks import task_with_name_equal_to_path

@task_with_name_equal_to_path
def _name_me_after_my_args(dirpath: Path, filename: str, file_extension: str) -> Path:
    return dirpath / filename / file_extension

def test_task_with_args_as_name():

    task_name = _name_me_after_my_args.run(dirpath=Path.cwd(), filename="data", file_extension=".csv")
    
    assert task_name is None


