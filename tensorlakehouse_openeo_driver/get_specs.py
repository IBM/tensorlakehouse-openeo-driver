before = dir()

from openeo_processes_dask.specs import *

after = dir()
process_names = [x for x in after if x not in before]
process_names.remove("before")


def get_process_names() -> list[str]:
    """get a list of process names by inspecting openeo_processes_dask.specs module

    Returns:
        List[str]: list of process names
    """
    return process_names
