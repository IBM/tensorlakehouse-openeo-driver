# dir() returns all the imported methods and classes, so by doing this we can list
# all methods imported from openeo_process_dask.process_implementation
# before = dir()

import inspect
from typing import Dict
from openeo_processes_dask.process_implementations import (
    arrays,
    comparison,
    core,
    data_model,
    exceptions,
    logic,
    math,
    utils,
)
from openeo_processes_dask.process_implementations.cubes import (
    resample,
    aggregate,
    experimental,
    indices,
    merge,
    general,
    load,
    reduce,
    utils as cubes_utils,
)
from openeo_processes_dask.process_implementations import cubes
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
# after = dir()
# impls = [x for x in after if x not in before]
# impls.remove("before")


def get_openeo_impls() -> Dict[str, str]:
    # this function returns all the names of the processes implemented by openeo
    # create a dict in which keys are the process names and values are the path to the process/function
    # TODO: apply function is hardcoded because it is a reserved python word
    processes = {
        "apply": "openeo_processes_dask.process_implementations.cubes.apply",
    }
    for m in [
        resample,
        aggregate,
        experimental,
        indices,
        merge,
        resample,
        general,
        load,
        reduce,
        cubes,
        cubes_utils,
    ]:
        # module_dir = "openeo_processes_dask.process_implementations.cubes"
        for proc_name in list_defined_functions(m):
            logger.debug(f"proc_name={proc_name=} {m.__name__}")
            processes[proc_name] = m.__name__

    for m in [
        arrays,
        comparison,
        core,
        data_model,
        exceptions,
        logic,
        math,
        utils,
    ]:
        # module_dir = "openeo_processes_dask.process_implementations"
        for proc_name in list_defined_functions(m):
            processes[proc_name] = m.__name__
    return processes


def list_defined_functions(module_or_function) -> set:
    """list attributes of the specified module

    Args:
        module (_type_):

    Returns:
        set: list of all attributes
    """
    if inspect.ismodule(module_or_function):
        module_name: str = module_or_function.__name__

        processes = [
            x
            for x in dir(module_or_function)
            if x in module_or_function.__dict__.keys()
            and getattr(module_or_function.__dict__[x], "__module__", "").startswith(
                module_name
            )
        ]
        logger.debug(f"list_defined_functions:: {module_name=}")
        if "mask" in module_name.lower():
            module_dict_keys = module_or_function.__dict__.keys()
            logger.debug(f"list_defined_functions - {module_dict_keys=}")
            dir_module = dir(module_or_function)
            logger.debug(f"list_defined_functions - {dir_module=}")
            logger.debug(
                f"list_defined_functions - Set of processes: {processes} module name={module_name}"
            )

        return set(processes)
    else:
        return set()
