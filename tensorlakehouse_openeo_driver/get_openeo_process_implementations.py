# dir() returns all the imported methods and classes, so by doing this we can list
# all methods imported from openeo_process_dask.process_implementation
# before = dir()
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

# after = dir()
# impls = [x for x in after if x not in before]
# impls.remove("before")


def get_openeo_impls() -> Dict[str, str]:
    # this function returns all the names of the processes implemented by openeo
    # TODO: apply function is hardcoded because it is a reserved python word
    processes = {"apply": "openeo_processes_dask.process_implementations.cubes.apply"}
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
        cubes_utils,
    ]:
        # module_dir = "openeo_processes_dask.process_implementations.cubes"
        for proc_name in list_defined_functions(m):
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


def list_defined_functions(module):
    processes = [
        x
        for x in dir(module)
        if x in module.__dict__.keys()
        and getattr(module.__dict__[x], "__module__", "") == module.__name__
    ]
    return set(processes)
