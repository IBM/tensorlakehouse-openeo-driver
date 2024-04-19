before = dir()
from tensorlakehouse_openeo_driver.processes import *  # noqa: ignore F403, F403, F401, E402

after = dir()
impls = [x for x in after if x not in before]
impls.remove("before")


def get_impls():
    # this function returns all the names of the processes implemented by geodn processing
    return impls
