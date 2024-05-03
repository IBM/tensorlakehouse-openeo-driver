# listing all modules before import
before = dir()
# import all processes
from tensorlakehouse_openeo_driver.processes import *  # noqa: F403, F401, E402

# list all modules after import
after = dir()

# keep everything that was only listed after import
impls = [x for x in after if x not in before]
impls.remove("before")


def get_impls():
    # this function returns all the names of the processes implemented by geodn processing
    print(impls)
    return impls
