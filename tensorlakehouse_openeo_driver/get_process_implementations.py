before = dir()
from tensorlakehouse_openeo_driver.processes import *

after = dir()
impls = [x for x in after if x not in before]
impls.remove("before")


def get_impls():
    # this function returns all the names of the processes implemented by geodn processing
    print(impls)
    return impls
