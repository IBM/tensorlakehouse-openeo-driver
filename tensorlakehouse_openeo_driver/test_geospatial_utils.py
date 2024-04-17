from openeo_geodn_driver.constants import DEFAULT_TIME_DIMENSION
from openeo_geodn_driver.geospatial_utils import remove_repeated_time_coords
import numpy as np
import pandas as pd
import xarray as xr


def test_squeeze():
    data = np.random.rand(4, 3)
    locs = ["IA", "IL", "IN"]
    times = [
        pd.Timestamp(2000, 1, 1),
        pd.Timestamp(2000, 1, 1),
        pd.Timestamp(2000, 1, 2),
        pd.Timestamp(2000, 1, 2),
    ]
    foo = xr.DataArray(
        data, coords=[times, locs], dims=[DEFAULT_TIME_DIMENSION, "space"]
    )
    da = remove_repeated_time_coords(foo)
    assert len(da[DEFAULT_TIME_DIMENSION].values) == 2
    assert len(da["space"].values) == 3
