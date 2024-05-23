import pytest
from typing import Dict, Tuple
from tensorlakehouse_openeo_driver.constants import (
    DEFAULT_TIME_DIMENSION,
    DEFAULT_X_DIMENSION,
    DEFAULT_Y_DIMENSION,
)
from tensorlakehouse_openeo_driver.geospatial_utils import (
    remove_repeated_time_coords,
    clip_box,
)
import numpy as np
import pandas as pd
import xarray as xr
from datetime import datetime
from tensorlakehouse_openeo_driver.tests.unit.unit_test_util import generate_xarray


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


@pytest.mark.parametrize(
    "bbox, filter_bbox, expected_dim_size",
    [
        ((0.0, 40.0, 1.0, 41.0), (0.25, 40.25, 0.75, 40.75), {}),
        (
            (0.0, 40.0, 1.0, 41.0),
            (0.00000001, 40.25, 0.00000002, 40.75),
            {DEFAULT_X_DIMENSION: 1},
        ),
    ],
)
def test_clip_box(
    bbox: Tuple[float, float, float, float],
    filter_bbox: Tuple[float, float, float, float],
    expected_dim_size: Dict[str, int],
):
    crs = 4326
    array = generate_xarray(
        lonmin=bbox[0],
        latmin=bbox[1],
        lonmax=bbox[2],
        latmax=bbox[3],
        temporal_extent=(datetime(2000, 1, 1), datetime(2001, 1, 1)),
        num_periods=365,
        freq=None,
        bands=["Band1"],
    )

    array_clipped = clip_box(
        data=array,
        bbox=filter_bbox,
        crs=crs,
        y_dim=DEFAULT_Y_DIMENSION,
        x_dim=DEFAULT_X_DIMENSION,
    )
    assert isinstance(array_clipped, xr.DataArray)
    # if dimension is greater than or equal to 3, it means 
    if array_clipped[DEFAULT_X_DIMENSION].size >= 3:
        minx = min(array_clipped[DEFAULT_X_DIMENSION].values)
        maxx = max(array_clipped[DEFAULT_X_DIMENSION].values)
        assert (
            filter_bbox[0] <= minx <= maxx <= filter_bbox[2]
        ), f"Error! {filter_bbox[0]=} {minx=} {maxx=} {filter_bbox[2]=}"
    if array_clipped[DEFAULT_Y_DIMENSION].size >= 3:
        miny = min(array_clipped[DEFAULT_Y_DIMENSION].values)
        maxy = max(array_clipped[DEFAULT_Y_DIMENSION].values)
        assert (
            filter_bbox[1] <= miny <= maxy <= filter_bbox[3]
        ), f"Error! {filter_bbox[1]=} {miny=} {maxy=} {filter_bbox[3]=}"
    for dim_name, dim_size in expected_dim_size.items():
        assert dim_size == array_clipped[dim_name].size
