from openeo_pg_parser_networkx.pg_schema import ParameterReference
from functools import partial
from typing import List, Union

from tensorlakehouse_openeo_driver.constants import (
    DEFAULT_BANDS_DIMENSION,
    DEFAULT_TIME_DIMENSION,
    DEFAULT_X_DIMENSION,
    DEFAULT_Y_DIMENSION,
)
from tensorlakehouse_openeo_driver.processes import (
    rename_dimension,
    rename_labels,
    aggregate_temporal,
    resample_spatial,
    merge_cubes,
    aggregate_temporal_period,
)
from tensorlakehouse_openeo_driver.processing import TensorlakehouseProcessing
from tensorlakehouse_openeo_driver.tests.unit.unit_test_util import (
    generate_xarray_datarray,
    validate_raster_datacube,
)
import pandas as pd
import pytest
import numpy as np
from rasterio import crs


@pytest.mark.parametrize("source,target", [(DEFAULT_X_DIMENSION, "x_new")])
def test_rename_dimension(source: str, target: str):
    array = generate_xarray_datarray(
        timestamps=(pd.Timestamp(2020, 1, 1), pd.Timestamp(2021, 1, 1)),
        latmax=50,
        latmin=40,
        lonmax=-80,
        lonmin=-90,
        bands=["B02"],
        freq=None,
    )
    da = rename_dimension(data=array, source=source, target=target)
    assert target in da.dims


@pytest.mark.parametrize(
    "source,target, dimension",
    [
        (["B02"], ["B04"], "bands"),
        (
            None,
            np.random.uniform(low=0, high=1, size=100).tolist(),
            DEFAULT_X_DIMENSION,
        ),
    ],
)
def test_rename_labels(
    source: List[Union[str, int]], target: List[Union[str, int]], dimension: str
):
    size_x = 100
    array = generate_xarray_datarray(
        timestamps=(pd.Timestamp(2020, 1, 1), pd.Timestamp(2021, 1, 1)),
        latmax=50,
        latmin=40,
        lonmax=-80,
        lonmin=-90,
        size_x=size_x,
        bands=["B02"],
        freq=None,
    )
    da = rename_labels(data=array, dimension=dimension, source=source, target=target)
    assert list(da.coords[dimension]) == target


@pytest.mark.skip("Error! Quarantine")
def test_aggregate_temporal():
    size_x = 100
    start = pd.Timestamp(2020, 1, 1)
    end = pd.Timestamp(2020, 2, 1)
    array = generate_xarray_datarray(
        timestamps=(start, end),
        latmax=50,
        latmin=40,
        lonmax=-80,
        lonmin=-90,
        bands=["B02"],
        size_x=size_x,
        num_periods=30,
    )
    time_interval = pd.date_range(start=start, end=end, freq="W")
    datetime_format = "%Y-%m-%dT%H:%M:%SZ"
    intervals = list()
    print(f"start={start} end={end}")
    i = 1
    while i < len(time_interval):
        s = time_interval[i - 1]
        e = time_interval[i]
        intervals.append([s.strftime(datetime_format), e.strftime(datetime_format)])
        i += 1
    da = aggregate_temporal(
        data=array, intervals=intervals, dimension=None, reducer="mean"
    )
    assert len(da.time) == 12


@pytest.mark.parametrize("resolution", [0, 0.01])
def test_resample_spatial(resolution: int):
    data = generate_xarray_datarray(
        bands=["b02", "b03"],
        latmax=41,
        latmin=40,
        lonmax=-90,
        lonmin=-91,
        timestamps=(pd.Timestamp(2020, 1, 1), pd.Timestamp(2021, 1, 1)),
        freq=None,
    )
    target_projection = 4236
    target_crs = crs.CRS.from_epsg(target_projection)
    assert data.rio.crs != target_crs
    resampled_data = resample_spatial(
        data=data, projection=target_projection, resolution=resolution
    )
    assert resampled_data.rio.crs == target_crs
    if resolution > 0:
        for i in range(1, resampled_data[DEFAULT_X_DIMENSION].size):
            dif = np.abs(
                resampled_data[DEFAULT_X_DIMENSION].values[i]
                - resampled_data[DEFAULT_X_DIMENSION].values[i - 1]
            )
            assert np.isclose(dif, resolution, rtol=0.10)
        for i in range(1, resampled_data[DEFAULT_Y_DIMENSION].size):
            dif = np.abs(
                resampled_data[DEFAULT_Y_DIMENSION].values[i]
                - resampled_data[DEFAULT_Y_DIMENSION].values[i - 1]
            )
            assert np.isclose(dif, resolution, rtol=0.10)


@pytest.mark.parametrize(
    "bands_1, bands_2, bbox_1, bbox_2, spatial_ext_1, spatial_ext_2, expected_dim_size",
    [
        (
            ["B02", "B03"],
            ["Fmask"],
            [-91, 40, -90, 41],
            [-91, 40, -90, 41],
            [pd.Timestamp(2020, 1, 1), pd.Timestamp(2021, 1, 1)],
            [pd.Timestamp(2020, 1, 1), pd.Timestamp(2021, 1, 1)],
            {DEFAULT_BANDS_DIMENSION: 3},
        ),
        (
            ["B02", "B03"],
            ["B02", "B03"],
            [-91, 40, -90, 41],
            [-91, 40, -90, 41],
            [pd.Timestamp(2020, 1, 1), pd.Timestamp(2021, 1, 1)],
            [pd.Timestamp(2021, 1, 2), pd.Timestamp(2022, 1, 1)],
            {DEFAULT_BANDS_DIMENSION: 2, DEFAULT_TIME_DIMENSION: 20},
        ),
    ],
)
def test_merge_cubes(
    bands_1, bands_2, bbox_1, bbox_2, spatial_ext_1, spatial_ext_2, expected_dim_size
):
    cube1 = generate_xarray_datarray(
        bands=bands_1,
        lonmin=bbox_1[0],
        latmin=bbox_1[1],
        lonmax=bbox_1[2],
        latmax=bbox_1[3],
        timestamps=spatial_ext_1,
        freq=None,
    )
    cube2 = generate_xarray_datarray(
        bands=bands_2,
        lonmin=bbox_2[0],
        latmin=bbox_2[1],
        lonmax=bbox_2[2],
        latmax=bbox_2[3],
        timestamps=spatial_ext_2,
        freq=None,
    )
    merged_cube = merge_cubes(cube1=cube1, cube2=cube2)
    assert sorted(merged_cube[DEFAULT_BANDS_DIMENSION].values) == sorted(
        set(bands_1 + bands_2)
    )
    for dim_name, dim_size in expected_dim_size.items():
        actual_size = merged_cube[dim_name].size
        assert (
            actual_size == dim_size
        ), f"Error! Invalid size: {dim_name} actual={actual_size} expected={dim_size}"


@pytest.mark.parametrize(
    "period, expected_size",
    [
        (
            "day",
            10,
        ),
        (
            "month",
            2,
        ),
    ],
)
def test_aggregate_temporal_period(period: str, expected_size: int):
    size_x = 100
    size_y = 100
    if period == "day":
        freq = "H"
        num_periods = 24 * expected_size
    elif period == "month":
        freq = "W"
        num_periods = 4 * expected_size
    data = generate_xarray_datarray(
        bands=["B02"],
        latmax=41,
        latmin=40,
        size_x=size_x,
        lonmax=-90,
        lonmin=-91,
        size_y=size_y,
        timestamps=(pd.Timestamp(2020, 1, 1), None),
        freq=freq,
        num_periods=num_periods,
    )

    proc = TensorlakehouseProcessing()
    reducer = partial(
        proc.process_registry["mean"].implementation,
        data=ParameterReference(from_parameter="data"),
    )
    aggregated_data = aggregate_temporal_period(
        data=data, reducer=reducer, period=period
    )
    validate_raster_datacube(
        cube=aggregated_data,
        expected_dim_size={
            DEFAULT_X_DIMENSION: size_x,
            DEFAULT_Y_DIMENSION: size_y,
            DEFAULT_TIME_DIMENSION: expected_size,
        },
        expected_attrs={},
        expected_crs=crs.CRS.from_epsg(4326),
    )
