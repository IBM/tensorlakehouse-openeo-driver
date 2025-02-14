from typing import Dict, List, Tuple, Union
import xarray as xr
from datetime import datetime
import numpy as np
import geopandas
from shapely.geometry.polygon import Polygon
import pandas as pd

from tensorlakehouse_openeo_driver import geospatial_utils
from tensorlakehouse_openeo_driver.constants import EPSG_4326
from tensorlakehouse_openeo_driver.geospatial_utils import reproject_bbox
from shapely.wkt import loads


def validate_raster_datacube(
    cube: xr.DataArray,
    spatial_extent: Union[Dict[str, float], str],
    temporal_extent: Tuple[datetime, datetime],
    expected_dims: Dict[str, str],
    expected_crs: str,
):
    # validate the size of each dimension
    actual_dims = cube.sizes
    assert (
        expected_dims == actual_dims
    ), f"Error! {expected_dims=} dimensions is different than {actual_dims=}"
    # compare coordinate reference system
    cube_crs: str = cube.rio.crs.to_string()
    assert (
        cube_crs.upper() == expected_crs.upper()
    ), f"Error! actual CRS {cube_crs} is different than expected CRS {expected_crs}"
    # compare the values of the coordinates of datacube and the values specified by the user
    Y_DIM = X_DIM = None
    for k in expected_dims.keys():
        if k.lower() in ["x", "lon", "longitude"]:
            X_DIM = k
        elif k.lower() in ["y", "lat", "latitude"]:
            Y_DIM = k

    tolerance_x_dim = 0.1
    tolerance_y_dim = 0.1

    # coordinates of the downloaded data
    minx = np.min(cube[X_DIM].values)
    maxx = np.max(cube[X_DIM].values)

    miny = np.min(cube[Y_DIM].values)
    maxy = np.max(cube[Y_DIM].values)

    # get the coordinates specified by the end-user
    if isinstance(spatial_extent, dict):
        user_west = spatial_extent["west"]
        user_east = spatial_extent["east"]
        user_south = spatial_extent["south"]
        user_north = spatial_extent["north"]
    else:
        assert isinstance(spatial_extent, str)
        p = loads(spatial_extent)
        user_west, user_south, user_east, user_north = p.bounds

    # reproject to WSG84
    west, south, east, north = reproject_bbox(
        bbox=(minx, miny, maxx, maxy), dst_crs=4326, src_crs=expected_crs
    )

    if cube[X_DIM].size > 1:
        assert (
            user_west - tolerance_x_dim <= west
        ), f"Error! {west=} {user_west=} {tolerance_x_dim=}"
        assert (
            user_east >= east - tolerance_x_dim
        ), f"Error! {east=} {user_east=} {tolerance_x_dim=}"
    if cube[Y_DIM].size > 1:
        assert (
            user_south - tolerance_y_dim * 2 <= south
        ), f"Error! {south=} {user_south=} {tolerance_x_dim=}"
        assert (
            user_north >= north - tolerance_y_dim * 2
        ), f"Error! {north=} {user_north=} {tolerance_x_dim=}"


def _guess_column(columns: List[str], pattern: str) -> str:
    found = False
    i = 0
    while i < len(columns) and not found:
        c = columns[i]
        i += 1
        if pattern.lower() in c.lower():
            found = True

    assert found
    return c


def validate_vector_datacube(
    vector_cube: geopandas.GeoDataFrame,
    spatial_extent: Dict[str, Union[float, int, str]],
    temporal_extent: Tuple[str, str],
    expected_dimensions: Dict[str, int],
    bands: List[str],
):
    """validate if resulting vector cube contains the expected columns, expected temporal extent,
    and expected spatial extent

    Args:
        vector_cube (geopandas.GeoDataFrame): resulting vector cube
        spatial_extent (Dict[str, float]): west, south, east, north dict
        temporal_extent (Tuple[str, str]): start and end datetime
        expected_dimensions (Dict[str, int]): name of dimension and its size
        bands (List[str]): list of band names
    """
    dimension_names = list(expected_dimensions.keys())
    expected_columns = dimension_names
    assert all(
        col in vector_cube.columns for col in expected_columns
    ), f"Error! Missing column: {expected_columns=} {vector_cube.columns=}"
    assert vector_cube.shape[0] > 0
    # guess geometry dim name
    geometry_dim_name = _guess_column(columns=vector_cube.columns, pattern="geom")
    # guess temporal dim name
    temporal_dim_name = _guess_column(columns=vector_cube.columns, pattern="time")

    # todo: the geom in vector_cube is in crs of data, need to convert to crs of spatial_extent or visa-versa
    if "crs" not in spatial_extent.keys():

        crs: Union[int, str] = EPSG_4326
    else:
        assert isinstance(
            spatial_extent["crs"], (int, str)
        ), f"Error! Unexpected data type: {spatial_extent=}"
        crs = spatial_extent["crs"]
    # validate if returned area of data is within the input area
    geom: Polygon
    if hasattr(vector_cube, "crs") and vector_cube.crs is not None:
        src_crs = vector_cube.crs.to_string()
    else:
        src_crs = EPSG_4326
    for geom in vector_cube[geometry_dim_name]:
        bbox = geospatial_utils.reproject_bbox(
            geom.bounds, src_crs=src_crs, dst_crs=crs
        )
        bounds = bbox
        west = spatial_extent["west"]
        assert isinstance(west, (int, float)), f"Error! west is invalid: {west=}"
        east = spatial_extent["east"]
        assert isinstance(east, (int, float)), f"Error! east is invalid: {east=}"
        assert west <= bounds[0] <= bounds[2] <= east

    # validate if time range of the returned data is within the input time range
    start = pd.Timestamp(temporal_extent[0])
    end = pd.Timestamp(temporal_extent[1])
    for t in vector_cube[temporal_dim_name]:
        pdts = pd.Timestamp(t)
        if pdts.tzinfo is None:
            pdts = pdts.tz_localize(tz="UTC")
        assert start <= pdts <= end, f"Error! {start=} {pdts=} {end=}"
