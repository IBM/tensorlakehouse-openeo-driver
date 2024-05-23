from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union, DefaultDict
import numpy as np
import pyproj
import xarray as xr
import pandas as pd
from rasterio.crs import CRS
from tensorlakehouse_openeo_driver.constants import DEFAULT_TIME_DIMENSION
from rasterio.enums import Resampling
from datetime import datetime
from dateutil import tz
from rioxarray.exceptions import OneDimensionalRaster
import bisect


def clip_box(
    data: xr.DataArray,
    bbox: Tuple[float, float, float, float],
    x_dim: str,
    y_dim: str,
    crs: Optional[int] = 4326,
) -> xr.DataArray:
    """filter out data that is not within bbox

    Args:
        data (xr.Dataset): data cube obtained from COS
        bbox (List[float]): area of interest
        crs (int): reference system
        items (List[Item]): list of STAC items

    Returns:
        xr.DataArray: filtered xarray
    """
    # set CRS
    if data.rio.crs is None:
        input_crs = CRS.from_epsg(crs)
        data.rio.write_crs(input_crs, inplace=True)
    # area selected by the end-user
    minx, miny, maxx, maxy = bbox
    # rename dimensions because clip_box accepts only x and y
    data = rename_dimension(data=data, rename_dict={x_dim: "x", y_dim: "y"})
    # clip data
    minx = max(minx, min(data["x"].values))
    maxx = min(maxx, max(data["x"].values))

    miny = max(miny, min(data["y"].values))
    maxy = min(maxy, max(data["y"].values))
    try:
        data = data.rio.clip_box(minx=minx, miny=miny, maxx=maxx, maxy=maxy, crs=crs)
    except OneDimensionalRaster:
        # handling exception when resulting dataarray has either x or y 1-size dimension 
        
        # assumption: coordinates are sorted
        # get index of x that is smaller than minx
        minx_index = bisect.bisect_left(a=data.x.values, x=minx)
        # get index of x that is greater than maxx
        maxx_index = bisect.bisect_right(a=data.x.values, x=maxx)
        if minx_index == maxx_index:
            if minx_index > 0:
                minx_index -= 1
            else:
                maxx_index += 1

        # get index of y that is smaller than miny
        miny_index = bisect.bisect_left(a=data.y.values, x=miny)
        # get index of y that is smaller than maxy
        maxy_index = bisect.bisect_right(a=data.y.values, x=maxy)
        if miny_index == maxy_index:
            if miny_index > 0:
                miny_index -= 1
            else:
                maxy_index += 1
        selector = {
                "x": slice(minx_index, maxx_index),
                "y": slice(miny_index, maxy_index)
            }

        data = data.isel(selector)
    # rename dimensions back to original
    data = rename_dimension(data=data, rename_dict={"x": x_dim, "y": y_dim})
    return data



def rename_dimension(data: xr.DataArray, rename_dict: Dict[str, str]):
    for source, target in rename_dict.items():
        if source in data.dims:
            data = data.rename({source: target})
    return data


def filter_by_time(
    data: xr.DataArray,
    temporal_extent: Tuple[datetime, Optional[datetime]],
    temporal_dim: str,
) -> xr.DataArray:
    """filter data by timestamp

    Args:
        data (xr.DataArray): datacube
        temporal_extent (Tuple[datetime, datetime]): start and end datetime
        temporal_dim (str): name of the temporal dimension


    Returns:
        xr.DataArray: datacube
    """
    start_datetime = temporal_extent[0]
    end_datetime = temporal_extent[1]
    ts = data[temporal_dim].values
    assert len(ts) > 0, "Error! temporal dimension is empty"
    # if end_datetime is None it is a open ended interval
    if end_datetime is None:
        end_datetime = sorted(ts)[-1]
    # get sample timestamp
    sample_ts = pd.Timestamp(ts[0])
    # if timestamp is naive set tz to None
    if sample_ts.tzinfo is None:
        start_datetime = start_datetime.astimezone(tz.tzutc()).replace(tzinfo=None)
        end_datetime = end_datetime.astimezone(tz.tzutc()).replace(tzinfo=None)
    else:
        start_datetime = start_datetime.astimezone(tz.tzutc())
        end_datetime = end_datetime.astimezone(tz.tzutc())
    data = data.sel({temporal_dim: slice(start_datetime, end_datetime)})
    return data


def remove_repeated_time_coords(
    data_array: xr.DataArray, time_dim: str = DEFAULT_TIME_DIMENSION
) -> xr.DataArray:
    """Squeeze duplicate timestamps into unique timestamps.
    This function keeps the time dimension but merges duplicate timestamps by backward filling nan values.
    """
    assert time_dim in data_array.dims, f"Error! {time_dim} is not in {data_array.dims}"
    # if there is no repeated timestamp, return same array
    if len(set(data_array[time_dim].values)) == len(data_array[time_dim].values):
        return data_array
    else:
        array_by_time: DefaultDict = defaultdict(list)
        for index, t in enumerate(data_array[time_dim].values):
            slice_array = data_array.isel({time_dim: index})
            if t in array_by_time.keys():
                array_by_time[t] = array_by_time[t].combine_first(slice_array)
            else:
                array_by_time[t] = slice_array
        # print('length of concat list', len(arr_timestamp_lst))
        arr: xr.DataArray = xr.concat(
            array_by_time.values(), dim=time_dim, compat="override", coords="minimal"
        )

        return arr


def remove_files_in_dir(dir_path: Path, prefix: str, suffix: str):
    files = _find_files_in_dir(dir_path=dir_path, prefix=prefix, suffix=suffix)
    for f in files:
        f.unlink()


def _find_files_in_dir(dir_path: Path, prefix: str, suffix: str) -> List[Path]:
    file_list = list()
    assert dir_path.exists()
    assert dir_path.is_dir()
    p = dir_path.glob("**/*")
    files = [x for x in p if x.is_file()]
    for f in files:
        parts = f.parts
        filename = parts[-1]
        if filename.startswith(prefix) and filename.endswith(suffix):
            file_list.append(f)
    return file_list


def reproject_cube(
    data_cube: xr.DataArray,
    target_projection: CRS,
    resolution: Optional[float],
    resampling: Resampling,
    shape: Optional[Tuple[int, int]] = None,
) -> xr.DataArray:
    # We collect all available dimensions
    non_spatial_dimension_names = [
        dim for dim in data_cube.dims if dim not in ["y", "x"]
    ]
    # This code assumes that all dimensions have coordinates.
    # I'm not aware of a use case we have where they not.
    # So we raise an exception if this fails.
    for dim in non_spatial_dimension_names:
        if dim not in data_cube.coords:
            raise ValueError(f"Dimension {dim} does not appear to have coordinates.")

    if "__unified_non_spatial_dimension__" in data_cube.dims:
        raise ValueError(
            "The data array must not contain a dimension with name `__unified_dimension__`."
        )

    # To reproject, we stack along a new dimension
    data_cube_stacked = data_cube.stack(
        dimensions={"__unified_non_spatial_dimension__": non_spatial_dimension_names},
        create_index=True,
    )
    # If we do not assign a no data value, we will get funny results
    if data_cube_stacked.rio.nodata is None:
        data_cube_stacked.rio.write_nodata(np.nan, inplace=True)
    assert data_cube_stacked.rio.nodata is not None

    # So we can finally reproject
    data_cube_stacked_reprojected: xr.DataArray = data_cube_stacked.transpose(
        "__unified_non_spatial_dimension__", "y", "x"
    ).rio.reproject(
        dst_crs=target_projection,
        resolution=resolution,
        resampling=resampling,
        shape=shape,
    )

    # In theory we would simply call `.unstack` to bring things back to the original form.
    # However, there seems to be a bug in rioxarray that multiindexes become indexes.
    # So we simply re-assign the old index since we did not touch it in the first place.
    data_cube_stacked_reprojected = data_cube_stacked_reprojected.assign_coords(
        {
            "__unified_non_spatial_dimension__": data_cube_stacked.indexes[
                "__unified_non_spatial_dimension__"
            ]
        }
    )
    # Now we can unstack
    data_cube_stacked_reprojected = data_cube_stacked_reprojected.unstack(
        "__unified_non_spatial_dimension__"
    )
    # And we bring the dimensions back to the original order
    data_cube_stacked_reprojected = data_cube_stacked_reprojected.transpose(
        *data_cube.dims
    )

    return data_cube_stacked_reprojected


def reproject_bbox(
    bbox: Tuple[float, float, float, float],
    dst_crs: Union[int, str],
    src_crs: Union[int, str] = 4326,
) -> Tuple[float, float, float, float]:
    """reproject bounding box to specified dst_crs

    Args:
        bbox (Tuple[float, float, float, float]): west, south, east, north
        dst_crs (Union[int, str]): destination CRS
        src_crs (Union[int, str], optional): source CRS. Defaults to 4326.

    Returns:
        Tuple[float, float, float, float]: reprojected bbox
    """
    crs_from = _get_epsg(crs_code=src_crs)
    crs_to = _get_epsg(crs_code=dst_crs)

    transformer = pyproj.Transformer.from_crs(
        crs_from=crs_from, crs_to=crs_to, always_xy=True
    )
    minx, miny, maxx, maxy = bbox
    assert minx <= maxx, f"Error! {minx=} <= {maxx=} is false"
    assert miny <= maxy, f"Error! {miny=} <= {maxy=} is false"
    repr_minx, repr_miny = transformer.transform(minx, miny)
    repr_maxx, repr_maxy = transformer.transform(maxx, maxy)
    assert repr_minx <= repr_maxx, f"Error! {repr_minx=} <= {repr_maxx=}"
    assert repr_miny <= repr_maxy, f"Error! {repr_miny=} <= {repr_maxy=}"
    return (repr_minx, repr_miny, repr_maxx, repr_maxy)


def _get_epsg(crs_code: Union[str, int]) -> CRS:
    if isinstance(crs_code, str):
        crs_code = int(crs_code.split(":")[1])
    crs_obj = pyproj.CRS.from_epsg(crs_code)
    return crs_obj
