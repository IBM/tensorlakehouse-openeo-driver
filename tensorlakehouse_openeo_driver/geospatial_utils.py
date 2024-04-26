from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import numpy as np
from pystac import Item
import xarray as xr
import pandas as pd
from rasterio.crs import CRS
from tensorlakehouse_openeo_driver.constants import DEFAULT_TIME_DIMENSION
from rasterio.enums import Resampling


def clip(
    data: xr.DataArray, bbox: Tuple[float, float, float, float], x_dim: str, y_dim: str, crs: int = 4326
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
    data = data.rio.clip_box(minx=minx, miny=miny, maxx=maxx, maxy=maxy, crs=crs)
    # rename dimensions back to original
    data = rename_dimension(data=data, rename_dict={"x": x_dim, "y": y_dim})
    return data


def get_dimension_name(
    item: Item, axis: Optional[str] = None, dim_type: Optional[str] = None
) -> Optional[str]:
    item_properties = item.properties
    cube_dims = item_properties["cube:dimensions"]
    assert isinstance(cube_dims, dict)
    found = None
    i = 0
    dim_list = list(cube_dims.items())
    while i < len(dim_list) and not found:
        k, v = dim_list[i]
        i += 1
        original_axis = v.get("axis")
        if axis is not None and original_axis is not None and original_axis == axis:
            dimension_name = k
            found = True
        if dim_type is not None and v.get("type") is not None and v.get("type") == dim_type:
            dimension_name = k
            found = True
    if found:
        return dimension_name
    else:
        raise ValueError("Error! Unable to find axis = {}")


def rename_dimension(data: xr.DataArray, rename_dict: Dict[str, str]):
    for source, target in rename_dict.items():
        if source in data.dims:
            data = data.rename({source: target})
    return data


def filter_by_time(
    data: xr.DataArray, timestamps: List[pd.Timestamp], temporal_dim: str
) -> xr.DataArray:
    """filter out data that is not within bbox

    Args:
        data (xr.Dataset): data cube obtained from COS
        bbox (List[float]): area of interest
        crs (int): reference system

    Returns:
        xr.DataArray: filtered xarray
    """

    # Convert the selected timestamps to datetime objects
    selected_timestamps = [xr.cftime_range(time, time)[0] for time in timestamps]

    # Select the specific timestamps using the .sel() method
    selected_data = data.sel({temporal_dim: selected_timestamps}, method="nearest")
    assert isinstance(selected_data, xr.DataArray), f"Error! Unexpected type: {type(selected_data)}"
    return selected_data


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
        array_by_time = defaultdict(list)
        for index, t in enumerate(data_array[time_dim].values):
            slice_array = data_array.isel({time_dim: index})
            if t in array_by_time.keys():
                array_by_time[t] = array_by_time[t].combine_first(slice_array)
            else:
                array_by_time[t] = slice_array
        # print('length of concat list', len(arr_timestamp_lst))
        arr = xr.concat(array_by_time.values(), dim=time_dim, compat="override", coords="minimal")
        # print('arr.shape', arr.shape)
        return arr


def _remove_files_in_dir(dir_path: Path, prefix: str, suffix: str):
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
    shape: Tuple[int, int] = None,
) -> xr.DataArray:
    # We collect all available dimensions
    non_spatial_dimension_names = [dim for dim in data_cube.dims if dim not in ["y", "x"]]
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
    data_cube_stacked_reprojected = data_cube_stacked.transpose(
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
    data_cube_stacked_reprojected = data_cube_stacked_reprojected.transpose(*data_cube.dims)

    return data_cube_stacked_reprojected
