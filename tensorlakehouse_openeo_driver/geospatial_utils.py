from collections import defaultdict
from typing import Dict, List, Optional, Tuple, Union, DefaultDict
import geojson
import numpy as np
import pyproj
import xarray as xr
import pandas as pd
from rasterio.crs import CRS
from tensorlakehouse_openeo_driver.constants import (
    DEFAULT_TIME_DIMENSION,
    DEFAULT_X_DIMENSION,
    DEFAULT_Y_DIMENSION,
)
from rasterio.enums import Resampling
from datetime import datetime
from rioxarray.exceptions import OneDimensionalRaster
import bisect
from cftime._cftime import Datetime360Day
import pytz
from shapely.geometry.polygon import Polygon
from shapely.geometry import shape

xr.set_options(keep_attrs=True)


def get_xarray_coord(data: xr.DataArray, dimension: str) -> str | None:
    """find coordinate name of a given dimension

    Args:
        data (xr.DataArray): _description_
        dimension (str): _description_

    Returns:
        str | None: _description_
    """
    # initialize variable
    coord_name = None
    # hardcoded values of longitude and latitude
    longitude_list = [DEFAULT_X_DIMENSION, "longitude", "lon", "long"]
    latitude_list = [DEFAULT_Y_DIMENSION, "latitude", "lat"]
    # assumption: dimension must one of the hardcoded values
    if dimension in longitude_list:
        possible_values = longitude_list
    elif dimension in latitude_list:
        possible_values = latitude_list
    else:
        raise ValueError(f"Error! Unable to find a coord that has {dimension=}")

    coordinates = list(data.coords.keys())
    found = False
    i = 0
    while i < len(coordinates) and not found:
        coord = coordinates[i]
        i += 1
        coord_dims = list(data.coords[coord].dims)

        if len(coord_dims) == 1 and dimension in coord_dims:
            coord_name = str(coord)
            found = True
            break
        elif (
            len(coord_dims) > 1 and dimension in coord_dims and coord in possible_values
        ):
            coord_name = str(coord)
            found = True
    return coord_name


def _rename_coords(
    data: xr.DataArray, y_coord: str, x_coord: str, y_dim: str, x_dim: str
) -> xr.DataArray:
    rename_dict = dict()
    if y_coord != y_dim:
        rename_dict[y_coord] = y_dim
    if x_coord != x_dim:
        rename_dict[x_coord] = x_dim
    if len(rename_dict) > 0:
        data = data.rename(rename_dict)
    return data


def _clip_curvilinear_raster(
    data: xr.DataArray,
    bbox: Tuple[float, float, float, float],
    x_dim: str,
    y_dim: str,
    x_coord: str,
    y_coord: str,
    crs: Optional[int] = 4326,
) -> xr.DataArray:
    # convert longitude values between [0,360] to [-180,180]
    data = data.assign_coords({x_coord: (((data[x_coord] + 180) % 360) - 180)})
    minx, miny, maxx, maxy = bbox
    mask = (
        (data[y_coord] >= miny)
        & (data[y_coord] <= maxy)
        & (data[x_coord] >= minx)
        & (data[x_coord] <= maxx)
    )

    data = data.where(
        mask,
        drop=True,
    )
    return data


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
        bbox (List[float]): area of interest (west, south, east, north)
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
    # get coords
    x_coord = get_xarray_coord(data=data, dimension=x_dim)
    assert x_coord is not None
    y_coord = get_xarray_coord(data=data, dimension=y_dim)
    assert y_coord is not None
    # "xarray disallows variables with more than 1 dimension that share a name with one of their
    # dimensions to avoid conflicts and ambiguity when accessing data". Thus, when coordinates
    # have two dimensions, we rely on "where()" to clip the data
    if any(c is not None and len(data.coords[c].dims) > 1 for c in [x_coord, y_coord]):
        data = _clip_curvilinear_raster(
            data=data,
            bbox=bbox,
            x_coord=x_coord,
            y_coord=y_coord,
            x_dim=x_dim,
            y_dim=y_dim,
            crs=crs,
        )
    else:

        data = data.assign_coords({x_coord: (((data[x_coord] + 180) % 360) - 180)})
        data = _rename_coords(
            data=data, x_coord=x_coord, x_dim=x_dim, y_coord=y_coord, y_dim=y_dim
        )
        # clip_box works if coords and dims have the same name
        rename_dict = dict()
        if y_dim != DEFAULT_Y_DIMENSION:
            rename_dict[y_dim] = DEFAULT_Y_DIMENSION
        if x_dim != DEFAULT_X_DIMENSION:
            rename_dict[x_dim] = DEFAULT_X_DIMENSION
        if len(rename_dict) > 0:
            data = data.rename(rename_dict)

        # adjust user input based on the limits of the data coordinates
        minx = max(minx, min(data[DEFAULT_X_DIMENSION].values.flatten()))
        maxx = min(maxx, max(data[DEFAULT_X_DIMENSION].values.flatten()))
        assert minx < maxx, f"Error! {minx=} >= {maxx=}"
        miny = max(miny, min(data[DEFAULT_Y_DIMENSION].values.flatten()))
        maxy = min(maxy, max(data[DEFAULT_Y_DIMENSION].values.flatten()))
        assert miny < maxy, f"Error! {miny=} >= {maxy=}"

        try:
            data = data.rio.clip_box(
                minx=minx, miny=miny, maxx=maxx, maxy=maxy, crs=crs
            )
            # restore original dimension names
            reversed_dict = {v: k for k, v in rename_dict.items()}
            if len(reversed_dict) > 0:
                data = data.rename(reversed_dict)
        except TypeError:
            # handling the case when a given coord has multiple dimensions (curvilinear)
            data = data.where(
                (data.x <= maxx)
                & (data.x >= minx)
                & (data.y <= maxy)
                & (data.y >= miny),
                drop=True,
            )
        except OneDimensionalRaster:
            # handling exception when resulting dataarray has either x or y 1-size dimension

            # assumption: coordinates are sorted
            # get index of x that is smaller than minx
            minx_index = bisect.bisect_left(a=data.x.values.flatten(), x=minx)
            # get index of x that is greater than maxx
            maxx_index = bisect.bisect_right(a=data.x.values.flatten(), x=maxx)
            if minx_index == maxx_index:
                if minx_index > 0:
                    minx_index -= 1
                else:
                    maxx_index += 1

            # get index of y that is smaller than miny
            miny_index = bisect.bisect_left(a=data.y.values.flatten(), x=miny)
            # get index of y that is smaller than maxy
            maxy_index = bisect.bisect_right(a=data.y.values.flatten(), x=maxy)
            if miny_index == maxy_index:
                if miny_index > 0:
                    miny_index -= 1
                else:
                    maxy_index += 1
            selector = {
                "x": slice(minx_index, maxx_index),
                "y": slice(miny_index, maxy_index),
            }

            data = data.isel(selector)
        # rename dimensions back to original
        assert isinstance(data, xr.DataArray)
    return data


def rename_vars(data: xr.Dataset) -> xr.Dataset:
    for var in data.variables:
        if var == DEFAULT_TIME_DIMENSION:
            data = data.rename_vars({var: "temp"})
    return data


def expand_time_dimension(
    data: xr.Dataset, time_dim: str | None, dt: str | None
) -> xr.Dataset:
    """
    Expands the time dimension in the given xarray Dataset.

    Parameters:
    data (xr.Dataset): The input xarray Dataset.
    time_dim (str | None): The name of the time dimension to expand. If None, no expansion is performed.
    dt (str | None): A string representing a date-time in the format 'YYYY-MM-DD HH:MM:SS'. If provided, the time dimension is expanded with this date-time.

    Returns:
    xr.Dataset: The xarray Dataset with the time dimension expanded.
    """
    if (
        # if time_dim is None then it is not one of the dimensions
        (time_dim is None or time_dim not in data.dims)
        # the default time dimension must not be one of the dimensions
        and DEFAULT_TIME_DIMENSION not in data.dims
        # if dt is none we cannot use it
        and dt is not None
    ):
        ts = pd.Timestamp(dt)
        pydt = ts.to_pydatetime()
        if time_dim is None:
            time_dim = DEFAULT_TIME_DIMENSION
        data = data.expand_dims({time_dim: [pydt]})
    return data


def create_missing_coords(data: xr.Dataset, time_dim: str | None) -> xr.Dataset:
    # create a new coordinate to be attached to an existing dimension
    if DEFAULT_TIME_DIMENSION in list(data.dims) and not any(
        t in list(data.coords) for t in [time_dim, DEFAULT_TIME_DIMENSION]
    ):
        time_values = data[DEFAULT_TIME_DIMENSION].values
        data = data.assign_coords(
            {DEFAULT_TIME_DIMENSION: (DEFAULT_TIME_DIMENSION, time_values)}
        )

    return data


def rename_dimensions(
    data: xr.Dataset,
    x_dim: str | None = DEFAULT_X_DIMENSION,
    time_dim: str | None = DEFAULT_TIME_DIMENSION,
    y_dim: str | None = DEFAULT_Y_DIMENSION,
) -> xr.Dataset:
    # Assisted by watsonx Code Assistant
    """
    Renames dimensions in an xarray Dataset.

    Args:
        data (xr.Dataset): The input xarray Dataset to rename dimensions in.
        x_dim (str, optional): The current name of the x-dimension. Defaults to DEFAULT_X_DIMENSION.
        time_dim (str, optional): The current name of the time dimension. Defaults to DEFAULT_TIME_DIMENSION.
        y_dim (str, optional): The current name of the y-dimension. Defaults to DEFAULT_Y_DIMENSION.

    Returns:
        xr.Dataset: The xarray Dataset with renamed dimensions.

    Raises:
        ValueError: If any of the provided dimension names do not exist in the input Dataset.

    This function renames the dimensions of an xarray Dataset based on the provided parameters.
    If a dimension name is provided and it exists in the Dataset, it will be renamed to the corresponding default dimension name.
    If no dimension name is provided or it matches the default dimension name, the dimension remains unchanged.

    The function returns the modified Dataset with the renamed dimensions.
    If any of the provided dimension names do not exist in the input Dataset, a ValueError is raised.

    The default dimension names are defined as constants:
    - DEFAULT_X_DIMENSION
    - DEFAULT_TIME_DIMENSION
    - DEFAULT_Y_DIMENSION

    These constants should be defined elsewhere in the codebase.
    """
    rename_dict = dict()
    if x_dim is not None and x_dim != DEFAULT_X_DIMENSION and x_dim in data.dims.keys():
        rename_dict[x_dim] = DEFAULT_X_DIMENSION
    if y_dim is not None and y_dim != DEFAULT_Y_DIMENSION and y_dim in data.dims.keys():
        rename_dict[y_dim] = DEFAULT_Y_DIMENSION
    if (
        time_dim is not None
        and time_dim != DEFAULT_TIME_DIMENSION
        and time_dim in data.dims.keys()
    ):
        rename_dict[time_dim] = DEFAULT_TIME_DIMENSION
    if len(rename_dict) > 0:
        data = data.rename_dims(rename_dict)
    return data


def _convert_to_datetime(
    datetime_index: List[Union[str, datetime, np.datetime64, Datetime360Day, int]],
) -> List[datetime]:
    """convert a list of datetime values to native datetime

    Args:
        datetime_index (_type_): _description_


    Returns:
        List[datetime]: list of timezone aware datetime objects
    """
    dt = datetime_index[0]
    timestamps: List[datetime] = list()
    if isinstance(dt, str) or isinstance(dt, datetime) or isinstance(dt, np.datetime64):
        for dt in datetime_index:
            ts = pd.Timestamp(dt)
            if ts.tzinfo is None:
                ts = ts.tz_localize(tz="UTC")
            timestamps.append(ts.to_pydatetime())
    elif isinstance(dt, Datetime360Day):
        for dt in datetime_index:
            julian = (dt.month - 1) * 30 + dt.day

            ts = pd.to_datetime(
                f"{dt.year}-{julian}T{dt.hour}:{dt.minute}:{dt.second}",
                format="%Y-%jT%H:%M:%S",
            )
            if ts.tzinfo is None:
                ts = ts.tz_localize(tz="UTC")
            timestamps.append(ts.to_pydatetime())
    elif isinstance(dt, int):
        for dt in datetime_index:
            assert isinstance(dt, int)
            timestamps.append(
                pd.Timestamp.fromtimestamp(dt / 1e9, tz="UTC").to_pydatetime()
            )
    return timestamps


def filter_by_time(
    data: Union[xr.DataArray, xr.Dataset],
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

    if isinstance(data, xr.Dataset):
        data = data.to_array()

    # convert 360 calendar to gregorian
    if isinstance(data[temporal_dim].values[0], Datetime360Day):
        data = data.convert_calendar(
            calendar="gregorian", dim=temporal_dim, align_on="year", use_cftime=False
        )
    start_datetime = temporal_extent[0]
    end_datetime = temporal_extent[1]
    ts = data[temporal_dim].values
    assert len(ts) > 0, "Error! temporal dimension is empty"
    # if end_datetime is None it is a open ended interval
    if end_datetime is None:
        end_datetime = sorted(ts)[-1]
    if start_datetime.tzinfo is None:
        start_datetime = pytz.UTC.localize(start_datetime)

    if end_datetime.tzinfo is None:
        end_datetime = pytz.UTC.localize(end_datetime)

    # convert temporal index to datetime timezone-aware
    timestamps = _convert_to_datetime(datetime_index=ts)
    # if length of timestamps equals 2, timestamsps have been converted
    if len(timestamps) > 0:
        start_index = bisect.bisect_left(timestamps, start_datetime)
        end_index = bisect.bisect_right(timestamps, end_datetime)
        if start_index == end_index:
            data = data.isel({temporal_dim: [start_index]})
        else:
            data = data.isel({temporal_dim: slice(start_index, end_index)})
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
    crs_from: CRS = _get_epsg(crs_code=src_crs)
    crs_to: CRS = _get_epsg(crs_code=dst_crs)
    if crs_from.to_epsg() == crs_to.to_epsg():
        return bbox

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


def convert_bbox_to_polygon(bbox: Tuple[float, float, float, float]) -> Polygon:
    west, south, east, north = bbox
    p = Polygon([[west, south], [east, south], [east, north], [west, north]])
    assert p.is_valid
    return p


def to_geojson(geom: Polygon, output_format: str = "dict") -> Union[Dict, str]:
    """convert shapely Polygon to either dict or str

    Args:
        geom (Polygon): geometry
        output_format (str, optional): _description_. Defaults to "dict".

    Returns:
        Union[Dict, str]: geojson
    """
    assert isinstance(geom, Polygon), f"Error! not a polygon: {type(geom)}"
    poly = geojson.Polygon(list(geom.exterior.coords))
    if output_format == "dict":
        output = dict(poly)
        assert isinstance(output, dict)
    else:
        output = geojson.dumps(poly)
        assert isinstance(output, str)
    return output


def from_geojson_to_polygon(geom_dict: Dict) -> Polygon:

    geom = shape(geom_dict)
    assert geom.is_valid
    return geom


def from_bbox_to_polygon(bbox: Tuple[float, float, float, float]) -> Polygon:
    """generates a polygon from a bounding box

    Args:
        bbox (Tuple[float, float, float, float]): right, bottom, left, top

    Returns:
        Polygon: _description_
    """
    west, south, east, north = bbox
    assert west <= east, f"Error! Invalid values: {west=} {east=}"
    assert south <= north, f"Error! Invalid values: {south=} {north=}"
    p = Polygon([[west, south], [west, north], [east, north], [east, south]])
    assert p.is_valid, f"Error! Invalid polygon {p=}"
    return p


def convert_longitude_coords(lon: float) -> float:
    new_lon = float(((lon + 180.0) % 360.0) - 180.0)
    return new_lon


def main():
    np.random.seed(0)
    temperature = 15 + 8 * np.random.randn(4, 4, 3)
    lon = [
        [-99.83, -99.32, -99.15, -99.05],
        [-99.79, -99.23, -99.10, -99.05],
        [-99.80, -99.24, -99.11, -99.06],
        [-99.81, -99.25, -99.12, -99.07],
    ]
    lat = [
        [42.25, 42.21, 42.15, 42.10],
        [42.63, 42.59, 42.44, 42.30],
        [42.27, 42.23, 42.17, 42.12],
        [42.65, 42.61, 42.46, 42.32],
    ]
    time = pd.date_range("2014-09-06", periods=3)
    reference_time = pd.Timestamp("2014-09-05")
    da = xr.DataArray(
        data=temperature,
        dims=["x", "y", "time"],
        coords=dict(
            lon=(["x", "y"], lon),
            lat=(["x", "y"], lat),
            time=time,
            reference_time=reference_time,
        ),
        attrs=dict(
            description="Ambient temperature.",
            units="degC",
        ),
    )

    bbox = (-99.79, 42.23, -99.11, 42.46)
    clip_box(data=da, bbox=bbox, x_dim="lon", y_dim="lat", crs=4326)


if __name__ == "__main__":
    main()
