import inspect
import logging
from collections import namedtuple
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union
from dask.array.core import Array
from rasterio.enums import Resampling
from tensorlakehouse_openeo_driver.process_implementations.load_collection import (
    LoadCollectionFromCOS,
)
import geopandas as gpd
import numpy as np
import openeo
import pandas as pd
import pyproj
import xarray as xr
from openeo_driver.errors import ProcessParameterInvalidException
from openeo_pg_parser_networkx.graph import Callable
from openeo.udf.udf_data import UdfData
from openeo.udf.xarraydatacube import XarrayDataCube
from openeo_pg_parser_networkx.pg_schema import (
    BoundingBox,
    TemporalInterval,
    TemporalIntervals,
)
from openeo.udf.run_code import run_udf_code
from openeo_processes_dask.process_implementations.data_model import (
    RasterCube,
    VectorCube,
)
from openeo_processes_dask.process_implementations.exceptions import (
    DimensionNotAvailable,
    OverlapResolverMissing,
    TooManyDimensions,
)
from openeo_processes_dask.process_implementations.math import (
    mean as openeo_processes_dask_mean,
)

from pyproj import Transformer
from rasterio import crs
from shapely.geometry import shape
from shapely.geometry.polygon import Polygon

from tensorlakehouse_openeo_driver.constants import (
    DEFAULT_BANDS_DIMENSION,
    GTIFF,
    NETCDF,
    PARQUET,
    STAC_DATETIME_FORMAT,
    STAC_URL,
    DEFAULT_TIME_DIMENSION,
    ZIP,
    DEFAULT_X_DIMENSION,
    DEFAULT_Y_DIMENSION,
)
from tensorlakehouse_openeo_driver.driver_data_cube import TensorLakehouseDataCube
from tensorlakehouse_openeo_driver.save_result import GeoDNImageCollectionResult
from tensorlakehouse_openeo_driver.geospatial_utils import reproject_cube
from tensorlakehouse_openeo_driver.stac import make_stac_client

logging.config.fileConfig(fname="logging.conf", disable_existing_loggers=False)
logger = logging.getLogger("geodnLogger")

NEW_DIM_NAME = "__cubes__"
NEW_DIM_COORDS = ["cube1", "cube2"]


Overlap = namedtuple("Overlap", ["only_in_cube1", "only_in_cube2", "in_both"])
GEOJSON = "GEOJSON"
# TODO remove hardcoded EPSG
CRS_EPSG_4326 = "epsg:4326"


def rename_dimension(data: RasterCube, source: str, target: str) -> RasterCube:
    """rename one of the dimensions of the datacube

    Args:
        data (RasterCube): xarray.DataArray that contains the data
        source (str): name to be replace
        target (str): new name

    Raises:
        ProcessParameterInvalidException: _description_
        NotImplementedError: _description_
        an: _description_
        ValueError: _description_

    Returns:
        RasterCube: datacube with new dim
    """
    logger.debug(f"rename_dimension - source={source} target={target}")
    assert source is not None
    assert isinstance(source, str)
    assert target is not None
    assert isinstance(target, str)
    # target value cannot be one of the existing dimensions
    if target in data.dims:
        raise ProcessParameterInvalidException(
            parameter=target,
            process="rename_dimension",
            reason="Error! target dimension name already exists",
        )
    # source must exist to execute rename
    if source in data.dims:
        data = data.rename({source: target})
    return data


def rename_labels(
    data: RasterCube,
    dimension: str,
    source: List[Union[str, int]],
    target: List[Union[str, int]],
) -> RasterCube:
    """rename labels of the datacube. In this case, labels are xarray.DataArray.coords values

    Args:
        data (RasterCube): xarray.DataArray that contains the data
        source (str): label values to be replaced
        target (str): new label values
        dimension (str): dimension name that will be changed

    Returns:
        RasterCube: datacube with new dim
    """
    logger.debug(f"rename_labels - dimension={dimension} target={target}")
    assert dimension is not None
    assert isinstance(dimension, str)
    assert target is not None

    if dimension in data.dims and source is not None:
        assert len(target) == len(
            source
        ), "LabelMismatch: The number of labels in the parameters `source` and `target` don't match."
        labels = data[dimension].values
        for s, t in zip(source, target):
            # replace source label with target label
            labels[labels == s] = t
        data = data.assign_coords({dimension: labels})
    elif dimension in data.dims and len(target) == len(data[dimension]):
        data = data.assign_coords({dimension: target})
    else:
        # TODO: Add errors form openeo, e.g. LabelNotAvailable, LabelMismatch
        raise ValueError(
            "LabelNotAvailable: A label with the specified name does not exist"
        )
    return data


def _get_bounding_box(spatial_extent: BoundingBox) -> Tuple[float, float, float, float]:
    """get bounds

    Args:
        spatial_extent (BoundingBox): _description_

    Returns:
        Tuple[float, float, float, float]: west, south, east, north
    """

    latmax = spatial_extent.north
    latmin = spatial_extent.south
    lonmax = spatial_extent.east
    lonmin = spatial_extent.west
    pyproj_crs = pyproj.CRS.from_string(spatial_extent.crs)
    epsg4326 = pyproj.CRS.from_epsg(4326)

    if pyproj_crs != epsg4326:
        lonmin, latmin, lonmax, latmax = to_epsg4326(
            latmax=latmax,
            latmin=latmin,
            lonmax=lonmax,
            lonmin=lonmin,
            crs_from=pyproj_crs,
        )
    assert 90 >= latmax >= latmin >= -90, f"Error! latmax < latmin: {latmax} < {latmin}"

    assert (
        180 >= lonmax >= lonmin >= -180
    ), f"Error! lonmax < lonmin: {lonmax} < {lonmin}"
    return lonmin, latmin, lonmax, latmax


def to_epsg4326(
    latmax: float, latmin: float, lonmax: float, lonmin: float, crs_from: pyproj.CRS
):
    """convert

    Args:
        latmax (float): _description_
        latmin (float): _description_
        lonmax (float): _description_
        lonmin (float): _description_
        crs_from (pyproj.CRS): _description_

    Returns:
        _type_: _description_
    """
    epsg4326 = pyproj.CRS.from_epsg(4326)
    transformer = Transformer.from_crs(
        crs_from=crs_from, crs_to=epsg4326, always_xy=True
    )
    east, north = transformer.transform(lonmax, latmax)
    west, south = transformer.transform(lonmin, latmin)
    return west, south, east, north


def _get_start_and_endtime(
    temporal_extent: TemporalInterval,
) -> Tuple[datetime, datetime]:
    """extract start and endtime from TemporalInterval

    Args:
        temporal_extent (TemporalInterval): _description_

    Raises:
        NotImplementedError: _description_
        an: _description_
        ValueError: _description_

    Returns:
        Tuple[datetime, datetime]: start, end
    """
    logger.debug(
        f"Converting datetime start={temporal_extent.start} end={temporal_extent.end}"
    )
    starttime = pd.Timestamp(temporal_extent.start.to_numpy()).to_pydatetime()
    endtime = pd.Timestamp(temporal_extent.end.to_numpy()).to_pydatetime()
    assert starttime <= endtime, f"Error! start > end: {starttime} > {endtime}"
    return starttime, endtime


def save_result(
    data: Union[RasterCube, GeoDNImageCollectionResult],
    format: str,
    options: Dict = {},
) -> GeoDNImageCollectionResult:
    """creates a GeoDNImageCollectionResult object that will later be saved. If necessary
    manipulates xr.Dataset to facilitate the storage according to the file format

    Args:
        data (xr.Dataset): _description_
        format (Union[OutputFormat, str]): output format, e.g., netcdf
        options (Dict, optional): _description_. Defaults to {}.

    Raises:
        NotImplementedError: _description_

    Returns:
        GeoDNImageCollectionResult:
    """
    logger.debug(f"Running save_result process: format={str(format)}")

    if format is not None and not isinstance(format, str):
        format = format.__root__.upper()
    # support NETCDF format
    assert isinstance(format, str), f"Error! Unexpected type of format var: {format}"
    format = format.upper()
    if format == NETCDF:
        assert isinstance(
            data, xr.DataArray
        ), f"Error! data is not a xarray.Dataset: {type(data)}"
        if "reduced_dimensions_min_values" in data.attrs.keys():
            attr: Dict[str, np.datetime64] = data.attrs["reduced_dimensions_min_values"]
            time_dim = next(iter(attr.keys()))
            data.attrs["reduced_dimensions_min_values"] = pd.Timestamp(
                attr[time_dim]
            ).isoformat()
        return GeoDNImageCollectionResult(
            cube=TensorLakehouseDataCube(data=data), format=format, options=options
        )
    elif format in [GTIFF, ZIP]:
        assert isinstance(
            data, xr.DataArray
        ), f"Error! data is not a xarray.Dataset: {type(data)}"
        if "reduced_dimensions_min_values" in data.attrs.keys():
            attr = data.attrs["reduced_dimensions_min_values"]
            data.attrs["reduced_dimensions_min_values"] = pd.Timestamp(
                attr[DEFAULT_TIME_DIMENSION]
            ).isoformat()
        return GeoDNImageCollectionResult(
            cube=TensorLakehouseDataCube(data=data), format=format, options=options
        )
    elif format == PARQUET:
        return GeoDNImageCollectionResult(
            cube=TensorLakehouseDataCube(data=data), format=format, options=options
        )
    else:
        raise NotImplementedError(f"Support for {format} is not implemented")


def load_collection(
    id: str,
    spatial_extent: BoundingBox,
    temporal_extent: TemporalInterval,
    bands: Optional[List[str]],
    properties: Optional[Dict[str, Any]] = {},
) -> Union[RasterCube, VectorCube]:
    """pull data from the data source in which the collection is stored

    Args:
        id (str): collection ID
        spatial_extent (BoundingBox): bounding box specified by users
        temporal_extent (TemporalInterval): time interval
        bands (Optional[List[str]]): band unique ids
        properties (Dict[str, Any]): property names are the keys and conditions are the values


    Returns:
        xr.DataArray: a data cube which has x, y, bands dimensions and optionally t dimension
    """
    logger.debug(
        f"Running load_collection process: collectiond ID={id} STAC URL={STAC_URL}"
    )
    stac_catalog = make_stac_client(url=STAC_URL)
    # extract coordinates from BoundingBox object
    try:
        collection = stac_catalog.get_collection(id)
        extra_fields = collection.extra_fields
        cube_dimensions = extra_fields["cube:dimensions"]
        assert isinstance(
            cube_dimensions, dict
        ), f"Error! Unexpected type {cube_dimensions}"
        assert isinstance(bands, list), f"Error! Unexpected type: {bands}"
        dimension_names = _get_dimension_names(cube_dimensions=cube_dimensions)
        loader = LoadCollectionFromCOS()
        data = loader.load_collection(
            id=id,
            spatial_extent=spatial_extent,
            temporal_extent=temporal_extent,
            bands=bands,
            properties=properties,
            dimensions=dimension_names,
        )
        return data
    except Exception as e:
        msg = f"Error! collection_id={id} spatial_extent={spatial_extent} temporal_extent={temporal_extent} msg={e}"
        logger.error(msg=msg)
        raise e


def _get_dimension_names(cube_dimensions: Dict[str, Any]) -> Dict[str, str]:
    """this method parses the cube:dimensions field from STAC and extracts the type and name of
    each dimension in order to support load_collection process to rename the dimensions according
    to the way they were specified in STAC

    Args:
        cube_dimensions (Dict[str, Any]): this is field cube:dimensions as specified by datacube
            STAC extension

    Returns:
        Dict[str, str]: _description_
    """
    dimension_names = dict()
    for name, value in cube_dimensions.items():
        # type is a mandatory field
        dimension_type = value["type"]
        # if this is a horizontal spatial dimension, then it has axis (either x, y, or z)
        if dimension_type == "spatial":
            axis = value["axis"]
            dimension_names[axis] = name
        elif dimension_type == "temporal":
            dimension_names[DEFAULT_TIME_DIMENSION] = name
        elif dimension_type == "bands":
            dimension_names[DEFAULT_BANDS_DIMENSION] = name
        else:
            dimension_names[dimension_type] = name
    return dimension_names


def _load_collection_from_external_openeo_instance(
    collection_id: str,
    spatial_extent: BoundingBox,
    temporal_extent: TemporalInterval,
    bands: Optional[List[str]],
    properties=None,
) -> xr.DataArray:
    west, south, east, north = _get_bounding_box(spatial_extent=spatial_extent)
    start, end = _get_start_and_endtime(temporal_extent=temporal_extent)
    temporal_extent = [
        start.strftime(STAC_DATETIME_FORMAT),
        end.strftime(STAC_DATETIME_FORMAT),
    ]
    logger.debug(
        f"Submitting request to openeo.cloud: \ncollection={collection_id} \nbands={bands} \
         \ncoords={[west, south, east, north]} \ntemporal_extent={temporal_extent}"
    )
    connection = openeo.connect("openeo.cloud").authenticate_oidc()
    coll_metadata = connection.describe_collection(collection_id=collection_id)
    print(coll_metadata)
    datacube = connection.load_collection(
        collection_id=collection_id,
        spatial_extent={"south": south, "west": west, "north": north, "east": east},
        temporal_extent=temporal_extent,
        bands=bands,
    )
    datacube = datacube.min_time()
    result = datacube.save_result("GTiff")
    filename = "sample_file_sentinel2.tiff"
    result.download(filename)
    logger.debug("Result download from openeo.cloud")
    ds = xr.open_dataset(filename)
    return ds.to_array()


def aggregate_spatial(
    data: RasterCube,
    geometries: VectorCube,
    reducer: Callable,
    target_dimension: str = "result",
    **kwargs,
) -> VectorCube:
    """Compute spatial aggregation by applying the reducer function to the data array clipped to
    the iterable geometries argument which is a List[ (Geojson representation: Polygon, Line, Multipolygon,...).
    The clipping operation is defined in the rasterio function described here:
    https://corteva.github.io/rioxarray/html/rioxarray.html#rioxarray.raster_array.RasterArray
    The function returns a stacked GeoDataFrame object time	bands	spatial_ref	count	reduced	geometry
0	2022-01-02 19:12:02	B02	0	42736	2049.484650	POLYGON ((-2716931.681 5751311.779, -2713046.0...
1	2022-01-02 19:12:16	B02	0	45230	2030.256644	POLYGON ((-2716931.681 5751311.779, -2713046.0...
.
.
6	2022-01-02 19:12:02	B8A	0	42736	2732.596616	POLYGON ((-2716931.681 5751311.779, -2713046.0...
7	2022-01-02 19:12:16	B8A	0	45230	2701.098806	POLYGON ((-2716931.681 5751311.779, -2713046.0..

    The 'reduced' column is the stacked timeseries of applying the reducer for each band
    The 'count' column is the count of NaN pixels in each band at each timestamp in the clipped geometry

    Currently, this function clips over the Union of shapes in the geometries, and the entries in the 'geometry'\
    column is simply the first on in the List of input geometries.
    This will be improved in a subsequent version of the function that will iterate over the geometries,
    returning a stacked dataframe having the (reducer & count) timeseries for each band and geometry in the
    stacked GDF

    Notes:
    When the CRS of the the clip area differs from that of the data, the clip area is
    reprojected to that of the data.
    Alternatively, the data could be reprojected to the CRS of the clipping area. However, this latter choice
    seems less natural and efficient
            # data_array.rio.write_crs(clip_area.crs.to_string(), inplace=True) # sample code to reproject the data

    TODO: If the queried data array spans multiple CRS (e.g. UTM) we
    may have to reproject the data to a common CRS. This issue should be revisited.

    Args:
        data (RasterCube): _description_
        geometries (VectorCube): A  multi-polygon clip area, type GeoDataFrame or GeoJson
        reducer (Callable): _description_
        target_dimension (str, optional): _description_. Defaults to "result".

    Returns:
        VectorCube: GeopandasDataFrame

    TODO:
        the var: str applicable_band_dim can probably be replaced in favor of
        constants.py:: DEFAULT_BANDS_DIMENSION as I think thats now standard dim name 'bands'
    """
    logger.debug(f"Running aggregate_spatial process; geometries: {geometries}")
    logger.debug(f"kwargs: {kwargs}")

    result: VectorCube = None

    # Validate inputs
    assert isinstance(
        data, xr.DataArray
    ), f"Expecting xr.DataArray and not {type(data)}"

    # Accept geometries argument as GeoDataFrame or GeoJson
    # If geojson, convert to GeoDataFrame
    clip_area: gpd.GeoDataFrame
    if isinstance(geometries, gpd.GeoDataFrame):
        clip_area = geometries
    elif isinstance(geometries, Dict):  # GeoJson
        # clip_area = shapely.geometry.shape(geometries)
        clip_area = geojson_dict_to_geodataframe(geometries)
    else:
        raise ValueError(
            f"Invalid input; 'geometries' must be GeoDataFrame or GeoJson type: {type(geometries)}, value: {geometries}"
        )

    # If no crs associated with geometries, set to 4326
    if not hasattr(clip_area, "crs") or clip_area.crs is None:
        clip_area.set_crs(crs=CRS_EPSG_4326, inplace=True)
    print(f"clip_area.crs: {clip_area.crs}")

    y_dim = data.openeo.y_dim
    x_dim = data.openeo.x_dim
    band_dims = data.openeo.band_dims
    applicable_band_dim = band_dims[0]

    time_dims = data.openeo.temporal_dims[0]
    logger.debug(f"Dimensions: y={y_dim} x={x_dim} band={applicable_band_dim}")
    print(f"agg_spatial_data dimensions: {[band_dims, time_dims, x_dim, y_dim]}")

    # Reproject Clip area CRS to match Data CRS
    clip_crs = clip_area.crs.to_string()
    data_crs = data.rio.crs.to_string()

    if clip_crs != data_crs:
        clip_area = clip_area.to_crs({"init": data_crs})

    if target_dimension == "result":
        # check if clipping area is within bbox
        _check_geometries_within_data_boundaries(clip_area=clip_area, data=data)

        clipped = data.rio.clip(clip_area.geometry.values, clip_area.crs)
        count = clipped.count(dim=[x_dim, y_dim])
        count.name = "count"
        print(f"count_data_array: {count}")

        aggdata = clipped.reduce(
            reducer, dim=[x_dim, y_dim], keepdims=False, data=clipped
        )
        aggdata.name = "reduced"
        print(f"aggdata_data_array: {aggdata}")

        aggdata = xr.merge([count, aggdata])
        print(f"merge result: {aggdata}")

        result = _dataset_to_GPDF(aggdata, clip_area, [applicable_band_dim, time_dims])

    else:
        raise Exception(
            f"Invalid argument value for target_dimension: {target_dimension}, default is: {applicable_band_dim}"
        )

    # r = [type(x) for x in result.columns]
    # print(f"GDF columns: {r}, {result.columns}")
    print(f"spatial_aggregation result:\n{result.to_dict()}")

    return result


def _dataset_to_GPDF(
    dataset: xr.Dataset,
    geometries: gpd.GeoDataFrame,
    dims: List[str],
) -> gpd.GeoDataFrame:
    """
    Convert the xarray.Dataset to Geopandas as this is the format expected on the openeo_client side.
    This GDF is serialized to a temp file on the backend as part of the sae_result/download processing of
    the process graph. The temp file is streamed to the client as part of the download() process graph operation
    The columns of the DataFrame will be ['time', 'geometry'] + [bands]

    Args:
        data (RasterCube): _description_
        geometries (VectorCube): Multipolygon GeoJson

    Returns:
        VectorCube: GeopandasDataFrame

    """
    logger.debug("Converting dataarray to GeoDataFrame")

    # pdf = dataarray.to_pandas()
    df = dataset.to_dataframe().reset_index()
    # print(f"xarray2pandas: {pdf}")
    print(f"dataset2df: {df}")

    crs = geometries.crs.to_string()
    geometry = [geometries.iloc[0][0]] * len(df)
    gpdf = gpd.GeoDataFrame(df, crs=crs, geometry=geometry)

    gpdf = gpdf.sort_values(by=dims).reset_index(drop=True)

    # gpdf.columns.name = 'statistic'

    return gpdf


def _check_geometries_within_data_boundaries(
    clip_area: gpd.GeoDataFrame, data: RasterCube
) -> bool:
    """validate whether the area that will be clipped is within the total area.

    Args:
        clip_area (gpd.GeoDataFrame): gpd.GeoDataFrame
        data_array (xr.DataArray): xr.DataArray

    Raises:
        ValueError:

    Returns:
        bool: True if clip_area is within data_array boundaries. Otherwise, raise an Exception
    """
    y_dim = data.openeo.y_dim
    x_dim = data.openeo.x_dim
    max_x = data.coords[x_dim].max()
    min_x = data.coords[x_dim].min()
    max_y = data.coords[y_dim].max()
    min_y = data.coords[y_dim].min()
    boundaries = Polygon(
        [[min_x, min_y], [max_x, min_y], [max_x, max_y], [min_x, max_y], [min_x, min_y]]
    )
    for aoi in clip_area.geometry:
        # aoi = Polygon(geom["coordinates"][0])
        if not aoi.within(boundaries):
            raise ValueError(f"Error! {aoi.wkt} is not within {boundaries.wkt}")

    return True


def geojson_dict_to_geodataframe(geometries: Dict[str, Any]) -> gpd.GeoDataFrame:
    """
    Convert a python dictionary that is nominally 'geojson' to Geodataframe

    Accepts a couple different variations on geojson as the python client/backend can munge
    the Dict a little before invoking us

    If the dict has 'features' collect and convert all features
    If a single type, use the coordinates values

    ARGS:
        geometries: Dict of geojson

    RETURNS:
        gpd.GeoDataFrame

    Full Geojson
    '{"type": "FeatureCollection", "features": [{"id": "0", "type": "Feature", "properties": {}, "geometry": {"type": "Polygon", "coordinates": [[[-121.5, 44.0], [-121.5, 44.025], [-121.475, 44.025], [-121.475, 44.0], [-121.5, 44.0]]]}, "bbox": [-121.5, 44.0, -121.475, 44.025]}], "bbox": [-121.5, 44.0, -121.475, 44.025]}'
    What is to aggregate_spatial() geometries is generally just features

    geom_dict["features"][0]["geometry"]

    geom_dict = { "type": "Polygon","coordinates": [ [[-121.5, 44.0], [-121.5, 44.025], [-121.475, 44.025],
                [-121.475, 44.0], [-121.5, 44.0]], ],}
    """

    gpdf: gpd.GeoDataFrame = None

    if "features" in geometries:
        shape_list = [shape(i.get("geometry")) for i in geometries["features"]]
        gpdf = gpd.GeoDataFrame(geometry=shape_list)
    elif "type" in geometries:
        poly = shape(geometries)
        gpdf = gpd.GeoDataFrame(geometry=poly)

    assert (
        gpdf is not None
    ), f"Cannot convert geometry to gpdf: geometries: {geometries}"

    return gpdf


def aggregate_temporal(
    data: RasterCube,
    intervals: Union[TemporalIntervals, list[TemporalInterval], list[Optional[str]]],
    reducer: Callable,
    labels: Optional[list] = None,
    dimension: Optional[str] = None,
    context: Optional[dict] = None,
    **kwargs,
) -> RasterCube:
    # this is an example of usage of the openeo custom accessor
    # https://docs.xarray.dev/en/stable/internals/extending-xarray.html#writing-custom-accessors
    temporal_dims = data.openeo.temporal_dims

    if dimension is not None:
        if dimension not in data.dims:
            raise DimensionNotAvailable(
                f"A dimension with the specified name: {dimension} does not exist."
            )
        applicable_temporal_dimension = dimension
    else:
        if not temporal_dims:
            raise DimensionNotAvailable(
                f"No temporal dimension detected on dataset. Available dimensions: {data.dims}"
            )
        if len(temporal_dims) > 1:
            raise TooManyDimensions(
                f"The data cube contains multiple temporal dimensions: {temporal_dims}. The parameter `dimension` must be specified."
            )
        applicable_temporal_dimension = temporal_dims[0]
    bins = _create_bins(intervals=intervals)
    grouped_data = data.groupby_bins(
        group=applicable_temporal_dimension, labels=labels, bins=bins
    )
    if isinstance(reducer, str):
        if reducer.lower() in ["mean", "average", "avg"]:
            aggregated_data = grouped_data.mean(
                skipna=True, dim=applicable_temporal_dimension
            )
        elif reducer.lower() in ["max", "maximum"]:
            aggregated_data = grouped_data.max(
                skipna=True, dim=applicable_temporal_dimension
            )
        elif reducer.lower() in ["min", "minimum"]:
            aggregated_data = grouped_data.min(
                skipna=True, dim=applicable_temporal_dimension
            )
        elif reducer.lower() in ["median"]:
            aggregated_data = grouped_data.median(
                skipna=True, dim=applicable_temporal_dimension
            )
        else:
            raise NotImplementedError(f"Error! {reducer} not supported")
    else:
        raise NotImplementedError(f"Error! {reducer} not supported")

    return aggregated_data


def _create_bins(intervals: List[TemporalInterval]) -> List[List[np.datetime64]]:
    """create bins (which are represented by two numpy.datetime objects) given a list of
    time ranges (TemporalInterval)

    Args:
        intervals (List[TemporalInterval]): list of time ranges


    Returns:
        List[List[np.datetime64]]: bins
    """
    numpy_intervals = list()
    for interval in intervals:
        s = interval.start.to_numpy()
        numpy_intervals.append(s)
        e = interval.end.to_numpy()
        numpy_intervals.append(e)
    return sorted(list(set(numpy_intervals)))


def resample_cube_spatial(
    data: RasterCube, target: RasterCube, method="near", options=None
) -> RasterCube:
    methods_list = [
        "near",
        "bilinear",
        "cubic",
        "cubicspline",
        "lanczos",
        "average",
        "mode",
        "max",
        "min",
        "med",
        "q1",
        "q3",
    ]

    # ODC reproject requires y to be before x
    required_dim_order = (
        data.openeo.band_dims
        + data.openeo.temporal_dims
        + tuple(data.openeo.y_dim)
        + tuple(data.openeo.x_dim)
    )

    data_reordered = data.transpose(*required_dim_order, missing_dims="ignore")
    target_reordered = target.transpose(*required_dim_order, missing_dims="ignore")

    if method == "near":
        method = "nearest"

    elif method not in methods_list:
        raise Exception(
            f'Selected resampling method "{method}" is not available! Please select one of '
            f"[{', '.join(methods_list)}]"
        )

    resampled_data = _reproject_cube_match(
        data_cube=data_reordered,
        match_data_array=target_reordered,
        resampling=Resampling[method],
    )

    # Order axes back to how they were before
    resampled_data = resampled_data.transpose(*data.dims)

    # Ensure that attrs except crs are copied over
    for k, v in data.attrs.items():
        if k.lower() != "crs":
            resampled_data.attrs[k] = v
    return resampled_data


def merge_cubes(
    cube1: RasterCube,
    cube2: RasterCube,
    overlap_resolver: Callable = None,
    context: Optional[dict] = None,
) -> RasterCube:
    if context is None:
        context = {}
    if not isinstance(cube1, type(cube2)):
        raise Exception(
            f"Provided cubes have incompatible types. cube1: {type(cube1)}, cube2: {type(cube2)}"
        )
    x_dim = cube1.openeo.x_dim
    y_dim = cube1.openeo.y_dim
    bands_dim = cube1.openeo.band_dims[0]
    # Key: dimension name
    # Value: (labels in cube1 not in cube2, labels in cube2 not in cube1)
    overlap_per_shared_dim = {
        dim: Overlap(
            only_in_cube1=np.setdiff1d(cube1[dim].data, cube2[dim].data),
            only_in_cube2=np.setdiff1d(cube2[dim].data, cube1[dim].data),
            in_both=np.intersect1d(cube1[dim].data, cube2[dim].data),
        )
        for dim in set(cube1.dims).intersection(set(cube2.dims))
    }

    # Check if x and y require resample_cube_spatial
    coords_label_diff = any(
        [
            len(overlap.only_in_cube1) != 0 or len(overlap.only_in_cube2) != 0
            for overlap in [
                overlap_per_shared_dim[x_dim],
                overlap_per_shared_dim[y_dim],
            ]
        ]
    )
    if coords_label_diff:
        # resample cube2 based on coordinates form cube1
        cube2 = resample_cube_spatial(cube2, cube1)
        # recompute dimension overlap after resample
        overlap_per_shared_dim = {
            dim: Overlap(
                only_in_cube1=np.setdiff1d(cube1[dim].data, cube2[dim].data),
                only_in_cube2=np.setdiff1d(cube2[dim].data, cube1[dim].data),
                in_both=np.intersect1d(cube1[dim].data, cube2[dim].data),
            )
            for dim in set(cube1.dims).intersection(set(cube2.dims))
        }

    differing_dims = set(cube1.dims).symmetric_difference(set(cube2.dims))

    if len(differing_dims) == 0:
        # Check whether all of the shared dims have exactly the same labels
        dims_have_no_label_diff = all(
            [
                len(overlap.only_in_cube1) == 0 and len(overlap.only_in_cube2) == 0
                for overlap in overlap_per_shared_dim.values()
            ]
        )
        if dims_have_no_label_diff:
            # Example 3: All dimensions and their labels are equal
            concat_both_cubes = xr.concat([cube1, cube2], dim=NEW_DIM_NAME).reindex(
                {NEW_DIM_NAME: NEW_DIM_COORDS}
            )

            # Need to rechunk here to ensure that the cube dimension isn't chunked and the chunks for the other dimensions are not too large.
            concat_both_cubes_rechunked = concat_both_cubes.chunk(
                {NEW_DIM_NAME: -1}
                | {dim: "auto" for dim in cube1.dims if dim != NEW_DIM_NAME}
            )
            if overlap_resolver is None:
                # Example 3.1: Concat along new "cubes" dimension
                merged_cube = concat_both_cubes_rechunked
            else:
                # Example 3.2: Elementwise operation
                positional_parameters: Dict = {}
                named_parameters = {
                    x_dim: cube1.data,
                    y_dim: cube2.data,
                    "context": context,
                }

                merged_cube = concat_both_cubes_rechunked.reduce(
                    overlap_resolver,
                    dim=NEW_DIM_NAME,
                    keep_attrs=True,
                    positional_parameters=positional_parameters,
                    named_parameters=named_parameters,
                )
        else:
            # Example 1 & 2
            dims_requiring_resolve = [
                dim
                for dim, overlap in overlap_per_shared_dim.items()
                if len(overlap.in_both) > 0
                and (len(overlap.only_in_cube1) > 0 or len(overlap.only_in_cube2) > 0)
            ]

            if len(dims_requiring_resolve) == 0:
                # Example 1: No overlap on any dimensions, can just combine by coords

                # We need to convert to dataset before calling `combine_by_coords` in order to avoid the bug raised in https://github.com/Open-EO/openeo-processes-dask/issues/102
                # This messes with the order of dimensions and the band dimension, so we need to reorder this correctly afterwards.
                previous_dim_order = list(cube1.dims) + [
                    dim for dim in cube2.dims if dim not in cube1.dims
                ]

                if len(cube1.openeo.band_dims) > 0 or len(cube2.openeo.band_dims) > 0:
                    # Same reordering issue mentioned above
                    previous_band_order = list(
                        cube1[cube1.openeo.band_dims[0]].values
                    ) + [
                        band
                        for band in list(cube2[cube2.openeo.band_dims[0]].values)
                        if band not in list(cube1[cube1.openeo.band_dims[0]].values)
                    ]
                    cube1 = cube1.to_dataset(cube1.openeo.band_dims[0])
                    cube2 = cube2.to_dataset(cube2.openeo.band_dims[0])

                # compat="override" to deal with potentially conflicting coords
                # see https://github.com/Open-EO/openeo-processes-dask/pull/148 for context
                merged_cube = xr.combine_by_coords(
                    [cube1, cube2], combine_attrs="drop_conflicts"
                )
                # merged_cube = xr.concat([cube1, cube2], dim=BANDS, compat="override", combine_attrs="drop_conflicts", coords="minimal")
                if isinstance(merged_cube, xr.Dataset):
                    merged_cube = merged_cube.to_array(dim=bands_dim)
                    merged_cube = merged_cube.reindex({bands_dim: previous_band_order})

                merged_cube = merged_cube.transpose(*previous_dim_order)

            elif len(dims_requiring_resolve) == 1:
                # Example 2: Overlap on one dimension, resolve these pixels with overlap resolver
                # and combine the rest by coords

                if overlap_resolver is None or not callable(overlap_resolver):
                    raise OverlapResolverMissing(
                        "Overlapping data cubes, but no overlap resolver has been specified."
                    )

                overlapping_dim = dims_requiring_resolve[0]

                stacked_conflicts = xr.concat(
                    [
                        cube1.sel(
                            **{
                                overlapping_dim: overlap_per_shared_dim[
                                    overlapping_dim
                                ].in_both
                            }
                        ),
                        cube2.sel(
                            **{
                                overlapping_dim: overlap_per_shared_dim[
                                    overlapping_dim
                                ].in_both
                            }
                        ),
                    ],
                    dim=NEW_DIM_NAME,
                ).reindex({NEW_DIM_NAME: NEW_DIM_COORDS})

                # Need to rechunk here to ensure that the cube dimension isn't chunked and the chunks for the other dimensions are not too large.
                stacked_conflicts_rechunked = stacked_conflicts.chunk(
                    {NEW_DIM_NAME: -1}
                    | {dim: "auto" for dim in cube1.dims if dim != NEW_DIM_NAME}
                )

                conflicts_cube_1 = cube1.sel(
                    **{overlapping_dim: overlap_per_shared_dim[overlapping_dim].in_both}
                )

                conflicts_cube_2 = cube2.sel(
                    **{overlapping_dim: overlap_per_shared_dim[overlapping_dim].in_both}
                )

                positional_parameters = {}
                named_parameters = {
                    x_dim: conflicts_cube_1.data,
                    y_dim: conflicts_cube_2.data,
                    "context": context,
                }

                merge_conflicts = stacked_conflicts_rechunked.reduce(
                    overlap_resolver,
                    dim=NEW_DIM_NAME,
                    keep_attrs=True,
                    positional_parameters=positional_parameters,
                    named_parameters=named_parameters,
                )

                rest_of_cube_1 = cube1.sel(
                    **{
                        overlapping_dim: overlap_per_shared_dim[
                            overlapping_dim
                        ].only_in_cube1
                    }
                )
                rest_of_cube_2 = cube2.sel(
                    **{
                        overlapping_dim: overlap_per_shared_dim[
                            overlapping_dim
                        ].only_in_cube2
                    }
                )
                merged_cube = xr.combine_by_coords(
                    [merge_conflicts, rest_of_cube_1, rest_of_cube_2],
                    combine_attrs="drop_conflicts",
                )

            else:
                raise ValueError(
                    "More than one overlapping dimension, merge not possible."
                )

    elif len(differing_dims) <= 2:
        if overlap_resolver is None or not callable(overlap_resolver):
            raise OverlapResolverMissing(
                "Overlapping data cubes, but no overlap resolver has been specified."
            )

        # Example 4: broadcast lower dimension cube to higher-dimension cube
        if len(cube1.dims) < len(cube2.dims):
            lower_dim_cube = cube1
            higher_dim_cube = cube2
            is_cube1_lower_dim = True

        else:
            lower_dim_cube = cube2
            higher_dim_cube = cube1
            is_cube1_lower_dim = False

        lower_dim_cube_broadcast = lower_dim_cube.broadcast_like(higher_dim_cube)

        # Stack both cubes and use overlap resolver to resolve each pixel
        both_stacked = xr.concat(
            [higher_dim_cube, lower_dim_cube_broadcast], dim=NEW_DIM_NAME
        ).reindex({NEW_DIM_NAME: NEW_DIM_COORDS})

        # Need to rechunk here to ensure that the cube dimension isn't chunked and the chunks for the other dimensions are not too large.
        both_stacked_rechunked = both_stacked.chunk(
            {NEW_DIM_NAME: -1}
            | {dim: "auto" for dim in cube1.dims if dim != NEW_DIM_NAME}
        )

        positional_parameters = {}

        named_parameters = {"context": context}
        if is_cube1_lower_dim:
            named_parameters["x"] = lower_dim_cube_broadcast.data
            named_parameters["y"] = higher_dim_cube.data
        else:
            named_parameters["x"] = higher_dim_cube.data
            named_parameters["y"] = lower_dim_cube_broadcast.data

        merged_cube = both_stacked_rechunked.reduce(
            overlap_resolver,
            dim=NEW_DIM_NAME,
            keep_attrs=True,
            positional_parameters=positional_parameters,
            named_parameters=named_parameters,
        )
    else:
        raise ValueError("Number of differing dimensions is >2, merge not possible.")

    return merged_cube


def _reproject_cube_match(
    data_cube: RasterCube,
    match_data_array: xr.DataArray,
    resampling: Resampling,
) -> xr.DataArray:
    # We collect all available dimensions
    band_dims = list(data_cube.openeo.band_dims)
    temporal_dims = list(data_cube.openeo.temporal_dims)
    non_spatial_dimension_names = band_dims + temporal_dims
    # non_spatial_dimension_names = [
    #     dim for dim in data_cube.dims if dim not in ["y", "x"]
    # ]
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
    y_dim = data_cube.openeo.y_dim
    x_dim = data_cube.openeo.x_dim
    data_cube_stacked_reprojected = data_cube_stacked.transpose(
        "__unified_non_spatial_dimension__", y_dim, x_dim
    ).rio.reproject_match(
        match_data_array=match_data_array,
        resampling=resampling,
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

    return data_cube_stacked_reprojected  # type: ignore[no-any-return]


def resample_spatial(
    data: RasterCube,
    projection: Optional[Union[str, int]] = None,
    resolution: Optional[int] = 0,
    method: str = "near",
    align: str = "upper-left",
) -> RasterCube:
    """Resamples the spatial dimensions (x,y) of the data cube to a specified resolution and/or
      warps the data cube to the target projection. At least resolution or projection must
      be specified

    Args:
        data (RasterCube): data array
        projection (Optional[Union[str, int]], optional): Warps the data cube to the
            target projection, specified as as EPSG code or WKT2 CRS string.
        resolution (int, optional): Resamples the data cube to the target resolution,
            which can be specified either as separate values for x and y or as a single value
            for both axes. Specified in the units of the target projection. Doesn't change the
            resolution by default (0). Defaults to 0.
        method (str, optional): Resampling method to use. Defaults to "near".
        align (str, optional): _description_. Defaults to "upper-left".

    Returns:
        RasterCube: data
    """
    assert isinstance(data, xr.DataArray), f"Error! Unexpected data type: {type(data)}"

    assert (
        resolution is not None or projection is not None
    ), "Error! projection and resolution cannot be both None"

    # if projection is a float, fix it by converting to int
    if projection is not None and isinstance(projection, (float, int)):
        projection = int(projection)

        target_crs = crs.CRS.from_epsg(projection)
    else:
        target_crs = crs.CRS.from_string(projection)
    assert isinstance(target_crs, crs.CRS)

    if method == "near":
        method = "nearest"

    # set resampling method
    try:
        resampling = [
            r[1]
            for r in inspect.getmembers(Resampling)
            if r[0] == method and inspect.isfunction(r) is False
        ].pop()
    except IndexError:
        msg = f"Error! Unable to find resampling method: {method}"
        logger.error(msg)
        raise ValueError(msg)

    # if source CRS is not equal to target CRS
    if data.rio.crs is not None and target_crs != data.rio.crs:
        # reproject
        if resolution == 0:
            resolution = None

        data = reproject_cube(
            data_cube=data,
            target_projection=target_crs,
            resolution=resolution,
            resampling=resampling,
        )
    elif (
        resolution is not None
        and isinstance(resolution, (float, int))
        and resolution > 0
    ):
        # based on https://corteva.github.io/rioxarray/html/examples/resampling.html
        # get bounding box
        min_x, min_y, max_x, max_y = data.rio.bounds()
        # compute new x and y coords
        new_width = max(round((max_x - min_x) / resolution), 1)
        new_height = max(round((max_y - min_y) / resolution), 1)
        data = reproject_cube(
            data_cube=data,
            target_projection=target_crs,
            resolution=None,
            resampling=resampling,
            shape=(new_height, new_width),
        )
    else:
        # if target CRS is equal to source CRS and resolution is zero, do nothing
        pass
    return data


def run_udf(
    data: RasterCube, udf: str, runtime: str, version: Optional[str] = None
) -> UdfData:
    """run an user-defined function

    Args:
        data (RasterCube): raster cube
        udf (str): user-defined function
        runtime (str): e.g., python
        version (Optional[str], optional): _description_. Defaults to None.

    Returns:
        UdfData: _description_
    """
    logger.debug(f"processes::run_udf {udf=} {data=} {runtime=}")
    if isinstance(data, Array):
        data.compute()
        data = xr.DataArray(
            data,
            dims=(
                DEFAULT_TIME_DIMENSION,
                DEFAULT_BANDS_DIMENSION,
                DEFAULT_Y_DIMENSION,
                DEFAULT_X_DIMENSION,
            ),
        )
    assert isinstance(data, xr.DataArray), f"Error! Unexpected data type: {type(data)}"
    udf_data = UdfData(datacube_list=[XarrayDataCube(data)])
    return run_udf_code(code=udf, data=udf_data)


def aggregate_temporal_period(
    data: RasterCube,
    reducer: Callable,
    period: str,
    dimension: Optional[str] = None,
) -> RasterCube:
    temporal_dims = data.openeo.temporal_dims

    if dimension is not None:
        if dimension not in data.dims:
            raise DimensionNotAvailable(
                f"A dimension with the specified name: {dimension} does not exist."
            )
        applicable_temporal_dimension = dimension
    else:
        if not temporal_dims:
            raise DimensionNotAvailable(
                f"No temporal dimension detected on dataset. Available dimensions: {data.dims}"
            )
        if len(temporal_dims) > 1:
            raise TooManyDimensions(
                f"The data cube contains multiple temporal dimensions: {temporal_dims}. The parameter `dimension` must be specified."
            )
        applicable_temporal_dimension = temporal_dims[0]

    periods_to_frequency = {
        "hour": "H",
        "day": "D",
        "week": "W",
        "month": "M",
        "season": "QS-DEC",
        "year": "AS",
    }

    if period in periods_to_frequency.keys():
        frequency = periods_to_frequency[period]
    else:
        raise NotImplementedError(
            f"The provided period '{period})' is not implemented yet. The available ones are {list(periods_to_frequency.keys())}."
        )

    resampled_data = data.resample({applicable_temporal_dimension: frequency})

    positional_parameters = {"data": 0}
    resampled_data = resampled_data.reduce(
        reducer, keep_attrs=True, positional_parameters=positional_parameters
    )

    resampled_data = resampled_data.dropna(dim=applicable_temporal_dimension, how="all")

    return resampled_data


def mean(data, ignore_nodata=True, axis=None, keepdims=False):
    return openeo_processes_dask_mean(
        data=data, ignore_nodata=ignore_nodata, axis=axis, keepdims=keepdims
    )
