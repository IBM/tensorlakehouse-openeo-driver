from openeo_pg_parser_networkx.pg_schema import ParameterReference
from functools import partial
from typing import Any, Dict, List, Optional, Union
from rasterio.crs import CRS
import pandas as pd
import pytest
import xarray as xr
from openeo_pg_parser_networkx.pg_schema import BoundingBox
import numpy as np
from tensorlakehouse_openeo_driver.process_implementations.load_collection import (
    LoadCollectionFromHBase,
)

from tensorlakehouse_openeo_driver.processes import (
    resample_spatial,
    aggregate_temporal_period,
    merge_cubes as geodn_merge_cubes,
)
from tensorlakehouse_openeo_driver.constants import (
    DEFAULT_BANDS_DIMENSION,
    GEODN_DISCOVERY_CRS,
    DEFAULT_TIME_DIMENSION,
    DEFAULT_X_DIMENSION,
    DEFAULT_Y_DIMENSION,
    logger,
)
from tensorlakehouse_openeo_driver.processes import (
    CRS_EPSG_4326,
    _get_bounding_box,
    load_collection,
)
from tensorlakehouse_openeo_driver.processing import GeoDNProcessing
from tensorlakehouse_openeo_driver.tests.unit.unit_test_util import (
    validate_raster_datacube,
    MockTemporalInterval,
)

COLLECTION_ID_HISTORICAL_CROP_PLANTING_MAP = "Historical crop planting map (USA)"
BAND_CROP_30M = "111"

COLLECTION_ID_ERA5_ZARR = "Global weather (ERA5) (ZARR)"
BAND_49459 = "49459"

COLLECTION_ID_ERA5 = "Global weather (ERA5)"

BAND_TOTAL_PRECIPITATION = "Total precipitation"

COLLECTION_ID_CROPSCAPE = "Historical crop planting map (USA)"
BAND_CROPSCAPE = "111"

COLLECTION_ID_TWC_SEASONAL_WEATHER_FORECAST = "TWC Seasonal Weather Forecast"
BAND_TWC_MIN_TEMP = "Minimum temperature"

COLLECTION_ID_SENTINEL_2_LAND_USE = "sentinel2-10m-lulc"
BAND_SENTINEL_2_LAND_USE_LULC = "lulc"


test_load_collection_input = [
    (
        COLLECTION_ID_ERA5,
        BoundingBox(
            west=-123.0,
            east=-122.0,
            north=48,
            south=47,
            crs="epsg:4326",
        ),
        MockTemporalInterval(pd.Timestamp(2021, 1, 1), pd.Timestamp(2021, 2, 1)),
        [BAND_TOTAL_PRECIPITATION],
        {
            DEFAULT_X_DIMENSION: 32,
            DEFAULT_Y_DIMENSION: 32,
            DEFAULT_BANDS_DIMENSION: 1,
            "time": 745,
        },
        {},
        CRS_EPSG_4326,
    ),
    (
        COLLECTION_ID_HISTORICAL_CROP_PLANTING_MAP,
        BoundingBox(
            west=-123.0,
            east=-122.0,
            north=48,
            south=47,
            crs="epsg:4326",
        ),
        MockTemporalInterval(pd.Timestamp(2019, 12, 31), pd.Timestamp(2021, 1, 2)),
        [BAND_CROP_30M],
        {
            "lon": 3907,
            "lat": 3907,
            DEFAULT_BANDS_DIMENSION: 1,
            DEFAULT_TIME_DIMENSION: 2,
        },
        {},
        CRS_EPSG_4326,
    ),
    (
        COLLECTION_ID_HISTORICAL_CROP_PLANTING_MAP,
        BoundingBox(
            west=-123.0,
            east=-122.0,
            north=48,
            south=47,
            crs="epsg:4326",
        ),
        MockTemporalInterval(pd.Timestamp(2021, 1, 1), pd.Timestamp(2021, 1, 1)),
        [BAND_CROP_30M],
        {
            DEFAULT_X_DIMENSION: 3907,
            DEFAULT_Y_DIMENSION: 3907,
            DEFAULT_BANDS_DIMENSION: 1,
            DEFAULT_TIME_DIMENSION: 1,
        },
        {},
        CRS_EPSG_4326,
    ),
    (
        COLLECTION_ID_SENTINEL_2_LAND_USE,
        BoundingBox(south=32.25, west=-114.75, north=32.75, east=-114.25),
        MockTemporalInterval(pd.Timestamp(2021, 1, 1), pd.Timestamp(2022, 1, 1)),
        [BAND_SENTINEL_2_LAND_USE_LULC],
        {
            DEFAULT_X_DIMENSION: 7813,
            DEFAULT_Y_DIMENSION: 7813,
            DEFAULT_BANDS_DIMENSION: 1,
            "time": 1,
        },
        {},
        CRS_EPSG_4326,
    ),
    (
        COLLECTION_ID_SENTINEL_2_LAND_USE,
        BoundingBox(south=39.6279, west=-102.1014, north=39.9276, east=-101.5892),
        MockTemporalInterval(pd.Timestamp(2022, 1, 1), pd.Timestamp(2022, 1, 1)),
        [BAND_SENTINEL_2_LAND_USE_LULC],
        {
            DEFAULT_X_DIMENSION: 8004,
            DEFAULT_Y_DIMENSION: 4684,
            DEFAULT_BANDS_DIMENSION: 1,
            "time": 1,
        },
        {},
        CRS_EPSG_4326,
    ),
]


@pytest.mark.parametrize(
    "collection_id, spatial_extent, temporal_extent, bands, expected_dims, expected_attrs, reference_system",
    test_load_collection_input,
)
def test_load_collection(
    collection_id: str,
    spatial_extent: BoundingBox,
    temporal_extent: MockTemporalInterval,
    bands: Optional[List[str]],
    expected_dims: Dict[str, Dict[str, Union[int, str]]],
    expected_attrs: Dict[str, Any],
    reference_system: str,
):
    if collection_id == COLLECTION_ID_HISTORICAL_CROP_PLANTING_MAP:
        pytest.skip(f"Fix dimensions name: {collection_id}")
    else:
        data = load_collection(
            id=collection_id,
            spatial_extent=spatial_extent,
            temporal_extent=temporal_extent,
            bands=bands,
        )
        assert isinstance(data, xr.DataArray), f"Error! data is not a xr.DataArray: {type(data)}"

        validate_raster_datacube(
            cube=data,
            expected_dim_size=expected_dims,
            expected_attrs=expected_attrs,
            expected_crs=reference_system,
        )

        temporal_dims = [
            t for t in list(expected_dims.keys()) if t in ["time", DEFAULT_TIME_DIMENSION, "t"]
        ]
        assert len(temporal_dims) > 0
        # assumption that there is only one time dimension
        time_dim = temporal_dims[0]

        # check time dimension
        for time in data[time_dim].values:
            t = pd.Timestamp(time)
            start = temporal_extent.start
            end = temporal_extent.end
            assert start <= t <= end, f"Error! invalid: {start} <= {t} <= {end}"
        west, south, east, north = _get_bounding_box(spatial_extent=spatial_extent)

        # check spatial dimension - tolerance value in degrees when validating x and y
        tolerance = 3.0
        for x in data.x.values:
            assert (
                west - tolerance <= x <= east + tolerance
            ), f"Invalid coordinate: west <= x <= east: {west - tolerance} <= {x} <= {east + tolerance}"

        for y in data.y.values:
            assert (
                south - tolerance <= y <= north + tolerance
            ), f"Invalid coordinate: south <= y <= north: {south- tolerance} <= {y} <= {north + tolerance}"


test_load_collection_via_dataservice_input = [
    (
        # COLLECTION_ID_ERA5_ZARR,
        "Global weather (ERA5)",
        BoundingBox(west=-91.0, east=-90.0, south=41.0, north=42.0, crs="epsg:4326"),
        MockTemporalInterval(pd.Timestamp(2023, 6, 20), pd.Timestamp(2023, 6, 21)),
        ["Total precipitation"],
        {
            DEFAULT_X_DIMENSION: DEFAULT_X_DIMENSION,
            DEFAULT_Y_DIMENSION: DEFAULT_Y_DIMENSION,
            DEFAULT_TIME_DIMENSION: "time",
            DEFAULT_BANDS_DIMENSION: DEFAULT_BANDS_DIMENSION,
        },
        {
            DEFAULT_X_DIMENSION: 32,
            DEFAULT_Y_DIMENSION: 32,
            "time": 25,
            DEFAULT_BANDS_DIMENSION: 1,
        },
        {},
    ),
]


@pytest.mark.parametrize(
    "collection_id, spatial_extent, temporal_extent, bands, dimensions, expected_dims, expected_attrs",
    test_load_collection_via_dataservice_input,
)
def test_LoadCollectionFromHBase(
    collection_id: str,
    spatial_extent: BoundingBox,
    temporal_extent: MockTemporalInterval,
    bands: Optional[List[str]],
    dimensions: Dict[str, str],
    expected_dims: Dict,
    expected_attrs: Dict,
):
    loader = LoadCollectionFromHBase()
    data = loader.load_collection(
        id=collection_id,
        spatial_extent=spatial_extent,
        temporal_extent=temporal_extent,
        bands=bands,
        dimensions=dimensions,
    )
    validate_raster_datacube(
        cube=data,
        expected_dim_size=expected_dims,
        expected_attrs=expected_attrs,
        expected_crs=GEODN_DISCOVERY_CRS,
    )


"""
### Test binary math ops (e.g. subtract) with broadcast ###

# from tensorlakehouse_openeo_driver.processes import (
#     subtract as norm_subtract,
# )
"""


test_subtract_datacubes_parameters = [
    (
        "Global weather (ERA5)",
        BoundingBox(west=-91.0, east=-90.0, south=41.0, north=42.0, crs="epsg:4326"),
        MockTemporalInterval(pd.Timestamp(2007, 6, 20), pd.Timestamp(2007, 6, 20)),
        MockTemporalInterval(pd.Timestamp(2007, 6, 21), pd.Timestamp(2007, 6, 21)),
        ["Total precipitation"],
        ["reduce", "single"],
    ),
    (
        "Global weather (ERA5)",
        BoundingBox(west=-91.0, east=-90.0, south=41.0, north=42.0, crs="epsg:4326"),
        MockTemporalInterval(pd.Timestamp(2007, 6, 20), pd.Timestamp(2007, 6, 20)),
        MockTemporalInterval(pd.Timestamp(2007, 6, 21), pd.Timestamp(2007, 6, 22)),
        ["Total precipitation"],
        ["reduce", "multi"],
    ),
    (
        "Global weather (ERA5)",
        BoundingBox(west=-91.0, east=-90.0, south=41.0, north=42.0, crs="epsg:4326"),
        MockTemporalInterval(pd.Timestamp(2007, 6, 20), pd.Timestamp(2007, 6, 21)),
        MockTemporalInterval(pd.Timestamp(2007, 6, 22), pd.Timestamp(2007, 6, 23)),
        ["Total precipitation"],
        ["reduce", "multi"],
    ),
    # (
    # This will fail
    # "Global weather (ERA5)",
    # BoundingBox(west=-91.0, east=-90.0, south=41.0, north=42.0, crs="epsg:4326"),
    # MockTemporalInterval(pd.Timestamp(2023, 6, 20), pd.Timestamp(2023, 6, 21)),
    # MockTemporalInterval(pd.Timestamp(2023, 6, 22), pd.Timestamp(2023, 6, 23)),
    # ["Total precipitation"],
    # ["multi", "multi"],
    # ),
    # (
    #     "HLSS30",
    #     BoundingBox(west=-120.1, east=-120.0, south=34.0, north=34.1, crs="epsg:4326"),
    #     MockTemporalInterval(pd.Timestamp(2020, 9, 1), pd.Timestamp(2020, 9, 2)),
    #     MockTemporalInterval(pd.Timestamp(2020, 9, 1), pd.Timestamp(2020, 9, 2)),
    #     ["B02"],
    #     [],
    # ),
]


@pytest.mark.parametrize(
    "collection_id, spatial_extent, temporal_extent_cube1, temporal_extent_cube2, bands, hints",
    test_subtract_datacubes_parameters,
)
def test_subtract_cubes(
    collection_id: str,
    spatial_extent: BoundingBox,
    temporal_extent_cube1: MockTemporalInterval,
    temporal_extent_cube2: MockTemporalInterval,
    bands: List[str],
    hints: Optional[List[str]],
):
    """
    Test how subtraction of DataArrays is performed within OpenEO
    given dataarrays from actual data
    Expect it to conform or be same as xr.DataArray '-' operator.

    - cube1 time coordinate has a single timestamp or we reduce it to none if it has multiple timestamps
    - If the binary operation did not succeed the result xarray has empty time dimension
    e.g.<xarray.DataArray '49459' (band: 1, time: 0, y: 32, x: 32)>
    This is a problem, it means operation has failed

    """

    cube1 = load_collection(
        id=collection_id,
        spatial_extent=spatial_extent,
        temporal_extent=temporal_extent_cube1,
        bands=bands,
    )
    logger.debug(f"cube1:\n{cube1}")
    time_dim_name = "time"
    if time_dim_name in cube1.dims:
        if len(cube1.coords[time_dim_name].values) > 0:
            if hints != [] and hints[0] in ["reduce", "single"]:
                cube1 = cube1.reduce(np.min, dim=time_dim_name)
                # cube1 = cube1.max(dim = TIME)
                logger.debug(f"cube1 reduced:\n{cube1}")

    cube2 = load_collection(
        id=collection_id,
        spatial_extent=spatial_extent,
        temporal_extent=temporal_extent_cube2,
        bands=bands,
    )

    assert isinstance(cube1, xr.DataArray) and isinstance(
        cube2, xr.DataArray
    ), f"cube1: {type(cube1)}, cube2:{type(cube1)}"

    logger.debug(f"cube2:\n{cube2}")

    result = cube1 - cube2
    logger.debug(f"result:\n{result}")

    assert time_dim_name in result.dims, "Time absent in result dimensions"

    if time_dim_name in result.dims:
        assert len(result.coords[time_dim_name].values) > 0
        f"result reduced sample values: {result.isel({time_dim_name: 0, DEFAULT_X_DIMENSION: [0,1], DEFAULT_Y_DIMENSION: [0,1]}).values}"


"""
### Test merge_cubes() with geodn fix and original openeo version ####
"""


###############################################################################
# Parameterization of tests
# Note: case 3 with overlaping bands B02 is failing using the geodn fix due
# to missing resolver function
###############################################################################

# collection_id, spatial_extent, temporal_extent, bands_cube1, bands_cube2
test_merge_cube_parameters = [
    # (
    #     "ibm-eis-ga-1-esa-sentinel-2-l2a",
    #     BoundingBox(west=-73.5, east=-73.4, south=44.9, north=45.0, crs="epsg:4326"),
    #     MockTemporalInterval(pd.Timestamp(2020, 1, 1), pd.Timestamp(2020, 1, 2)),
    #     ["B02", "B05"],
    #     ["B07"],
    # ),
    (
        "HLSS30",
        BoundingBox(west=-122.0, east=-120.0, south=34.0, north=36.0, crs="epsg:4326"),
        MockTemporalInterval(pd.Timestamp(2020, 9, 1), pd.Timestamp(2020, 9, 2)),
        ["B02"],
        ["Fmask"],
    ),
    (
        "HLSS30",
        BoundingBox(west=-122.0, east=-120.0, south=34.0, north=36.0, crs="epsg:4326"),
        MockTemporalInterval(pd.Timestamp(2020, 9, 1), pd.Timestamp(2020, 9, 2)),
        ["B02", "B03", "B04"],
        ["Fmask"],
    ),
    (
        "HLSS30",
        BoundingBox(west=-122.0, east=-120.0, south=34.0, north=36.0, crs="epsg:4326"),
        MockTemporalInterval(pd.Timestamp(2020, 9, 1), pd.Timestamp(2020, 9, 2)),
        ["B02", "B03", "B04"],
        ["B02"],
    ),
]


@pytest.mark.parametrize(
    "collection_id, spatial_extent, temporal_extent, bands1, bands2",
    test_merge_cube_parameters,
)
def test_merge_cubes(
    collection_id: str,
    spatial_extent: BoundingBox,
    temporal_extent: MockTemporalInterval,
    bands1: List[str],
    bands2: List[str],
):
    """
    Test merge_cubes() with geodn fix and original openeo version
    Note: there is also a unit test for merge_cubes() in  tensorlakehouse_openeo_driver/tests/unit/test_processes.py

    Note: The test for the original openeo version or merge_cubes won't work until the name of
    the 'band' dimension returned in our load_collection is changed to 'bands' as
    that is expected and hard coded in openeo's merge_cube in merge.py. We could rename the dimension
    in the cubes in our test case I think from 'band' to 'bands' to get further.
    Invoking the openeo_merge_cubes the error is
    supplied_dims = ('time', 'band', 'y', 'x'), dims = ('bands', 'time', 'y', 'x'), missing_dims = 'raise'

    Note: invoke from terminal with e.g. $ pytest -rpf -s -k merge_cube
    """

    intersect = set(bands1).intersection(set(bands2))
    if len(intersect) >= 1:
        pytest.skip("Skip when the intersection is not empty")
    else:
        # load data cube 1
        cube1 = load_collection(
            id=collection_id,
            spatial_extent=spatial_extent,
            temporal_extent=temporal_extent,
            bands=bands1,
        )
        assert isinstance(cube1, xr.DataArray), f"cube1 not a xr.DataArray: {type(cube1)}"
        # check time dimension

        logger.debug(f"Cube1:\n{cube1}")

        # load data cube 2
        cube2 = load_collection(
            id=collection_id,
            spatial_extent=spatial_extent,
            temporal_extent=temporal_extent,
            bands=bands2,
        )
        assert isinstance(cube2, xr.DataArray), f"cube2 not a xr.DataArray: {type(cube2)}"

        cube1_bands = set(cube1[DEFAULT_BANDS_DIMENSION].values)
        cube2_bands = set(cube2[DEFAULT_BANDS_DIMENSION].values)
        isect_bands = cube1_bands.intersection(cube2_bands)
        union_bands = cube1_bands.union(cube2_bands)

        assert (
            isect_bands == set()
        ), f"Input cubes share >= 1 common bands: {isect_bands}, requires an overlap_resolver"

        result = geodn_merge_cubes(cube1, cube2)

        result_bands = {x for x in result[DEFAULT_BANDS_DIMENSION].values}
        assert (
            result_bands == union_bands
        ), f"Result bands: {result_bands} not union of input bands: {union_bands}"


@pytest.mark.parametrize(
    "collection_id, spatial_extent, temporal_extent, bands, projection, resolution",
    [
        (
            "HLSS30",
            [-121.9021462, 35.1330063, -120.6687854, 36.1397407],
            [pd.Timestamp(2020, 9, 1), pd.Timestamp(2020, 9, 2)],
            ["B02"],
            4326,
            0,
        ),
        (
            "HLSS30",
            [-121.9021462, 35.1330063, -120.6687854, 36.1397407],
            [pd.Timestamp(2020, 9, 1), pd.Timestamp(2020, 9, 2)],
            ["B02"],
            4326,
            30,
        ),
        (
            "HLSS30",
            [-121.9021462, 35.1330063, -120.6687854, 36.1397407],
            [pd.Timestamp(2020, 9, 1), pd.Timestamp(2020, 9, 2)],
            ["B02"],
            32617,
            30,
        ),
    ],
)
def test_resample_spatial(
    collection_id: str,
    spatial_extent,
    temporal_extent,
    bands: List[str],
    projection: int,
    resolution,
):
    bbox = BoundingBox(
        west=spatial_extent[0],
        east=spatial_extent[2],
        south=spatial_extent[1],
        north=spatial_extent[3],
        crs="4326",
    )
    temp_interval = MockTemporalInterval(start=temporal_extent[0], end=temporal_extent[1])
    target_crs = CRS.from_epsg(projection)
    data = load_collection(
        id=collection_id,
        spatial_extent=bbox,
        temporal_extent=temp_interval,
        bands=bands,
    )
    resampled_data = resample_spatial(data=data, projection=projection, resolution=resolution)
    assert resampled_data.rio.crs is not None and resampled_data.rio.crs == target_crs


# @pytest.mark.parametrize(
#     "collection_id, spatial_extent, temporal_extent, bands, period, reducer, expected_dims",
#     [
#         (
#             "HLSL30",
#             [-117.0, 33.9, -116.9, 34.0],
#             [pd.Timestamp(2020, 7, 19), pd.Timestamp(2020, 7, 29)],
#             ["B02", "B03"],
#             "day",
#             mean,
#             [1, 2, 371, 309],
#         ),
#         (
#             "HLSL30",
#             [-117.0, 33.9, -116.9, 34.0],
#             [pd.Timestamp(2020, 9, 1), pd.Timestamp(2020, 9, 17)],
#             ["B02", "B03"],
#             "month",
#             mean,
#             [1, 2, 371, 309],
#         ),
#     ],
# )
# def test_aggregate_temporal_period(
#     collection_id: str,
#     spatial_extent,
#     temporal_extent,
#     bands: List[str],
#     period: str,
#     reducer: Callable,
#     expected_dims: List[int],
# ):
#     bbox = BoundingBox(
#         west=spatial_extent[0],
#         east=spatial_extent[2],
#         south=spatial_extent[1],
#         north=spatial_extent[3],
#         crs=spatial_extent[4] if len(spatial_extent) == 5 else "4326",
#     )
#     temp_interval = MockTemporalInterval(
#         start=temporal_extent[0], end=temporal_extent[1]
#     )

#     data = load_collection(
#         id=collection_id,
#         spatial_extent=bbox,
#         temporal_extent=temp_interval,
#         bands=bands,
#     )

#     aggregated_data = aggregate_temporal_period(
#         data=data, period=period, reducer=reducer
#     )

#     assert list(aggregated_data.shape) == expected_dims

TEST_AGG_TEMPORAL_PERIOD = [
    (
        "Global weather (ERA5)",
        BoundingBox(west=-0.400, east=-0.39, south=53.79, north=53.80, crs="epsg:4326"),
        MockTemporalInterval(start=pd.Timestamp(2007, 1, 1), end=pd.Timestamp(2007, 1, 3)),
        ["Temperature"],
        "day",
    ),
    # (
    #     "CEH gridded hourly rainfall for Great Britain",
    #     BoundingBox(west=-3.6, south=51.0, east=-2.6, north=52.0),
    #     MockTemporalInterval(
    #         start=pd.Timestamp("2007-01-01T11:00:00Z"),
    #         end=pd.Timestamp("2007-01-03T11:00:00Z"),
    #     ),
    #     ["CEH rainfall for Great Britain"],
    #     "day",
    # ),
    # (
    #     "HLSL30",
    #     BoundingBox(west=-117.00, south=33.98, east=-116.98, north=34.00),
    #     MockTemporalInterval(
    #         start=pd.Timestamp("2020-09-01T00:00:00Z"),
    #         end=pd.Timestamp("2020-11-01T00:00:00Z"),
    #     ),
    #     ["B02"],
    #     "week",
    # ),
    (
        "HLSS30",
        BoundingBox(west=-117.0, south=33.9, east=-116.9, north=34.0),
        MockTemporalInterval(
            start=pd.Timestamp("2020-09-01T00:00:00Z"),
            end=pd.Timestamp("2020-11-01T00:00:00Z"),
        ),
        ["B02"],
        "week",
    ),
]


@pytest.mark.parametrize(
    "collection_id, spatial_extent, temporal_extent, bands, period",
    TEST_AGG_TEMPORAL_PERIOD,
)
def test_aggregate_temporal_period(collection_id, spatial_extent, temporal_extent, bands, period):
    # load datacube for test
    assert len(bands) == 1
    data = load_collection(
        id=collection_id,
        spatial_extent=spatial_extent,
        temporal_extent=temporal_extent,
        bands=bands,
    )
    # assumption: two consecutive timestamps don't have the same values
    temporal_dims = data.openeo.temporal_dims
    time_dim = temporal_dims[0]
    assert data[time_dim].size > 1

    # collect statistics of each timestamp
    stats_by_timestamp = list()
    size_temporal_dim = data[time_dim].size
    sample_size = min(size_temporal_dim, 10)
    for t_index in range(0, sample_size):
        slice_data = data.isel({time_dim: t_index, DEFAULT_BANDS_DIMENSION: 0})

        slice_data_max = float(slice_data.max())
        slice_data_min = float(slice_data.min())
        slice_data_mean = float(slice_data.mean())
        stats_by_timestamp.append(
            {"max": slice_data_max, "min": slice_data_min, "mean": slice_data_mean}
        )

    # compare stats between consecutive timestamps
    i = 1
    found = False
    while i < len(stats_by_timestamp) and not found:
        prev = stats_by_timestamp[i - 1]
        cur = stats_by_timestamp[i]
        i += 1
        # assumption: if max, min, and mean are equal between two timestamps,
        # then both timestamps are equal
        if prev["max"] != cur["max"] or prev["min"] != cur["min"] or prev["mean"] != cur["mean"]:
            found = True

    # create reducer object
    proc = GeoDNProcessing()
    reducer = partial(
        proc.process_registry["mean"].implementation,
        data=ParameterReference(from_parameter="data"),
    )
    # call process under test
    agg_data = aggregate_temporal_period(data=data, reducer=reducer, period=period)
    assert isinstance(agg_data, xr.DataArray)

    assert agg_data[DEFAULT_BANDS_DIMENSION].size == 1
    assert agg_data[time_dim].size > 1

    # collect stats of aggregated data
    size_agg_temporal_dim = agg_data[time_dim].size
    agg_sample_size = min(10, size_agg_temporal_dim)
    stats_agg_data = list()
    for t_index in range(0, agg_sample_size):
        slice_data = agg_data.isel({time_dim: t_index, DEFAULT_BANDS_DIMENSION: 0})
        max_cur_array = float(slice_data.max())
        min_cur_array = float(slice_data.min())
        mean_cur_array = float(slice_data.mean())
        stats_agg_data.append({"max": max_cur_array, "min": min_cur_array, "mean": mean_cur_array})
    # compare if two consecutive timestamps have different stats for aggregate data as well
    for i in range(1, len(stats_agg_data)):
        prev = stats_agg_data[i - 1]
        cur = stats_agg_data[i]
        if prev["max"] != 0 and cur["max"] != 0:
            assert (
                prev["max"] != cur["max"]
                or prev["min"] != cur["min"]
                or prev["mean"] != cur["mean"]
            )
