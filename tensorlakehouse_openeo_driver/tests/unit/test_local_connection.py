from typing import Dict, List, Optional, Tuple
from openeo.local import LocalConnection
from pathlib import Path
import pytest
import xarray as xr
from rasterio import crs
from tensorlakehouse_openeo_driver.constants import (
    DEFAULT_TIME_DIMENSION,
    DEFAULT_X_DIMENSION,
    DEFAULT_Y_DIMENSION,
)
from tensorlakehouse_openeo_driver.tests.unit.unit_test_util import (
    validate_raster_datacube,
)

# from tensorlakehouse_openeo_driver.local.connection import GeodnLocalConnection


def _extract_metadata(coll: Dict) -> Dict:
    cube_dimensions = coll["cube:dimensions"]
    temporal_dim_name = x_dim_name = y_dim_name = None
    for dim_name, dim_info in cube_dimensions.items():
        if dim_info.get("axis") is not None:
            if dim_info.get("axis") == DEFAULT_X_DIMENSION:
                x_dim_name = dim_name
                west = dim_info["extent"][0]
                east = dim_info["extent"][1]
            if dim_info.get("axis") == DEFAULT_Y_DIMENSION:
                y_dim_name = dim_name
                south = dim_info["extent"][0]
                north = dim_info["extent"][1]
        elif dim_info.get("type") == "temporal":
            temporal_dim_name = dim_name
    metadata = {
        DEFAULT_X_DIMENSION: x_dim_name,
        DEFAULT_Y_DIMENSION: y_dim_name,
        DEFAULT_TIME_DIMENSION: temporal_dim_name,
        "bbox": [west, south, east, north],
    }
    return metadata


@pytest.mark.parametrize(
    "path, spatial_extent, temporal_extent, expected_dim_size, expected_attrs, expected_crs, band_names",
    [
        (
            "/Users/ltizzei/Projects/Orgs/IBM/tensorlakehouse-openeo-driver/tensorlakehouse_openeo_driver/tests/test_data/test_openeo_Globalweather-ERA5-.nc",
            None,
            None,
            {"x": 32, "y": 32},
            {},
            4326,
            [],
        ),
        (
            "/Users/ltizzei/Projects/Orgs/IBM/tensorlakehouse-openeo-driver/tensorlakehouse_openeo_driver/tests/test_data/test_openeo_Globalweather-ERA5-.nc",
            {"west": -1.0, "east": 0.0, "south": 53.0, "north": 54.0},
            ["2007-01-14T00:00:00Z", "2007-01-28T00:00:00Z"],
            {"x": 32, "y": 32},
            {},
            4326,
            [],
        ),
        # "/Users/ltizzei/Projects/Orgs/IBM/tensorlakehouse-openeo-driver/tensorlakehouse_openeo_driver/tests/test_data/tasmax_rcp85_land-cpm_uk_2.2km/tasmax_rcp85_land-cpm_uk_2.2km_01_day_20701201-20711130_reproj.nc",
    ],
)
def test_local_connection(
    path: str,
    spatial_extent: Optional[Dict[str, float]],
    temporal_extent: Optional[Tuple[str, str]],
    expected_dim_size: Dict[str, int],
    expected_attrs: Dict[str, str],
    expected_crs: int,
    band_names: List[str],
):
    p = Path(path)
    assert p.exists(), f"Error! File does not exist {p}"

    local_conn = LocalConnection(p.parent)

    # list_collections = local_conn.list_collections()
    coll = local_conn.describe_collection(path)

    metadata = _extract_metadata(coll=coll)
    temporal_dim_name = metadata[DEFAULT_TIME_DIMENSION]
    # x_dim_name = metadata[DEFAULT_X_DIMENSION]
    # y_dim_name = metadata[DEFAULT_Y_DIMENSION]
    datacube = local_conn.load_collection(
        collection_id=path,
        spatial_extent=spatial_extent,
        temporal_extent=temporal_extent,
    )
    datacube = datacube.reduce_dimension(dimension=temporal_dim_name, reducer="mean")
    raster_cube: xr.DataArray = datacube.execute()
    assert isinstance(raster_cube, xr.DataArray)
    assert temporal_dim_name not in raster_cube.dims
    # ds = xr.open_dataset(path)
    validate_raster_datacube(
        cube=raster_cube,
        expected_dim_size=expected_dim_size,
        expected_attrs=expected_attrs,
        expected_crs=crs.CRS.from_epsg(expected_crs),
        band_names=band_names,
    )
    # assert agg_datacube[x_dim_name].size == ds[x_dim_name].size
    # assert agg_datacube[y_dim_name].size == ds[y_dim_name].size
