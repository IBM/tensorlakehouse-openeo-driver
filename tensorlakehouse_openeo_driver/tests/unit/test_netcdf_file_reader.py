from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import pytest
import xarray as xr
from tensorlakehouse_openeo_driver.constants import (
    DEFAULT_BANDS_DIMENSION,
    DEFAULT_TIME_DIMENSION,
    TEST_WORKING_DIR
)
from tensorlakehouse_openeo_driver.file_reader.cloud_storage_file_reader import (
    CloudStorageFileReader,
)
from tensorlakehouse_openeo_driver.file_reader.netcdf_file_reader import (
    NetCDFFileReader,
)
from datetime import datetime
from unittest.mock import patch
from rasterio.crs import CRS
from openeo_pg_parser_networkx.pg_schema import ParameterReference
from tensorlakehouse_openeo_driver.stac.stac_utils import make_pystac_item
from tensorlakehouse_openeo_driver.util import object_storage_util
import os

FILENAME_2000_2001 = TEST_WORKING_DIR / "unit_test_data" / "filename_2000_2001.nc"
FILENAME_2001_2002 = TEST_WORKING_DIR / "unit_test_data" / "filename_2001_2002.nc"
NO_TIME_DIM_DATA = TEST_WORKING_DIR / "unit_test_data" / "no_time_dim_data_.nc"

ITEM_CUBE_DIM_LEVEL = {
    "bbox": [-18, -9, 17, 8],
    "assets": {"data": {"href": NO_TIME_DIM_DATA}},
    "properties": {
        "datetime": "2000-11-30T00:00:00Z",
        "cube:dimensions": {
            "longitude": {
                "axis": "x",
                "step": 0.039572477064220186,
                "type": "spatial",
                "extent": [-18, 17],
                "reference_system": 4326,
            },
            "latitude": {
                "axis": "y",
                "step": 0.017471758104738153,
                "type": "spatial",
                "extent": [-9, 8],
                "reference_system": 4326,
            },
            "level": {
                "type": "spatial",
                "axis": "z",
                "values": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            },
            "time": {
                "type": "temporal",
                "extent": [
                    "2000-11-30T00:00:00Z",
                    "2000-11-30T00:00:00Z",
                ],
            },
        },
    },
}

# MOCK_ERA5_ITEM = {
#     "stac_version": "1.0.0",
#     "stac_extensions": [
#         "https://stac-extensions.github.io/scientific/v1.0.0/schema.json",
#         "https://stac-extensions.github.io/datacube/v2.2.0/schema.json",
#     ],
#     "type": "Feature",
#     "id": "era5_global_jan2024_t2m_conv_latlon",
#     "collection": "era5-reanalysis-global-conv-latlon",
#     "geometry": {
#         "type": "Polygon",
#         "coordinates": [
#             [
#                 [-180.0, -90.0],
#                 [179.75, -90.0],
#                 [179.75, 90.0],
#                 [-180.0, 90.0],
#                 [-180.0, -90.0],
#             ]
#         ],
#     },
#     "bbox": [-180.0, -90.0, 179.75, 90.0],
#     "properties": {
#         "datetime": None,
#         "start_datetime": "2024-01-01T00:00:00+00:00",
#         "end_datetime": "2024-01-31T23:00:00+00:00",
#         "cube:dimensions": {
#             "valid_time": {
#                 "type": "temporal",
#                 "extent": ["2020-01-01T00:00:00+00:00", "2020-01-31T23:00:00+00:00"],
#             },
#             "longitude": {
#                 "type": "spatial",
#                 "axis": "x",
#                 "extent": [0.0, 360.0],
#                 "reference_system": 4326,
#                 "step": 0.01,
#             },
#             "latitude": {
#                 "type": "spatial",
#                 "axis": "y",
#                 "extent": [-90.0, 90.0],
#                 "reference_system": 4326,
#                 "step": 0.01,
#             },
#         },
#         "cube:variables": {
#             "t2m": {
#                 "dimensions": ["valid_time", "latitude", "longitude"],
#                 "type": "float32",
#                 "description": "2 metre temperature",
#                 "unit": "K",
#             }
#         },
#         "gsd": 0.25,
#     },
#     "assets": {
#         "data": {
#             "href": "/Users/ltizzei/Downloads/era5_global_jan2024_t2m.nc",
#             "type": "application/netcdf",
#             "title": "ERA5 Reanalysis Data",
#             "description": "NetCDF file containing ERA5 reanalysis data",
#             "roles": ["data"],
#         }
#     },
#     "links": [
#         {
#             "rel": "collection",
#             "href": "./era5-reanalysis-global-conv-latlon/collection.json",
#             "type": "application/json",
#         }
#     ],
# }


class FakeS3Filesystem:

    def open(self, href, mode):
        return href


@pytest.mark.parametrize(
    "items, spatial_extent, temporal_extent, properties, bands, crs, expected_dim_size",
    [
        (
            [
                {
                    "bbox": [-1, 51, 0, 52],
                    "assets": {
                        "data": {"href": FILENAME_2000_2001}
                    },
                    "properties": {
                        "start_datetime": "2000-01-01T00:00:00Z",
                        "end_datetime": "2001-01-01T00:00:00Z",
                        "cube:dimensions": {
                            "y": {
                                "axis": "y",
                                "step": 0.017471758104738153,
                                "type": "spatial",
                                "extent": [46.991275, 61.003625],
                                "reference_system": 4326,
                            },
                            "x": {
                                "axis": "x",
                                "step": 0.039572477064220186,
                                "type": "spatial",
                                "extent": [-15.01975, 6.54725],
                                "reference_system": 4326,
                            },
                            "t": {
                                "type": "temporal",
                                "extent": [
                                    "2000-01-01T00:00:00Z",
                                    "2001-01-01T00:00:00Z",
                                ],
                            },
                        },
                    },
                },
                {
                    "bbox": [-1, 51, 0, 52],
                    "assets": {
                        "data": {"href": FILENAME_2001_2002}
                    },
                    "properties": {
                        "start_datetime": "2000-01-01T00:00:00Z",
                        "end_datetime": "2001-01-01T00:00:00Z",
                        "cube:dimensions": {
                            "y": {
                                "axis": "y",
                                "step": 0.017471758104738153,
                                "type": "spatial",
                                "extent": [46.991275, 61.003625],
                                "reference_system": 4326,
                            },
                            "x": {
                                "axis": "x",
                                "step": 0.039572477064220186,
                                "type": "spatial",
                                "extent": [-15.01975, 6.54725],
                                "reference_system": 4326,
                            },
                            "t": {
                                "type": "temporal",
                                "extent": [
                                    "2000-01-01T00:00:00Z",
                                    "2001-01-01T12:00:00Z",
                                ],
                            },
                        },
                    },
                },
            ],
            (-1.0, 51, 0.0, 52),
            (datetime(2000, 11, 30), datetime(2000, 11, 30)),
            None,
            ["tasmax"],
            4326,
            {
                DEFAULT_TIME_DIMENSION: 1,
                "x": 100,
                "y": 100,
            },
        ),
        (
            [ITEM_CUBE_DIM_LEVEL],
            (-15.0, -1.0, -13.0, 2.0),
            (datetime(2000, 11, 30), datetime(2000, 11, 30)),
            None,
            ["temperature"],
            4326,
            {
                "time": 1,
                "longitude": 3,
                "latitude": 4,
                DEFAULT_BANDS_DIMENSION: 1,
                "level": 10,
            },
        ),
        (
            [ITEM_CUBE_DIM_LEVEL],
            (-15.0, -1.0, -13.0, 2.0),
            (datetime(2000, 11, 30), datetime(2000, 11, 30)),
            {
                "cube:dimensions.level.values": {
                    "process_graph": {
                        "eq1": {
                            "process_id": "eq",
                            "arguments": {
                                "x": ParameterReference(from_parameter="value"),
                                "y": 7,
                            },
                            "result": True,
                        }
                    }
                },
            },
            ["temperature"],
            4326,
            {
                "time": 1,
                "longitude": 3,
                "latitude": 4,
                DEFAULT_BANDS_DIMENSION: 1,
                "level": 1,
            },
        ),
        # (
        #     [MOCK_ERA5_ITEM],
        #     (-0.9, 51.2, -0.1, 51.9),
        #     (datetime(2024, 1, 1), datetime(2024, 1, 3)),
        #     None,
        #     ["t2m"],
        #     4326,
        #     {
        #         "valid_time": 49,
        #         "longitude": 4,
        #         "latitude": 4,
        #         DEFAULT_BANDS_DIMENSION: 1,
        #     },
        # ),
    ],
)
def test_load_items(
    items: List[Dict],
    spatial_extent: Tuple[float, float, float, float],
    temporal_extent: Tuple[datetime, datetime],
    properties: Optional[Dict[str, Any]],
    bands: List[str],
    crs: str,
    expected_dim_size: Dict[str, int],
):
    os.environ["TLH_MYBUCKET_ACCESS_KEY_ID"] = "my-access-key"
    os.environ["TLH_MYBUCKET_SECRET_ACCESS_KEY"] = "my-secret-key"
    os.environ["TLH_MYBUCKET_ENDPOINT"] = (
        "s3.us-south.cloud-object-storage.appdomain.cloud"
    )
    for item in items:
        asset = item["assets"]["data"]
        href = asset["href"]
        path = Path(href)
        # path = TEST_WORKING_DIR / href
        assert path.exists(), f"Error! {path} file does not exist"

    with patch.object(
        NetCDFFileReader, "create_s3filesystem", return_value=FakeS3Filesystem()
    ):

        with patch.object(
            object_storage_util,
            "get_credentials_by_bucket",
            return_value={"access_key_id": "", "secret_access_key": "", "endpoint": ""},
        ):
            with patch.object(
                object_storage_util,
                "parse_region",
                return_value="us-east",
            ):
                with patch.object(
                    CloudStorageFileReader,
                    "_extract_bucket_name_from_url",
                    return_value="fake-bucket-name",
                ):
                    pystac_items = list()
                    for item in items:
                        pystac_items.append(make_pystac_item(item))
                    reader = NetCDFFileReader(
                        items=pystac_items,
                        bbox=spatial_extent,
                        temporal_extent=temporal_extent,
                        bands=bands,
                        properties=properties,
                    )
                    array = reader.load_items()
                    assert isinstance(array, xr.DataArray)
                    for dim, expected_size in expected_dim_size.items():
                        actual_size = array[dim].size
                        assert (
                            actual_size == expected_size
                        ), f"Error! {dim=} {actual_size=} {expected_size=}"
                    assert array.rio.crs == CRS.from_epsg(crs)
