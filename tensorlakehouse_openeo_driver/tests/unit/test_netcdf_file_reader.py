from pathlib import Path
from typing import Dict, List, Tuple
import pytest
import xarray as xr
from tensorlakehouse_openeo_driver.constants import (
    DEFAULT_TIME_DIMENSION,
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

from tensorlakehouse_openeo_driver.util import object_storage_util
import os


class FakeS3Filesystem:

    def open(self, href, mode):
        return href


def mock_open_dataset(filename_or_obj: str) -> xr.Dataset:
    path = Path(filename_or_obj)
    ds = xr.open_dataset(path)
    return ds


@pytest.mark.parametrize(
    "items, spatial_extent, temporal_extent, bands, crs, expected_dim_size",
    [
        (
            [
                {
                    "assets": {
                        "data": {
                            "href": Path(
                                "./tensorlakehouse_openeo_driver/tests/test_data/filename_2000_2001.nc"
                            )
                        }
                    },
                    "properties": {
                        "cube:dimensions": {
                            "lat": {
                                "axis": "y",
                                "step": 0.017471758104738153,
                                "type": "spatial",
                                "extent": [46.991275, 61.003625],
                                "reference_system": 4326,
                            },
                            "lon": {
                                "axis": "x",
                                "step": 0.039572477064220186,
                                "type": "spatial",
                                "extent": [-15.01975, 6.54725],
                                "reference_system": 4326,
                            },
                            "time": {
                                "type": "temporal",
                                "extent": [
                                    "1999-12-01T12:00:00Z",
                                    "2000-11-30T12:00:00Z",
                                ],
                            },
                        }
                    },
                },
                {
                    "assets": {
                        "data": {
                            "href": Path(
                                "./tensorlakehouse_openeo_driver/tests/test_data/filename_2001_2002.nc"
                            )
                        }
                    },
                    "properties": {
                        "cube:dimensions": {
                            "lat": {
                                "axis": "y",
                                "step": 0.017471758104738153,
                                "type": "spatial",
                                "extent": [46.991275, 61.003625],
                                "reference_system": 4326,
                            },
                            "lon": {
                                "axis": "x",
                                "step": 0.039572477064220186,
                                "type": "spatial",
                                "extent": [-15.01975, 6.54725],
                                "reference_system": 4326,
                            },
                            "time": {
                                "type": "temporal",
                                "extent": [
                                    "1999-12-01T12:00:00Z",
                                    "2000-11-30T12:00:00Z",
                                ],
                            },
                        }
                    },
                },
            ],
            (-1.0, 51, 0.0, 52),
            (datetime(2000, 1, 1), datetime(2001, 1, 1)),
            ["tasmax"],
            4326,
            {
                DEFAULT_TIME_DIMENSION: 732,
                "lon": 100,
                "lat": 100,
            },
        ),
    ],
)
def test_load_items(
    items: List[Dict],
    spatial_extent: Tuple[float, float, float, float],
    temporal_extent: Tuple[datetime, datetime],
    bands: List[str],
    crs: str,
    expected_dim_size: Dict[str, int],
):
    os.environ["TLH_MYBUCKET_ACCESS_KEY_ID"] = "my-access-key"
    os.environ["TLH_MYBUCKET_SECRET_ACCESS_KEY"] = "my-secret-key"
    os.environ["TLH_MYBUCKET_ENDPOINT"] = (
        "s3.us-south.cloud-object-storage.appdomain.cloud"
    )

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

                    reader = NetCDFFileReader(
                        items=items,
                        bbox=spatial_extent,
                        temporal_extent=temporal_extent,
                        bands=bands,
                        dimension_map=None,
                    )
                    array = reader.load_items()
                    assert isinstance(array, xr.DataArray)
                    for dim, expected_size in expected_dim_size.items():
                        actual_size = array[dim].size
                        assert (
                            actual_size == expected_size
                        ), f"Error! {dim=} {actual_size=} {expected_size=}"
                    assert array.rio.crs == CRS.from_epsg(crs)
