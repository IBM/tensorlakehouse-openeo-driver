import os
import pandas as pd
from typing import Dict, List, Tuple
import pytest
import xarray as xr
from tensorlakehouse_openeo_driver.constants import (
    DEFAULT_TIME_DIMENSION,
    DEFAULT_X_DIMENSION,
    DEFAULT_Y_DIMENSION,
)
from tensorlakehouse_openeo_driver.file_reader.zarr_file_reader import (
    ZarrFileReader,
)
from datetime import datetime
from unittest.mock import patch
from rasterio.crs import CRS

from tensorlakehouse_openeo_driver.geospatial_utils import reproject_bbox
from tensorlakehouse_openeo_driver.stac.stac_utils import make_pystac_item
from tensorlakehouse_openeo_driver.tests.unit.unit_test_util import (
    generate_xarray,
)
import numpy as np


class FakeS3Filesystem:

    def get_mapper(self, root="", check=False, create=False, missing_exceptions=None):
        pass


@pytest.mark.parametrize(
    "items, bbox, temporal_extent, bands, crs, expected_dim_size",
    [
        (
            [
                {
                    "bbox": [0.0, 50.0, 5.0, 55.0],
                    "assets": {
                        "data": {
                            "href": "https://s3.us-east.cloud-object-storage.appdomain.cloud/my-bucket/radar.zarr"
                        }
                    },
                    "properties": {
                        "start_datetime": "2000-01-01T00:00:00.000Z",
                        "end_datetime": "2020-01-31T00:00:00.000Z",
                        "cube:dimensions": {
                            DEFAULT_TIME_DIMENSION: {
                                "step": "P0DT0H10M0S",
                                "type": "temporal",
                                "extent": [
                                    "2000-01-01T00:00:00.000Z",
                                    "2020-01-31T00:00:00.000Z",
                                ],
                            },
                            DEFAULT_Y_DIMENSION: {
                                "axis": "y",
                                "step": 0.009000778,
                                "type": "spatial",
                                "extent": [50.0, 55.0],
                                "reference_system": 4326,
                            },
                            DEFAULT_X_DIMENSION: {
                                "axis": "x",
                                "step": 0.018447876,
                                "type": "spatial",
                                "extent": [0.0, 5.0],
                                "reference_system": 4326,
                            },
                        },
                    },
                }
            ],
            (0.5, 51, 1.5, 52),
            (datetime(2000, 1, 1), datetime(2001, 1, 1)),
            ["tasmax"],
            "EPSG:4326",
            {
                DEFAULT_TIME_DIMENSION: 98,
                DEFAULT_X_DIMENSION: 108,
                DEFAULT_Y_DIMENSION: 118,
            },
        )
    ],
)
def test_load_items(
    items: List[Dict],
    bbox: Tuple[float, float, float, float],
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
    lonmin, latmin, lonmax, latmax = bbox
    temporal_ext = (
        pd.Timestamp(temporal_extent[0] - pd.Timedelta(1, unit="D")),
        pd.Timestamp(temporal_extent[1] + pd.Timedelta(1, unit="D")),
    )
    # expected CRS is the one specified on STAC item
    dst_crs = items[0]["properties"]["cube:dimensions"][DEFAULT_X_DIMENSION][
        "reference_system"
    ]
    # reproject user-specified bbox to raw data CRS
    bbox = reproject_bbox(bbox=bbox, src_crs=4326, dst_crs=dst_crs)
    lonmin, latmin, lonmax, latmax = bbox
    # compute the step on the x dim and y dimensions
    x_coords = np.linspace(
        start=lonmin, stop=lonmax, num=expected_dim_size[DEFAULT_X_DIMENSION]
    )
    step_x = x_coords[1] - x_coords[0]
    y_coords = np.linspace(
        start=latmin, stop=latmax, num=expected_dim_size[DEFAULT_Y_DIMENSION]
    )
    step_y = y_coords[1] - y_coords[0]
    ds = generate_xarray(
        lonmin=lonmin - step_x,
        lonmax=lonmax + step_x,
        latmin=latmin - step_y,
        latmax=latmax + step_y,
        bands=bands,
        crs=crs,
        temporal_extent=temporal_ext,
        freq=None,
        num_periods=expected_dim_size[DEFAULT_TIME_DIMENSION] + 2,
        size_x=expected_dim_size[DEFAULT_X_DIMENSION] + 2,
        size_y=expected_dim_size[DEFAULT_Y_DIMENSION] + 2,
        is_dataset=True,
    )
    with patch.object(
        ZarrFileReader, "create_s3filesystem", return_value=FakeS3Filesystem()
    ):

        with patch.object(xr, "open_zarr", return_value=ds):
            pystac_items = list()
            for item in items:
                pystac_items.append(make_pystac_item(item))
            reader = ZarrFileReader(
                items=pystac_items,
                bbox=bbox,
                temporal_extent=temporal_extent,
                bands=bands,
                properties=None,
            )

            array = reader.load_items()
            assert isinstance(array, xr.DataArray)
            for dim, expected_size in expected_dim_size.items():
                actual_size = array[dim].size
                assert (
                    actual_size == expected_size
                ), f"Error! {dim=} {actual_size=} {expected_size=}"
            assert array.rio.crs == CRS.from_epsg(dst_crs)
