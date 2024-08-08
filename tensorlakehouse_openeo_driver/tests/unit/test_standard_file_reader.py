from pathlib import Path
from typing import Dict, List, Tuple
import pytest
import xarray as xr

import fstd2nc  # noqa: F401
from tensorlakehouse_openeo_driver.file_reader.standard_file_reader import (
    FSTDFileReader,
)
from datetime import datetime
from rasterio.crs import CRS


@pytest.mark.skip("Missing example")
@pytest.mark.parametrize(
    "items, spatial_extent, temporal_extent, bands, crs, expected_dim_size",
    [
        (
            [
                {
                    "assets": {
                        "data": {
                            "href": Path(
                                "./tensorlakehouse_openeo_driver/tests/unit_test_data/example.fst"
                            )
                        }
                    },
                    "properties": {
                        "cube:dimensions": {
                            "rlat1": {
                                "axis": "y",
                                "step": 0.022500038,
                                "type": "spatial",
                                "extent": [-12.775001, 17.1725],
                                "reference_system": 4326,
                            },
                            "rlon1": {
                                "axis": "x",
                                "step": 0.039572477064220186,
                                "type": "spatial",
                                "extent": [-15.293716, 42.778793],
                                "reference_system": 4326,
                            },
                            "time": {
                                "type": "temporal",
                                "extent": [
                                    "2019-12-15T00:00:00Z",
                                    "2019-12-15T00:00:00Z",
                                ],
                            },
                        }
                    },
                },
            ],
            (-10.0, 0.0, 0.0, 5.0),
            (datetime(2019, 12, 14), datetime(2019, 12, 16)),
            ["ME"],
            4326,
            {
                "time": 1,
                "rlon1": 446,
                "rlat1": 223,
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

    reader = FSTDFileReader(
        items=items,
        bbox=spatial_extent,
        temporal_extent=temporal_extent,
        bands=bands,
        dimension_map=None,
    )
    array = reader.load_items()
    assert isinstance(array, xr.DataArray)
    for dim, expected_size in expected_dim_size.items():
        dimensions = list(array.dims)
        assert dim in dimensions, f"Error! {dim=} is not part of {dimensions=}"
        actual_size = array[dim].size
        assert (
            actual_size == expected_size
        ), f"Error! {dim=} {actual_size=} {expected_size=}"
    assert array.rio.crs == CRS.from_epsg(crs)
