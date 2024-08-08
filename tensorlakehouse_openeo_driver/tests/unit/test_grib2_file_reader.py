from pathlib import Path
from typing import Dict, List, Tuple
import pytest
import xarray as xr

from tensorlakehouse_openeo_driver.file_reader.grib2_file_reader import Grib2FileReader
from datetime import datetime
from rasterio.crs import CRS


@pytest.mark.parametrize(
    "items, spatial_extent, temporal_extent, bands, crs, expected_dim_size",
    [
        (
            [
                {
                    "assets": {
                        "data": {
                            "href": Path(
                                "./tensorlakehouse_openeo_driver/tests/unit_test_data/example.grib"
                            )
                        }
                    },
                    "properties": {
                        "cube:dimensions": {
                            "latitude": {
                                "axis": "y",
                                "step": 90,
                                "type": "spatial",
                                "extent": [-90, 90],
                                "reference_system": 4326,
                            },
                            "longitude": {
                                "axis": "x",
                                "step": 90,
                                "type": "spatial",
                                "extent": [0, 270],
                                "reference_system": 4326,
                            },
                            "time": {
                                "type": "temporal",
                                "extent": [
                                    "2017-01-01T00:00:00Z",
                                    "2017-01-02T00:00:00Z",
                                ],
                            },
                        }
                    },
                },
            ],
            (0, 0, 180, 90),
            (datetime(2016, 12, 31), datetime(2017, 1, 2)),
            ["z", "t"],
            4326,
            {
                "time": 3,
                "longitude": 3,
                "latitude": 2,
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

    reader = Grib2FileReader(
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
