from typing import Any, Dict, List, Optional, Tuple
import pytest
import xarray as xr

from tensorlakehouse_openeo_driver.file_reader.grib2_file_reader import Grib2FileReader
from datetime import datetime
from rasterio.crs import CRS
from openeo_pg_parser_networkx.pg_schema import ParameterReference


@pytest.mark.parametrize(
    "items, spatial_extent, temporal_extent, properties, bands, crs, expected_dim_size",
    [
        (
            [
                {
                    "assets": {
                        "data": {
                            "href": "./tensorlakehouse_openeo_driver/tests/unit_test_data/example.grib"
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
            None,
            ["z", "t"],
            4326,
            {
                "time": 3,
                "longitude": 2,
                "latitude": 2,
            },
        ),
        (
            [
                {
                    "assets": {
                        "data": {
                            "href": "/Users/ltizzei/Downloads/ECCC_dont_share/gfs_4_20190101_0000_039.pl.grb2"
                        }
                    },
                    "properties": {
                        "cube:dimensions": {
                            "latitude": {
                                "axis": "y",
                                "step": 1,
                                "type": "spatial",
                                "extent": [-90, 90],
                                "reference_system": 4326,
                            },
                            "longitude": {
                                "axis": "x",
                                "step": 1,
                                "type": "spatial",
                                "extent": [0, 360],
                                "reference_system": 4326,
                            },
                            "time": {
                                "type": "temporal",
                                "extent": [
                                    "2019-01-01T00:00:00Z",
                                    "2019-01-01T00:00:00Z",
                                ],
                            },
                            "isobaricInhPa": {
                                "type": "spatial",
                                "extent": [
                                    1000.0,
                                    975.0,
                                    950.0,
                                    925.0,
                                    900.0,
                                    850.0,
                                    800.0,
                                    750.0,
                                    700.0,
                                    650.0,
                                    600.0,
                                    550.0,
                                    500.0,
                                    450.0,
                                    400.0,
                                    350.0,
                                    300.0,
                                    250.0,
                                    200.0,
                                    150.0,
                                    100.0,
                                    70.0,
                                    50.0,
                                    30.0,
                                    20.0,
                                    10.0,
                                    7.0,
                                    5.0,
                                    3.0,
                                    2.0,
                                    1.0,
                                ],
                            },
                        }
                    },
                },
            ],
            (0, 0, 180, 90),
            (datetime(2019, 1, 1), datetime(2019, 1, 1)),
            {
                "cube:dimensions.isobaricInhPa.values": {
                    "process_graph": {
                        "eq1": {
                            "process_id": "eq",
                            "arguments": {
                                "x": ParameterReference(from_parameter="value"),
                                "y": 100,
                            },
                            "result": True,
                        }
                    }
                },
            },
            ["gh", "t"],
            4326,
            {
                "time": 1,
                "longitude": 360,
                "latitude": 181,
                "isobaricInhPa": 1,
                "bands": 2,
            },
        ),
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

    reader = Grib2FileReader(
        items=items,
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
