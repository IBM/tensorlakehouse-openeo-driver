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
                                "unit": "degrees_east"
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
                            "href": "./tensorlakehouse_openeo_driver/tests/unit_test_data/test_extra_dim.grib2"
                        }
                    },
                    "properties": {
                        "cube:dimensions": {
                            "latitude": {
                                "axis": "y",
                                "step": 1,
                                "type": "spatial",
                                "extent": [51, 52],
                                "reference_system": 4326,
                                "unit": "degrees"
                            },
                            "longitude": {
                                "axis": "x",
                                "step": 1,
                                "type": "spatial",
                                "extent": [-1, 0],
                                "reference_system": 4326,
                                "unit": "degrees_east"
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
                                "extent": [1, 2, 3, 4, 5, 6, 7, 8, 9],
                            },
                        }
                    },
                },
            ],
            (-0.5, 51.2, -0.2, 51.9),
            (datetime(2019, 1, 1), datetime(2019, 1, 1)),
            {
                "cube:dimensions.isobaricInhPa.values": {
                    "process_graph": {
                        "eq1": {
                            "process_id": "eq",
                            "arguments": {
                                "x": ParameterReference(from_parameter="value"),
                                "y": 1,
                            },
                            "result": True,
                        }
                    }
                },
            },
            ["t"],
            4326,
            {
                "time": 1,
                "longitude": 31,
                "latitude": 70,
                "isobaricInhPa": 1,
                "bands": 1,
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
