from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import pytest
import xarray as xr

from tensorlakehouse_openeo_driver.constants import (
    DEFAULT_BANDS_DIMENSION,
    TEST_DATA_ROOT,
)
from tensorlakehouse_openeo_driver.file_reader.grib2_file_reader import Grib2FileReader
from datetime import datetime
from rasterio.crs import CRS
from openeo_pg_parser_networkx.pg_schema import ParameterReference


@pytest.mark.parametrize(
    "items, spatial_extent, temporal_extent, properties, bands, crs, expected_dim_size",
    [
        # (
        #     [
        #         {
        #             "type": "Feature",
        #             "id": "gfs_4_20190101_0000_372.sfc.grb2",
        #             "links": [],
        #             "properties": {
        #                 "datetime": "2019-01-16T12:00:00Z",
        #                 "cube:dimensions": {
        #                     "depthBelowLandLayer": {
        #                         "type": "custom",
        #                         "values": [0.0, 0.4, 0.1, 1.0],
        #                     },
        #                     "longitude": {
        #                         "type": "spatial",
        #                         "axis": "x",
        #                         "extent": [0.0, 359.5],
        #                         "step": 0.5,
        #                         "reference_system": 4326,
        #                         "unit": "degrees_east",
        #                     },
        #                     "latitude": {
        #                         "type": "spatial",
        #                         "axis": "y",
        #                         "extent": [-90.0, 90.0],
        #                         "step": -0.5,
        #                         "reference_system": 4326,
        #                         "unit": "degrees_north",
        #                     },
        #                     "time": {
        #                         "type": "temporal",
        #                         "step": "P0DT0H0M0S",
        #                         "extent": [
        #                             "2019-01-16T12:00:00Z",
        #                             "2019-01-16T12:00:00Z",
        #                         ],
        #                     },
        #                     "heightAboveGround": {
        #                         "type": "custom",
        #                         "values": [80.0, 2.0, 10.0, 100.0],
        #                     },
        #                     "heightAboveSea": {
        #                         "type": "custom",
        #                         "values": [3658.0, 1829.0, 2743.0],
        #                     },
        #                 },
        #                 "cube:variables": {
        #                     "st": {
        #                         "type": "data",
        #                         "dimensions": [
        #                             "depthBelowLandLayer",
        #                             "longitude",
        #                             "latitude",
        #                         ],
        #                     },
        #                     "soilw": {
        #                         "type": "data",
        #                         "dimensions": [
        #                             "depthBelowLandLayer",
        #                             "longitude",
        #                             "latitude",
        #                         ],
        #                     },
        #                     "pres": {
        #                         "type": "data",
        #                         "dimensions": ["longitude", "latitude"],
        #                     },
        #                     "u": {
        #                         "type": "data",
        #                         "dimensions": [
        #                             "heightAboveSea",
        #                             "longitude",
        #                             "latitude",
        #                         ],
        #                     },
        #                     "v": {
        #                         "type": "data",
        #                         "dimensions": [
        #                             "heightAboveSea",
        #                             "longitude",
        #                             "latitude",
        #                         ],
        #                     },
        #                     "q": {
        #                         "type": "data",
        #                         "dimensions": ["longitude", "latitude"],
        #                     },
        #                     "t": {
        #                         "type": "data",
        #                         "dimensions": [
        #                             "heightAboveSea",
        #                             "heightAboveGround",
        #                             "longitude",
        #                             "latitude",
        #                         ],
        #                     },
        #                     "u10": {
        #                         "type": "data",
        #                         "dimensions": ["longitude", "latitude"],
        #                     },
        #                     "v10": {
        #                         "type": "data",
        #                         "dimensions": ["longitude", "latitude"],
        #                     },
        #                     "t2m": {
        #                         "type": "data",
        #                         "dimensions": ["longitude", "latitude"],
        #                     },
        #                     "d2m": {
        #                         "type": "data",
        #                         "dimensions": ["longitude", "latitude"],
        #                     },
        #                     "tmax": {
        #                         "type": "data",
        #                         "dimensions": ["longitude", "latitude"],
        #                     },
        #                     "tmin": {
        #                         "type": "data",
        #                         "dimensions": ["longitude", "latitude"],
        #                     },
        #                     "sh2": {
        #                         "type": "data",
        #                         "dimensions": ["longitude", "latitude"],
        #                     },
        #                     "r2": {
        #                         "type": "data",
        #                         "dimensions": ["longitude", "latitude"],
        #                     },
        #                     "aptmp": {
        #                         "type": "data",
        #                         "dimensions": ["longitude", "latitude"],
        #                     },
        #                     "u100": {
        #                         "type": "data",
        #                         "dimensions": ["longitude", "latitude"],
        #                     },
        #                     "v100": {
        #                         "type": "data",
        #                         "dimensions": ["longitude", "latitude"],
        #                     },
        #                     "ustm": {
        #                         "type": "data",
        #                         "dimensions": ["longitude", "latitude"],
        #                     },
        #                     "vstm": {
        #                         "type": "data",
        #                         "dimensions": ["longitude", "latitude"],
        #                     },
        #                     "hlcy": {
        #                         "type": "data",
        #                         "dimensions": ["longitude", "latitude"],
        #                     },
        #                     "prmsl": {
        #                         "type": "data",
        #                         "dimensions": ["longitude", "latitude"],
        #                     },
        #                     "mslet": {
        #                         "type": "data",
        #                         "dimensions": ["longitude", "latitude"],
        #                     },
        #                     "uswrf": {
        #                         "type": "data",
        #                         "dimensions": ["longitude", "latitude"],
        #                     },
        #                     "ulwrf": {
        #                         "type": "data",
        #                         "dimensions": ["longitude", "latitude"],
        #                     },
        #                     "unknown": {
        #                         "type": "data",
        #                         "dimensions": ["longitude", "latitude"],
        #                     },
        #                     "siconc": {
        #                         "type": "data",
        #                         "dimensions": ["longitude", "latitude"],
        #                     },
        #                     "cape": {
        #                         "type": "data",
        #                         "dimensions": ["longitude", "latitude"],
        #                     },
        #                     "sp": {
        #                         "type": "data",
        #                         "dimensions": ["longitude", "latitude"],
        #                     },
        #                     "lsm": {
        #                         "type": "data",
        #                         "dimensions": ["longitude", "latitude"],
        #                     },
        #                     "vis": {
        #                         "type": "data",
        #                         "dimensions": ["longitude", "latitude"],
        #                     },
        #                     "prate": {
        #                         "type": "data",
        #                         "dimensions": ["longitude", "latitude"],
        #                     },
        #                     "acpcp": {
        #                         "type": "data",
        #                         "dimensions": ["longitude", "latitude"],
        #                     },
        #                     "sde": {
        #                         "type": "data",
        #                         "dimensions": ["longitude", "latitude"],
        #                     },
        #                     "utaua": {
        #                         "type": "data",
        #                         "dimensions": ["longitude", "latitude"],
        #                     },
        #                     "vtaua": {
        #                         "type": "data",
        #                         "dimensions": ["longitude", "latitude"],
        #                     },
        #                     "cin": {
        #                         "type": "data",
        #                         "dimensions": ["longitude", "latitude"],
        #                     },
        #                     "orog": {
        #                         "type": "data",
        #                         "dimensions": ["longitude", "latitude"],
        #                     },
        #                     "tp": {
        #                         "type": "data",
        #                         "dimensions": ["longitude", "latitude"],
        #                     },
        #                     "msshf": {
        #                         "type": "data",
        #                         "dimensions": ["longitude", "latitude"],
        #                     },
        #                     "mslhf": {
        #                         "type": "data",
        #                         "dimensions": ["longitude", "latitude"],
        #                     },
        #                     "crain": {
        #                         "type": "data",
        #                         "dimensions": ["longitude", "latitude"],
        #                     },
        #                     "cfrzr": {
        #                         "type": "data",
        #                         "dimensions": ["longitude", "latitude"],
        #                     },
        #                     "cicep": {
        #                         "type": "data",
        #                         "dimensions": ["longitude", "latitude"],
        #                     },
        #                     "csnow": {
        #                         "type": "data",
        #                         "dimensions": ["longitude", "latitude"],
        #                     },
        #                     "cprat": {
        #                         "type": "data",
        #                         "dimensions": ["longitude", "latitude"],
        #                     },
        #                     "cpofp": {
        #                         "type": "data",
        #                         "dimensions": ["longitude", "latitude"],
        #                     },
        #                     "sdwe": {
        #                         "type": "data",
        #                         "dimensions": ["longitude", "latitude"],
        #                     },
        #                     "gust": {
        #                         "type": "data",
        #                         "dimensions": ["longitude", "latitude"],
        #                     },
        #                     "u-gwd": {
        #                         "type": "data",
        #                         "dimensions": ["longitude", "latitude"],
        #                     },
        #                     "v-gwd": {
        #                         "type": "data",
        #                         "dimensions": ["longitude", "latitude"],
        #                     },
        #                     "dswrf": {
        #                         "type": "data",
        #                         "dimensions": ["longitude", "latitude"],
        #                     },
        #                     "dlwrf": {
        #                         "type": "data",
        #                         "dimensions": ["longitude", "latitude"],
        #                     },
        #                     "lftx": {
        #                         "type": "data",
        #                         "dimensions": ["longitude", "latitude"],
        #                     },
        #                     "lftx4": {
        #                         "type": "data",
        #                         "dimensions": ["longitude", "latitude"],
        #                     },
        #                     "watr": {
        #                         "type": "data",
        #                         "dimensions": ["longitude", "latitude"],
        #                     },
        #                     "gflux": {
        #                         "type": "data",
        #                         "dimensions": ["longitude", "latitude"],
        #                     },
        #                     "hindex": {
        #                         "type": "data",
        #                         "dimensions": ["longitude", "latitude"],
        #                     },
        #                     "wilt": {
        #                         "type": "data",
        #                         "dimensions": ["longitude", "latitude"],
        #                     },
        #                     "landn": {
        #                         "type": "data",
        #                         "dimensions": ["longitude", "latitude"],
        #                     },
        #                     "fldcp": {
        #                         "type": "data",
        #                         "dimensions": ["longitude", "latitude"],
        #                     },
        #                     "al": {
        #                         "type": "data",
        #                         "dimensions": ["longitude", "latitude"],
        #                     },
        #                     "SUNSD": {
        #                         "type": "data",
        #                         "dimensions": ["longitude", "latitude"],
        #                     },
        #                     "gh": {
        #                         "type": "data",
        #                         "dimensions": ["longitude", "latitude"],
        #                     },
        #                     "icaht": {
        #                         "type": "data",
        #                         "dimensions": ["longitude", "latitude"],
        #                     },
        #                     "trpp": {
        #                         "type": "data",
        #                         "dimensions": ["longitude", "latitude"],
        #                     },
        #                     "vwsh": {
        #                         "type": "data",
        #                         "dimensions": ["longitude", "latitude"],
        #                     },
        #                 },
        #             },
        #             "assets": {
        #                 "data": {
        #                     "href": "/Users/ltizzei/Downloads/ECCC_demo_2024_sep/gfs_4_20190101_0000_372.sfc.grb2",
        #                     "type": "application/x-grib2",
        #                 }
        #             },
        #             "stac_version": "1.1.0-beta.1",
        #             "stac_extensions": [
        #                 "https://stac-extensions.github.io/datacube/v2.2.0/schema.json"
        #             ],
        #             "geometry": {
        #                 "type": "Polygon",
        #                 "coordinates": [
        #                     [
        #                         [179.5, 90.0],
        #                         [179.5, -90.0],
        #                         [-180.0, -90.0],
        #                         [-180.0, 90.0],
        #                         [179.5, 90.0],
        #                     ]
        #                 ],
        #             },
        #             "collection": "GFS",
        #             "bbox": [-180.0, -90.0, 179.5, 90.0],
        #         }
        #     ],
        #     (0, 0, 180, 90),
        #     (datetime(2019, 1, 16), datetime(2019, 1, 17)),
        #     {
        #         "cube:dimensions.heightAboveGround.values": {
        #             "process_graph": {
        #                 "eq1": {
        #                     "process_id": "eq",
        #                     "arguments": {
        #                         "x": ParameterReference(from_parameter="value"),
        #                         "y": 80,
        #                     },
        #                     "result": True,
        #                 }
        #             }
        #         },
        #     },
        #     ["t"],
        #     4326,
        #     {"bands": 1, "latitude": 181, "time": 1, "longitude": 360},
        # ),
        (
            [
                {
                    "assets": {
                        "data": {
                            "href": "./tensorlakehouse_openeo_driver/tests/unit_test_data/mock_extra_dim_2000_01_01.grib2"
                        }
                    },
                    "properties": {
                        "datetime": "2000-01-01T00:00:00Z",
                        "cube:dimensions": {
                            "latitude": {
                                "axis": "y",
                                "step": 1,
                                "type": "spatial",
                                "extent": [51, 52],
                                "reference_system": 4326,
                                "unit": "degrees",
                            },
                            "longitude": {
                                "axis": "x",
                                "step": 1,
                                "type": "spatial",
                                "extent": [-1, 0],
                                "reference_system": 4326,
                                "unit": "degrees_east",
                            },
                            "isobaricInhPa": {
                                "type": "spatial",
                                "extent": [1, 2, 3, 4, 5, 6, 7, 8, 9],
                            },
                            "time": {
                                "type": "temporal",
                                "extent": [
                                    "2000-01-01T00:00:00Z",
                                    "2000-01-01T00:00:00Z",
                                ],
                            },
                        },
                        "cube:variables": {
                            "t": {
                                "type": "data",
                                "unit": "",
                                "description": "2-meter air temperature",
                                "values": [],
                                "dimensions": ["x", "y", "time", "isobaricInhPa"],
                            }
                        },
                    },
                },
                {
                    "assets": {
                        "data": {
                            "href": "./tensorlakehouse_openeo_driver/tests/unit_test_data/mock_extra_dim_2000_01_02.grib2"
                        }
                    },
                    "properties": {
                        "datetime": "2000-01-02T00:00:00Z",
                        "cube:dimensions": {
                            "latitude": {
                                "axis": "y",
                                "step": 1,
                                "type": "spatial",
                                "extent": [51, 52],
                                "reference_system": 4326,
                                "unit": "degrees",
                            },
                            "longitude": {
                                "axis": "x",
                                "step": 1,
                                "type": "spatial",
                                "extent": [-1, 0],
                                "reference_system": 4326,
                                "unit": "degrees_east",
                            },
                            "isobaricInhPa": {
                                "type": "spatial",
                                "extent": [1, 2, 3, 4, 5, 6, 7, 8, 9],
                            },
                            "time": {
                                "type": "temporal",
                                "extent": [
                                    "2000-01-02T00:00:00Z",
                                    "2000-01-02T00:00:00Z",
                                ],
                            },
                        },
                        "cube:variables": {
                            "t": {
                                "type": "data",
                                "unit": "",
                                "description": "2-meter air temperature",
                                "values": [],
                                "dimensions": ["x", "y", "time", "isobaricInhPa"],
                            }
                        },
                    },
                },
            ],
            (-0.5, 51.2, -0.2, 51.9),
            (datetime(2000, 1, 1), datetime(2000, 1, 3)),
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
                "longitude": 31,
                "latitude": 70,
                DEFAULT_BANDS_DIMENSION: 1,
                "time": 2,
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
    assert dict(array.sizes) == expected_dim_size
    assert array.rio.crs == CRS.from_epsg(crs)
    ds = array.to_dataset(dim=DEFAULT_BANDS_DIMENSION)
    path = TEST_DATA_ROOT / "test_convert_grib2_to_netcdf.nc"
    if Path(path).exists():
        Path(path).unlink()
    ds.to_netcdf(path=path, engine="netcdf4")  # type: ignore[call-overload]
    if Path(path).exists():
        Path(path).unlink()
