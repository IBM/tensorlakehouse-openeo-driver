import geopandas
import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union
from zipfile import ZipFile
import matplotlib.pyplot as plt

from cfgrib.xarray_to_grib import to_grib
import numpy as np
import pandas as pd
import rioxarray
import xarray as xr
from jsonschema import validate
from pystac import Collection, Extent, SpatialExtent, Summaries, TemporalExtent
from rasterio.crs import CRS

from tensorlakehouse_openeo_driver.constants import (
    DEFAULT_BANDS_DIMENSION,
    EPSG_4326,
    GEOTIFF_PREFIX,
    GTIFF,
    NETCDF,
    PARQUET,
    TEST_DATA_ROOT,
    DEFAULT_TIME_DIMENSION,
    ZIP,
    DEFAULT_X_DIMENSION,
    DEFAULT_Y_DIMENSION,
    logger,
)
from tensorlakehouse_openeo_driver.dataset import DatasetMetadata
from tensorlakehouse_openeo_driver.layer import LayerMetadata
from tensorlakehouse_openeo_driver.tests.utils import (
    validate_vector_datacube,
)

TEMPORAL_GUESSES = [
    "DATE",
    "time",
    "t",
    "year",
    "quarter",
    "month",
    "week",
    "day",
    "hour",
    "second",
]
X_GUESSES = ["x", "lon", "longitude"]
Y_GUESSES = ["y", "lat", "latitude"]
BANDS_GUESSES = ["b", "bands", "band"]
GRIB2 = "GRIB2"
HLSS30_ITEMS = (
    {
        "id": "HLS.S30.T18TYQ.2023242T153821.v2.0.Fmask",
        "bbox": [-72.4573686, 44.0997709, -71.1300132, 45.1255249],
        "type": "Feature",
        "assets": {
            "data": {
                "href": "s3://fm-geospatial-cos-standard-poc/lchu/HLSS30/vermont_manual/la+444844+447409lo-719616-716025/2023/08/HLS.S30.T18TYQ.2023242T153821.v2.0.Fmask.tif",
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "roles": ["data"],
                "title": "Fmask",
                "description": "",
            }
        },
        "geometry": {
            "type": "Polygon",
            "coordinates": [
                [
                    [-72.4573686, 45.1255249],
                    [-72.5001512, 44.1380077],
                    [-71.1300132, 44.0997709],
                    [-71.0639024, 45.085957],
                    [-72.4573686, 45.1255249],
                ]
            ],
        },
        "collection": "HLSS30",
        "properties": {
            "tile": "T18TYP",
            "created": "2023-09-19T13:00:11.245361Z",
            "updated": "2023-09-19T13:00:11.245361Z",
            "datetime": "2023-08-30T15:38:21Z",
            "cloud_coverage": 98,
            "cube:variables": {
                "Fmask": {
                    "type": "data",
                    "unit": "",
                    "values": [],
                    "dimensions": ["x", "y", "time"],
                }
            },
            "cube:dimensions": {
                "x": {
                    "axis": "x",
                    "step": 30,
                    "type": "spatial",
                    "extent": [699960, 809760],
                    "reference_system": 32618,
                },
                "y": {
                    "axis": "y",
                    "step": -30,
                    "type": "spatial",
                    "extent": [5000040, 4890240],
                    "reference_system": 32618,
                },
                "time": {
                    "type": "temporal",
                    "extent": ["2023-08-30T15:38:21Z", "2023-08-30T15:50:51Z"],
                },
            },
        },
        "stac_version": "1.0.0",
        "links": [
            {
                "rel": "collection",
                "type": "application/json",
                "href": "https://stac-fastapi-pgstac-nasageospatial-dev.cash.sl.cloud9.ibm.com/collections/HLSS30",
            },
            {
                "rel": "parent",
                "type": "application/json",
                "href": "https://stac-fastapi-pgstac-nasageospatial-dev.cash.sl.cloud9.ibm.com/collections/HLSS30",
            },
            {
                "rel": "root",
                "type": "application/json",
                "href": "https://stac-fastapi-pgstac-nasageospatial-dev.cash.sl.cloud9.ibm.com/",
            },
            {
                "rel": "self",
                "type": "application/geo+json",
                "href": "https://stac-fastapi-pgstac-nasageospatial-dev.cash.sl.cloud9.ibm.com/collections/HLSS30/items/HLS.S30.T18TYQ.2023242T153821.v2.0.Fmask",
            },
        ],
    },
    {
        "id": "HLS.S30.T18TYQ.2023242T153821.v2.0.B8A",
        "bbox": [-72.4573686, 44.0997709, -71.1300132, 45.1255249],
        "type": "Feature",
        "assets": {
            "data": {
                "href": "s3://fm-geospatial-cos-standard-poc/lchu/HLSS30/vermont_manual/la+444844+447409lo-719616-716025/2023/08/HLS.S30.T18TYQ.2023242T153821.v2.0.B8A.tif",
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "roles": ["data"],
                "title": "B8A",
                "description": "",
            }
        },
        "geometry": {
            "type": "Polygon",
            "coordinates": [
                [
                    [-72.4573686, 45.1255249],
                    [-72.5001512, 44.1380077],
                    [-71.1300132, 44.0997709],
                    [-71.0639024, 45.085957],
                    [-72.4573686, 45.1255249],
                ]
            ],
        },
        "collection": "HLSS30",
        "properties": {
            "tile": "T18TYQ",
            "created": "2023-09-19T13:00:10.916202Z",
            "updated": "2023-09-19T13:00:10.916202Z",
            "datetime": "2023-08-30T15:38:21Z",
            "cloud_coverage": 97,
            "cube:variables": {
                "B8A": {
                    "type": "data",
                    "unit": "",
                    "values": [],
                    "dimensions": ["x", "y", "time"],
                }
            },
            "cube:dimensions": {
                "x": {
                    "axis": "x",
                    "step": 30,
                    "type": "spatial",
                    "extent": [699960, 809760],
                    "reference_system": 32618,
                },
                "y": {
                    "axis": "y",
                    "step": -30,
                    "type": "spatial",
                    "extent": [5000040, 4890240],
                    "reference_system": 32618,
                },
                "time": {
                    "type": "temporal",
                    "extent": ["2023-08-30T15:38:21Z", "2023-08-30T15:50:51Z"],
                },
            },
        },
        "stac_version": "1.0.0",
        "links": [
            {
                "rel": "collection",
                "type": "application/json",
                "href": "https://stac-fastapi-pgstac-nasageospatial-dev.cash.sl.cloud9.ibm.com/collections/HLSS30",
            },
            {
                "rel": "parent",
                "type": "application/json",
                "href": "https://stac-fastapi-pgstac-nasageospatial-dev.cash.sl.cloud9.ibm.com/collections/HLSS30",
            },
            {
                "rel": "root",
                "type": "application/json",
                "href": "https://stac-fastapi-pgstac-nasageospatial-dev.cash.sl.cloud9.ibm.com/",
            },
            {
                "rel": "self",
                "type": "application/geo+json",
                "href": "https://stac-fastapi-pgstac-nasageospatial-dev.cash.sl.cloud9.ibm.com/collections/HLSS30/items/HLS.S30.T18TYQ.2023242T153821.v2.0.B8A",
            },
        ],
    },
    {
        "id": "HLS.S30.T18TYQ.2023242T153821.v2.0.B12",
        "bbox": [-72.4573686, 44.0997709, -71.1300132, 45.1255249],
        "type": "Feature",
        "assets": {
            "data": {
                "href": "s3://fm-geospatial-cos-standard-poc/lchu/HLSS30/vermont_manual/la+444844+447409lo-719616-716025/2023/08/HLS.S30.T18TYQ.2023242T153821.v2.0.B12.tif",
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "roles": ["data"],
                "title": "B12",
                "description": "",
            }
        },
        "geometry": {
            "type": "Polygon",
            "coordinates": [
                [
                    [-72.4573686, 45.1255249],
                    [-72.5001512, 44.1380077],
                    [-71.1300132, 44.0997709],
                    [-71.0639024, 45.085957],
                    [-72.4573686, 45.1255249],
                ]
            ],
        },
        "collection": "HLSS30",
        "properties": {
            "tile": "T18TYQ",
            "created": "2023-09-19T13:00:10.596792Z",
            "updated": "2023-09-19T13:00:10.596792Z",
            "datetime": "2023-08-30T15:38:21Z",
            "cloud_coverage": 99,
            "cube:variables": {
                "B12": {
                    "type": "data",
                    "unit": "",
                    "values": [],
                    "dimensions": ["x", "y", "time"],
                }
            },
            "cube:dimensions": {
                "x": {
                    "axis": "x",
                    "step": 30,
                    "type": "spatial",
                    "extent": [699960, 809760],
                    "reference_system": 32618,
                },
                "y": {
                    "axis": "y",
                    "step": -30,
                    "type": "spatial",
                    "extent": [5000040, 4890240],
                    "reference_system": 32618,
                },
                "time": {
                    "type": "temporal",
                    "extent": ["2023-08-30T15:38:21Z", "2023-08-30T15:50:51Z"],
                },
            },
        },
        "stac_version": "1.0.0",
        "links": [
            {
                "rel": "collection",
                "type": "application/json",
                "href": "https://stac-fastapi-pgstac-nasageospatial-dev.cash.sl.cloud9.ibm.com/collections/HLSS30",
            },
            {
                "rel": "parent",
                "type": "application/json",
                "href": "https://stac-fastapi-pgstac-nasageospatial-dev.cash.sl.cloud9.ibm.com/collections/HLSS30",
            },
            {
                "rel": "root",
                "type": "application/json",
                "href": "https://stac-fastapi-pgstac-nasageospatial-dev.cash.sl.cloud9.ibm.com/",
            },
            {
                "rel": "self",
                "type": "application/geo+json",
                "href": "https://stac-fastapi-pgstac-nasageospatial-dev.cash.sl.cloud9.ibm.com/collections/HLSS30/items/HLS.S30.T18TYQ.2023242T153821.v2.0.B12",
            },
        ],
    },
)


STAC_ITEMS = [
    {
        "stac_version": "1.0.0",
        "stac_extensions": [],
        "type": "Feature",
        "id": "0bba530824d148fcb4629123e4a1328f",
        "bbox": [-179.934464, -89.934464, 189.03321599996895, 111.261056],
        "geometry": {
            "type": "Polygon",
            "coordinates": [
                [
                    [-179.934464, -89.934464],
                    [-179.934464, 111.261056],
                    [189.03321599996895, 111.261056],
                    [189.03321599996895, -89.934464],
                    [-179.934464, -89.934464],
                ]
            ],
        },
        "properties": {
            "created": "2023-06-29T15:41:52.471025Z",
            "updated": "2023-06-29T15:41:52.471025Z",
            "datetime": "2021-12-21T03:00:00Z",
            "cube:variables": {
                "49459": {
                    "type": "data",
                    "dimensions": ["lon", "lat", "time"],
                }
            },
            "cube:dimensions": {
                "lat": {"axis": "y", "extent": [-89.934464, 111.261056]},
                "lon": {
                    "axis": "x",
                    "extent": [-179.934464, 189.03321599996895],
                },
                "time": {
                    "step": "P0DT0H0M0S",
                    "extent": [
                        "2021-12-21T03:00:00+00:00",
                        "2021-12-21T03:00:00+00:00",
                    ],
                },
            },
        },
        "collection": "Global weather (ERA5) (ZARR)",
        "links": [
            {
                "rel": "self",
                "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-staging-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/collections/Global weather (ERA5) (ZARR)/items/0bba530824d148fcb4629123e4a1328f",
                "type": "application/geo+json",
            },
            {
                "rel": "parent",
                "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-staging-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/collections/Global weather (ERA5) (ZARR)",
                "type": "application/json",
            },
            {
                "rel": "collection",
                "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-staging-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/collections/Global weather (ERA5) (ZARR)",
                "type": "application/json",
            },
            {
                "rel": "root",
                "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-staging-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/",
                "type": "application/json",
                "title": "stac-fastapi",
            },
        ],
        "assets": {
            "data": {
                "href": "https://s3.us-east.cloud-object-storage.appdomain.cloud/geofm-sampling-strategies/raster/dset_id=190/layer_id=49459/year=2021/data.zarr",
                "type": "application/zip+zarr",
                "roles": ["data"],
                "title": "Total precipitation",
            }
        },
    },
    {
        "stac_version": "1.0.0",
        "stac_extensions": [],
        "type": "Feature",
        "id": "a22242194cc0471d93ec1850a8312ba5",
        "bbox": [-179.934464, -89.934464, 189.03321599996895, 111.261056],
        "geometry": {
            "type": "Polygon",
            "coordinates": [
                [
                    [-179.934464, -89.934464],
                    [-179.934464, 111.261056],
                    [189.03321599996895, 111.261056],
                    [189.03321599996895, -89.934464],
                    [-179.934464, -89.934464],
                ]
            ],
        },
        "properties": {
            "created": "2023-06-29T15:41:52.471398Z",
            "updated": "2023-06-29T15:41:52.471398Z",
            "datetime": "2021-12-21T02:00:00Z",
            "cube:variables": {
                "49459": {
                    "type": "data",
                    "dimensions": ["lon", "lat", "time"],
                }
            },
            "cube:dimensions": {
                "lat": {"axis": "y", "extent": [-89.934464, 111.261056]},
                "lon": {
                    "axis": "x",
                    "extent": [-179.934464, 189.03321599996895],
                },
                "time": {
                    "step": "P0DT0H0M0S",
                    "extent": [
                        "2021-12-21T02:00:00+00:00",
                        "2021-12-21T02:00:00+00:00",
                    ],
                },
            },
        },
        "collection": "Global weather (ERA5) (ZARR)",
        "links": [
            {
                "rel": "self",
                "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-staging-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/collections/Global weather (ERA5) (ZARR)/items/a22242194cc0471d93ec1850a8312ba5",
                "type": "application/geo+json",
            },
            {
                "rel": "parent",
                "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-staging-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/collections/Global weather (ERA5) (ZARR)",
                "type": "application/json",
            },
            {
                "rel": "collection",
                "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-staging-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/collections/Global weather (ERA5) (ZARR)",
                "type": "application/json",
            },
            {
                "rel": "root",
                "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-staging-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/",
                "type": "application/json",
                "title": "stac-fastapi",
            },
        ],
        "assets": {
            "data": {
                "href": "https://s3.us-east.cloud-object-storage.appdomain.cloud/geofm-sampling-strategies/raster/dset_id=190/layer_id=49459/year=2021/data.zarr",
                "type": "application/zip+zarr",
                "roles": ["data"],
                "title": "Total precipitation",
            }
        },
    },
    {
        "stac_version": "1.0.0",
        "stac_extensions": [],
        "type": "Feature",
        "id": "d140ccbc65d8448fa7f36b8e87175e86",
        "bbox": [-179.934464, -89.934464, 189.03321599996895, 111.261056],
        "geometry": {
            "type": "Polygon",
            "coordinates": [
                [
                    [-179.934464, -89.934464],
                    [-179.934464, 111.261056],
                    [189.03321599996895, 111.261056],
                    [189.03321599996895, -89.934464],
                    [-179.934464, -89.934464],
                ]
            ],
        },
        "properties": {
            "created": "2023-06-29T15:41:52.471481Z",
            "updated": "2023-06-29T15:41:52.471481Z",
            "datetime": "2021-12-21T01:00:00Z",
            "cube:variables": {
                "49459": {
                    "type": "data",
                    "dimensions": ["lon", "lat", "time"],
                }
            },
            "cube:dimensions": {
                "lat": {"axis": "y", "extent": [-89.934464, 111.261056]},
                "lon": {
                    "axis": "x",
                    "extent": [-179.934464, 189.03321599996895],
                },
                "time": {
                    "step": "P0DT0H0M0S",
                    "extent": [
                        "2021-12-21T01:00:00+00:00",
                        "2021-12-21T01:00:00+00:00",
                    ],
                },
            },
        },
        "collection": "Global weather (ERA5) (ZARR)",
        "links": [
            {
                "rel": "self",
                "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-staging-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/collections/Global weather (ERA5) (ZARR)/items/d140ccbc65d8448fa7f36b8e87175e86",
                "type": "application/geo+json",
            },
            {
                "rel": "parent",
                "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-staging-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/collections/Global weather (ERA5) (ZARR)",
                "type": "application/json",
            },
            {
                "rel": "collection",
                "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-staging-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/collections/Global weather (ERA5) (ZARR)",
                "type": "application/json",
            },
            {
                "rel": "root",
                "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-staging-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/",
                "type": "application/json",
                "title": "stac-fastapi",
            },
        ],
        "assets": {
            "data": {
                "href": "https://s3.us-east.cloud-object-storage.appdomain.cloud/geofm-sampling-strategies/raster/dset_id=190/layer_id=49459/year=2021/data.zarr",
                "type": "application/zip+zarr",
                "roles": ["data"],
                "title": "Total precipitation",
            }
        },
    },
    {
        "stac_version": "1.0.0",
        "stac_extensions": [],
        "type": "Feature",
        "id": "5132df11e3794f588df9cb3885ebae9f",
        "bbox": [-179.934464, -89.934464, 189.03321599996895, 111.261056],
        "geometry": {
            "type": "Polygon",
            "coordinates": [
                [
                    [-179.934464, -89.934464],
                    [-179.934464, 111.261056],
                    [189.03321599996895, 111.261056],
                    [189.03321599996895, -89.934464],
                    [-179.934464, -89.934464],
                ]
            ],
        },
        "properties": {
            "created": "2023-06-29T15:41:52.471670Z",
            "updated": "2023-06-29T15:41:52.471670Z",
            "datetime": "2021-12-21T00:00:00Z",
            "cube:variables": {
                "49459": {
                    "type": "data",
                    "dimensions": ["lon", "lat", "time"],
                }
            },
            "cube:dimensions": {
                "lat": {"axis": "y", "extent": [-89.934464, 111.261056]},
                "lon": {
                    "axis": "x",
                    "extent": [-179.934464, 189.03321599996895],
                },
                "time": {
                    "step": "P0DT0H0M0S",
                    "extent": [
                        "2021-12-21T00:00:00+00:00",
                        "2021-12-21T00:00:00+00:00",
                    ],
                },
            },
        },
        "collection": "Global weather (ERA5) (ZARR)",
        "links": [
            {
                "rel": "self",
                "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-staging-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/collections/Global weather (ERA5) (ZARR)/items/5132df11e3794f588df9cb3885ebae9f",
                "type": "application/geo+json",
            },
            {
                "rel": "parent",
                "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-staging-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/collections/Global weather (ERA5) (ZARR)",
                "type": "application/json",
            },
            {
                "rel": "collection",
                "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-staging-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/collections/Global weather (ERA5) (ZARR)",
                "type": "application/json",
            },
            {
                "rel": "root",
                "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-staging-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/",
                "type": "application/json",
                "title": "stac-fastapi",
            },
        ],
        "assets": {
            "data": {
                "href": "https://s3.us-east.cloud-object-storage.appdomain.cloud/geofm-sampling-strategies/raster/dset_id=190/layer_id=49459/year=2021/data.zarr",
                "type": "application/zip+zarr",
                "roles": ["data"],
                "title": "Total precipitation",
            }
        },
    },
    {
        "stac_version": "1.0.0",
        "stac_extensions": [],
        "type": "Feature",
        "id": "473cd65b195d4ebf800a1c5601e70f0e",
        "bbox": [-179.934464, -89.934464, 189.03321599996895, 111.261056],
        "geometry": {
            "type": "Polygon",
            "coordinates": [
                [
                    [-179.934464, -89.934464],
                    [-179.934464, 111.261056],
                    [189.03321599996895, 111.261056],
                    [189.03321599996895, -89.934464],
                    [-179.934464, -89.934464],
                ]
            ],
        },
        "properties": {
            "created": "2023-06-29T15:41:52.471711Z",
            "updated": "2023-06-29T15:41:52.471711Z",
            "datetime": "2021-12-20T23:00:00Z",
            "cube:variables": {
                "49459": {
                    "type": "data",
                    "dimensions": ["lon", "lat", "time"],
                }
            },
            "cube:dimensions": {
                "lat": {"axis": "y", "extent": [-89.934464, 111.261056]},
                "lon": {
                    "axis": "x",
                    "extent": [-179.934464, 189.03321599996895],
                },
                "time": {
                    "step": "P0DT0H0M0S",
                    "extent": [
                        "2021-12-20T23:00:00+00:00",
                        "2021-12-20T23:00:00+00:00",
                    ],
                },
            },
        },
        "collection": "Global weather (ERA5) (ZARR)",
        "links": [
            {
                "rel": "self",
                "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-staging-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/collections/Global weather (ERA5) (ZARR)/items/473cd65b195d4ebf800a1c5601e70f0e",
                "type": "application/geo+json",
            },
            {
                "rel": "parent",
                "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-staging-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/collections/Global weather (ERA5) (ZARR)",
                "type": "application/json",
            },
            {
                "rel": "collection",
                "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-staging-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/collections/Global weather (ERA5) (ZARR)",
                "type": "application/json",
            },
            {
                "rel": "root",
                "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-staging-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/",
                "type": "application/json",
                "title": "stac-fastapi",
            },
        ],
        "assets": {
            "data": {
                "href": "https://s3.us-east.cloud-object-storage.appdomain.cloud/geofm-sampling-strategies/raster/dset_id=190/layer_id=49459/year=2021/data.zarr",
                "type": "application/zip+zarr",
                "roles": ["data"],
                "title": "Total precipitation",
            }
        },
    },
    {
        "stac_version": "1.0.0",
        "stac_extensions": [],
        "type": "Feature",
        "id": "1d6e22d8dcb648348fe5ed12601a79c1",
        "bbox": [-179.934464, -89.934464, 189.03321599996895, 111.261056],
        "geometry": {
            "type": "Polygon",
            "coordinates": [
                [
                    [-179.934464, -89.934464],
                    [-179.934464, 111.261056],
                    [189.03321599996895, 111.261056],
                    [189.03321599996895, -89.934464],
                    [-179.934464, -89.934464],
                ]
            ],
        },
        "properties": {
            "created": "2023-06-29T15:41:52.471993Z",
            "updated": "2023-06-29T15:41:52.471993Z",
            "datetime": "2021-12-20T22:00:00Z",
            "cube:variables": {
                "49459": {
                    "type": "data",
                    "dimensions": ["lon", "lat", "time"],
                }
            },
            "cube:dimensions": {
                "lat": {"axis": "y", "extent": [-89.934464, 111.261056]},
                "lon": {
                    "axis": "x",
                    "extent": [-179.934464, 189.03321599996895],
                },
                "time": {
                    "step": "P0DT0H0M0S",
                    "extent": [
                        "2021-12-20T22:00:00+00:00",
                        "2021-12-20T22:00:00+00:00",
                    ],
                },
            },
        },
        "collection": "Global weather (ERA5) (ZARR)",
        "links": [
            {
                "rel": "self",
                "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-staging-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/collections/Global weather (ERA5) (ZARR)/items/1d6e22d8dcb648348fe5ed12601a79c1",
                "type": "application/geo+json",
            },
            {
                "rel": "parent",
                "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-staging-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/collections/Global weather (ERA5) (ZARR)",
                "type": "application/json",
            },
            {
                "rel": "collection",
                "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-staging-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/collections/Global weather (ERA5) (ZARR)",
                "type": "application/json",
            },
            {
                "rel": "root",
                "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-staging-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/",
                "type": "application/json",
                "title": "stac-fastapi",
            },
        ],
        "assets": {
            "data": {
                "href": "https://s3.us-east.cloud-object-storage.appdomain.cloud/geofm-sampling-strategies/raster/dset_id=190/layer_id=49459/year=2021/data.zarr",
                "type": "application/zip+zarr",
                "roles": ["data"],
                "title": "Total precipitation",
            }
        },
    },
    {
        "stac_version": "1.0.0",
        "stac_extensions": [],
        "type": "Feature",
        "id": "bfcfeb748a6f49b983054599875be670",
        "bbox": [-179.934464, -89.934464, 189.03321599996895, 111.261056],
        "geometry": {
            "type": "Polygon",
            "coordinates": [
                [
                    [-179.934464, -89.934464],
                    [-179.934464, 111.261056],
                    [189.03321599996895, 111.261056],
                    [189.03321599996895, -89.934464],
                    [-179.934464, -89.934464],
                ]
            ],
        },
        "properties": {
            "created": "2023-06-29T15:41:52.472098Z",
            "updated": "2023-06-29T15:41:52.472098Z",
            "datetime": "2021-12-20T21:00:00Z",
            "cube:variables": {
                "49459": {
                    "type": "data",
                    "dimensions": ["lon", "lat", "time"],
                }
            },
            "cube:dimensions": {
                "lat": {"axis": "y", "extent": [-89.934464, 111.261056]},
                "lon": {
                    "axis": "x",
                    "extent": [-179.934464, 189.03321599996895],
                },
                "time": {
                    "step": "P0DT0H0M0S",
                    "extent": [
                        "2021-12-20T21:00:00+00:00",
                        "2021-12-20T21:00:00+00:00",
                    ],
                },
            },
        },
        "collection": "Global weather (ERA5) (ZARR)",
        "links": [
            {
                "rel": "self",
                "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-staging-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/collections/Global weather (ERA5) (ZARR)/items/bfcfeb748a6f49b983054599875be670",
                "type": "application/geo+json",
            },
            {
                "rel": "parent",
                "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-staging-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/collections/Global weather (ERA5) (ZARR)",
                "type": "application/json",
            },
            {
                "rel": "collection",
                "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-staging-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/collections/Global weather (ERA5) (ZARR)",
                "type": "application/json",
            },
            {
                "rel": "root",
                "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-staging-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/",
                "type": "application/json",
                "title": "stac-fastapi",
            },
        ],
        "assets": {
            "data": {
                "href": "https://s3.us-east.cloud-object-storage.appdomain.cloud/geofm-sampling-strategies/raster/dset_id=190/layer_id=49459/year=2021/data.zarr",
                "type": "application/zip+zarr",
                "roles": ["data"],
                "title": "Total precipitation",
            }
        },
    },
    {
        "stac_version": "1.0.0",
        "stac_extensions": [],
        "type": "Feature",
        "id": "0aadbb6b79374ee49e3000e94343aaea",
        "bbox": [-179.934464, -89.934464, 189.03321599996895, 111.261056],
        "geometry": {
            "type": "Polygon",
            "coordinates": [
                [
                    [-179.934464, -89.934464],
                    [-179.934464, 111.261056],
                    [189.03321599996895, 111.261056],
                    [189.03321599996895, -89.934464],
                    [-179.934464, -89.934464],
                ]
            ],
        },
        "properties": {
            "created": "2023-06-29T15:41:52.472176Z",
            "updated": "2023-06-29T15:41:52.472176Z",
            "datetime": "2021-12-20T20:00:00Z",
            "cube:variables": {
                "49459": {
                    "type": "data",
                    "dimensions": ["lon", "lat", "time"],
                }
            },
            "cube:dimensions": {
                "lat": {"axis": "y", "extent": [-89.934464, 111.261056]},
                "lon": {
                    "axis": "x",
                    "extent": [-179.934464, 189.03321599996895],
                },
                "time": {
                    "step": "P0DT0H0M0S",
                    "extent": [
                        "2021-12-20T20:00:00+00:00",
                        "2021-12-20T20:00:00+00:00",
                    ],
                },
            },
        },
        "collection": "Global weather (ERA5) (ZARR)",
        "links": [
            {
                "rel": "self",
                "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-staging-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/collections/Global weather (ERA5) (ZARR)/items/0aadbb6b79374ee49e3000e94343aaea",
                "type": "application/geo+json",
            },
            {
                "rel": "parent",
                "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-staging-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/collections/Global weather (ERA5) (ZARR)",
                "type": "application/json",
            },
            {
                "rel": "collection",
                "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-staging-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/collections/Global weather (ERA5) (ZARR)",
                "type": "application/json",
            },
            {
                "rel": "root",
                "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-staging-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/",
                "type": "application/json",
                "title": "stac-fastapi",
            },
        ],
        "assets": {
            "data": {
                "href": "https://s3.us-east.cloud-object-storage.appdomain.cloud/geofm-sampling-strategies/raster/dset_id=190/layer_id=49459/year=2021/data.zarr",
                "type": "application/zip+zarr",
                "roles": ["data"],
                "title": "Total precipitation",
            }
        },
    },
    {
        "stac_version": "1.0.0",
        "stac_extensions": [],
        "type": "Feature",
        "id": "3299076cbb754731bf404f4bba2a3ef2",
        "bbox": [-179.934464, -89.934464, 189.03321599996895, 111.261056],
        "geometry": {
            "type": "Polygon",
            "coordinates": [
                [
                    [-179.934464, -89.934464],
                    [-179.934464, 111.261056],
                    [189.03321599996895, 111.261056],
                    [189.03321599996895, -89.934464],
                    [-179.934464, -89.934464],
                ]
            ],
        },
        "properties": {
            "created": "2023-06-29T15:41:52.472285Z",
            "updated": "2023-06-29T15:41:52.472285Z",
            "datetime": "2021-12-20T19:00:00Z",
            "cube:variables": {
                "49459": {
                    "type": "data",
                    "dimensions": ["lon", "lat", "time"],
                }
            },
            "cube:dimensions": {
                "lat": {"axis": "y", "extent": [-89.934464, 111.261056]},
                "lon": {
                    "axis": "x",
                    "extent": [-179.934464, 189.03321599996895],
                },
                "time": {
                    "step": "P0DT0H0M0S",
                    "extent": [
                        "2021-12-20T19:00:00+00:00",
                        "2021-12-20T19:00:00+00:00",
                    ],
                },
            },
        },
        "collection": "Global weather (ERA5) (ZARR)",
        "links": [
            {
                "rel": "self",
                "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-staging-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/collections/Global weather (ERA5) (ZARR)/items/3299076cbb754731bf404f4bba2a3ef2",
                "type": "application/geo+json",
            },
            {
                "rel": "parent",
                "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-staging-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/collections/Global weather (ERA5) (ZARR)",
                "type": "application/json",
            },
            {
                "rel": "collection",
                "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-staging-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/collections/Global weather (ERA5) (ZARR)",
                "type": "application/json",
            },
            {
                "rel": "root",
                "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-staging-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/",
                "type": "application/json",
                "title": "stac-fastapi",
            },
        ],
        "assets": {
            "data": {
                "href": "https://s3.us-east.cloud-object-storage.appdomain.cloud/geofm-sampling-strategies/raster/dset_id=190/layer_id=49459/year=2021/data.zarr",
                "type": "application/zip+zarr",
                "roles": ["data"],
                "title": "Total precipitation",
            }
        },
    },
    {
        "stac_version": "1.0.0",
        "stac_extensions": [],
        "type": "Feature",
        "id": "22ec9312e00f47289826aa30ba5257a5",
        "bbox": [-179.934464, -89.934464, 189.03321599996895, 111.261056],
        "geometry": {
            "type": "Polygon",
            "coordinates": [
                [
                    [-179.934464, -89.934464],
                    [-179.934464, 111.261056],
                    [189.03321599996895, 111.261056],
                    [189.03321599996895, -89.934464],
                    [-179.934464, -89.934464],
                ]
            ],
        },
        "properties": {
            "created": "2023-06-29T15:41:52.472421Z",
            "updated": "2023-06-29T15:41:52.472421Z",
            "datetime": "2021-12-20T18:00:00Z",
            "cube:variables": {
                "49459": {
                    "type": "data",
                    "dimensions": ["lon", "lat", "time"],
                }
            },
            "cube:dimensions": {
                "lat": {"axis": "y", "extent": [-89.934464, 111.261056]},
                "lon": {
                    "axis": "x",
                    "extent": [-179.934464, 189.03321599996895],
                },
                "time": {
                    "step": "P0DT0H0M0S",
                    "extent": [
                        "2021-12-20T18:00:00+00:00",
                        "2021-12-20T18:00:00+00:00",
                    ],
                },
            },
        },
        "collection": "Global weather (ERA5) (ZARR)",
        "links": [
            {
                "rel": "self",
                "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-staging-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/collections/Global weather (ERA5) (ZARR)/items/22ec9312e00f47289826aa30ba5257a5",
                "type": "application/geo+json",
            },
            {
                "rel": "parent",
                "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-staging-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/collections/Global weather (ERA5) (ZARR)",
                "type": "application/json",
            },
            {
                "rel": "collection",
                "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-staging-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/collections/Global weather (ERA5) (ZARR)",
                "type": "application/json",
            },
            {
                "rel": "root",
                "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-staging-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/",
                "type": "application/json",
                "title": "stac-fastapi",
            },
        ],
        "assets": {
            "data": {
                "href": "https://s3.us-east.cloud-object-storage.appdomain.cloud/geofm-sampling-strategies/raster/dset_id=190/layer_id=49459/year=2021/data.zarr",
                "type": "application/zip+zarr",
                "roles": ["data"],
                "title": "Total precipitation",
            }
        },
    },
]


@xr.register_dataarray_accessor("openeo")
class OpenEOExtensionDa:
    def __init__(self, xarray_obj):
        self._obj = xarray_obj
        self._spatial_dims = self._guess_dims_for_type(
            X_GUESSES
        ) + self._guess_dims_for_type(Y_GUESSES)
        self._temporal_dims = self._guess_dims_for_type(TEMPORAL_GUESSES)
        self._bands_dims = self._guess_dims_for_type(BANDS_GUESSES)
        self._other_dims = [
            dim
            for dim in self._obj.dims
            if dim not in self._spatial_dims + self._temporal_dims + self._bands_dims
        ]

    @property
    def _lowercase_dims(self):
        return [str(dim).casefold() for dim in self._obj.dims]

    def _guess_dims_for_type(self, guesses) -> List[str]:
        found_dims = []
        datacube_dims = self._lowercase_dims
        for guess in guesses:
            if guess in datacube_dims:
                i = datacube_dims.index(guess)
                found_dims.append(self._obj.dims[i])
        return found_dims

    def _get_existing_dims_and_pop_missing(self, expected_dims: List[str]) -> List[str]:
        existing_dims: List[str] = []
        size_expected_dims = len(expected_dims)
        for i in range(size_expected_dims - 1, -1, -1):
            dim = expected_dims[i]
            if dim in self._obj.dims:
                existing_dims.append(dim)
            else:
                expected_dims.pop(i)
        return existing_dims

    @property
    def spatial_dims(self) -> tuple[str, ...]:
        """Find and return all spatial dimensions of the datacube as a tuple."""
        spatial_dims = tuple(
            self._get_existing_dims_and_pop_missing(self._spatial_dims)
        )
        return spatial_dims

    @property
    def temporal_dims(self) -> tuple[str, ...]:
        """Find and return all temporal dimensions of the datacube as a list."""
        return tuple(self._get_existing_dims_and_pop_missing(self._temporal_dims))

    @property
    def band_dims(self) -> tuple[str, ...]:
        """Find and return all bands dimensions of the datacube as a list."""
        return tuple(self._get_existing_dims_and_pop_missing(self._bands_dims))

    @property
    def other_dims(self) -> tuple[str, ...]:
        """Find and return any dimensions with type other as s list."""
        return tuple(self._get_existing_dims_and_pop_missing(self._other_dims))

    @property
    def x_dim(self) -> Optional[str]:
        return next(
            iter(
                [
                    dim
                    for dim in self.spatial_dims
                    if str(dim).casefold() in X_GUESSES and dim in self._obj.dims
                ]
            ),
            None,
        )

    @property
    def y_dim(self) -> Optional[str]:
        return next(
            iter(
                [
                    dim
                    for dim in self.spatial_dims
                    if str(dim).casefold() in Y_GUESSES and dim in self._obj.dims
                ]
            ),
            None,
        )

    @property
    def z_dim(self):
        raise NotImplementedError()

    def add_dim_type(self, name: str, type: str) -> None:
        """Add dimension name to the list of guesses when calling add_dimension."""

        if name not in self._obj.dims:
            raise ValueError("Trying to add a dimension that doesn't exist")

        if type == "spatial":
            self._spatial_dims.append(name)
        elif type == "temporal":
            self._temporal_dims.append(name)
        elif type == "bands":
            self._bands_dims.append(name)
        elif type == "other":
            self._other_dims.append(name)
        else:
            raise ValueError(f"Type {type} is not understood")


class MockTemporalInterval:
    def __init__(self, start: pd.Timestamp, end: pd.Timestamp) -> None:
        self.start = start
        self.end = end


class MockGeoDNDiscovery:
    def __init__(
        self,
        api_key: Optional[str] = None,
        client_id: str = "ibm-pairs",
        password: Optional[str] = None,
        auth_url: str = "https://auth-b2b-twc.ibm.com/auth/GetBearerForClient",
        api_url: str = "https://pairs.res.ibm.com",
    ) -> None:
        self.api_key = None

    def list_datasets(self) -> List[DatasetMetadata]:
        return [
            DatasetMetadata(
                dataset_id="123",
                latitude_min=-90,
                latitude_max=90,
                longitude_max=180,
                longitude_min=-180,
                name="fake dataset",
                description_short="short description",
                license="unknown",
                level=11,
                temporal_max=None,
                temporal_min=None,
                spatial_resolution_of_raw_data=None,
                temporal_resolution_description=None,
            )
        ]


def make_pystac_client_collection(collection_id: str = "fake-id") -> Collection:
    xmin, ymin, xmax, ymax = -100, 40, -90, 45
    start = datetime(2000, 1, 1)
    end = datetime(2000, 2, 1)
    temporal = TemporalExtent(intervals=[[start, end]])

    spatial_extent = SpatialExtent(bboxes=[[xmin, ymin, xmax, ymax]])  # type: ignore
    extent = Extent(spatial=spatial_extent, temporal=temporal)
    cube_dimensions = {
        "cube:dimensions": {
            DEFAULT_X_DIMENSION: {
                "axis": "x",
                "step": 0.000064,
                "type": "spatial",
                "extent": [xmin, xmax],
                "reference_system": 4326,
            },
            DEFAULT_Y_DIMENSION: {
                "axis": "y",
                "step": 0.000064,
                "type": "spatial",
                "extent": [ymin, ymax],
                "reference_system": 4326,
            },
            DEFAULT_TIME_DIMENSION: {
                "type": "temporal",
                "extent": [
                    start.isoformat(sep="T"),
                    end.isoformat(sep="T"),
                ],
            },
            DEFAULT_BANDS_DIMENSION: {"type": "bands", "values": ["lulc"]},
        }
    }
    summaries_dict = {
        "datetime": {
            "minimum": start.isoformat(sep="T"),
            "maximum": end.isoformat(sep="T"),
        }
    }
    summaries = Summaries(summaries=summaries_dict)

    return Collection(
        id=collection_id,
        description="description",
        extent=extent,
        title="title",
        href="https://fake-cos-cloud.com",
        summaries=summaries,
        extra_fields=cube_dimensions,
    )


def get_collection_items(collection_id: str, parameters: Optional[str] = None):
    s = {
        "type": "FeatureCollection",
        "features": STAC_ITEMS,
        "links": [{}],
        "timeStamp": "2023-08-24T19:47:14.625036",
        "numberMatched": 10,
        "numberReturned": 10,
    }
    return s


class MockStacSearchResult:

    def __init__(self, items) -> None:
        self.items = items

    def items_as_dicts(self):
        return self.items


class MockPystacClient:
    def __init__(self, collection_id: str = "fake-collection-id") -> None:
        self._collection_id = collection_id

    def get_collections(self):
        return [make_pystac_client_collection(collection_id=self._collection_id)]

    def get_collection(self, collection_id):
        return make_pystac_client_collection(collection_id=collection_id)

    def search(
        self,
        method: Optional[str] = "POST",
        max_items: Optional[int] = None,
        limit: Optional[int] = None,
        ids: Optional[str] = None,
        collections: Optional[str] = None,
        bbox: Optional[List] = None,
        datetime: Optional[str] = None,
        fields: Dict[str, Any] = {},
    ):
        return MockStacSearchResult(items=HLSS30_ITEMS)


class MockSTACClient:
    @staticmethod
    def open(url):
        return MockPystacClient()


def generate_xarray(
    bands: List[str],
    latmax: float,
    latmin: float,
    lonmax: float,
    lonmin: float,
    temporal_extent: Optional[Tuple[pd.Timestamp, pd.Timestamp]],
    extra_dimensions: Dict[str, List[str]] = {},
    size_x: int = 100,
    size_y: int = 100,
    num_periods: Optional[int] = 10,
    freq: Optional[str] = "D",
    crs: Optional[str] = EPSG_4326,
    is_dataset: bool = False,
) -> Union[xr.DataArray, xr.Dataset]:
    """generate a synthetic data array for testing

    Args:
        bands (List[str]): _description_
        latmax (float): _description_
        latmin (float): _description_
        lonmax (float): _description_
        lonmin (float): _description_
        timestamps (Tuple[pd.Timestamp, pd.Timestamp]): start and end
        size_x (int, optional): _description_. Defaults to 100.
        size_y (int, optional): _description_. Defaults to 100.
        size_time (int, optional): _description_. Defaults to 10.
        crs (str, optional): _description_. Defaults to 4326.
        is_dataset (bool): True if the object that is returned is a xr.Dataset and False if it is
            xr.DataArray

    Raises:
        ValueError: _description_

    Returns:
        Union[xr.DataArray, xr.Dataset]: raster data cube
    """
    np.random.seed(0)
    x = np.linspace(lonmin, lonmax, size_x).tolist()
    y = np.linspace(latmin, latmax, size_y).tolist()
    if temporal_extent is not None:
        start, stop = temporal_extent
        timestamps = pd.date_range(
            start=start, end=stop, periods=num_periods, freq=freq
        )
        if num_periods is None:
            num_periods = len(timestamps)
        assert isinstance(num_periods, int)
        dims = [DEFAULT_TIME_DIMENSION, DEFAULT_Y_DIMENSION, DEFAULT_X_DIMENSION]
        coords = {
            DEFAULT_TIME_DIMENSION: timestamps.values,
            DEFAULT_Y_DIMENSION: y,
            DEFAULT_X_DIMENSION: x,
        }
    else:
        dims = [DEFAULT_Y_DIMENSION, DEFAULT_X_DIMENSION]
        num_periods = None
        coords = {
            DEFAULT_Y_DIMENSION: y,
            DEFAULT_X_DIMENSION: x,
        }

    arrays = list()
    for band_name in bands:
        if num_periods is None:
            band_data = 15 + 8 * np.random.randn(size_y, size_x)
        else:
            band_data = 15 + 8 * np.random.randn(num_periods, size_y, size_x)
        # reference_time = pd.Timestamp("2014-09-05")
        da = xr.DataArray(
            name=band_name,
            data=band_data,
            dims=dims,
            coords=coords,
        )
        map_dims = {DEFAULT_BANDS_DIMENSION: 1}
        coords_map = {DEFAULT_BANDS_DIMENSION: [band_name]}
        for k, v in extra_dimensions.items():
            size = len(v)
            map_dims = map_dims | {k: size}
            coords_map = coords_map | {k: v}
        da = da.expand_dims(map_dims)
        da = da.assign_coords(coords_map)
        arrays.append(da)
    da = xr.concat(arrays, pd.Index(bands, name=DEFAULT_BANDS_DIMENSION))
    if crs is not None:
        da.rio.write_crs(crs, inplace=True)
    assert isinstance(da, xr.DataArray)
    assert isinstance(da.openeo.spatial_dims, tuple)
    if is_dataset:
        ds = da.to_dataset(dim=DEFAULT_BANDS_DIMENSION)
        return ds
    else:
        return da


def validate_OpenEO_collection(collection: Dict, full: bool = False) -> None:
    """validate openeo collection (STAC and DataCube)

    Args:
        collection (Dict): _description_
        full (bool): if True validate the response of GET /collections/{collection_id}, otherwise validate the response of GET /collections
    Returns:
        None:
    """
    # validate_stac_object_against_schema(stac_object=collection)
    # check id
    assert isinstance(collection["id"], str), f"Error! id not a str: {collection}"
    basic_keys = [
        "stac_version",
        "stac_extensions",
        "id",
        "title",
        "description",
        "keywords",
        "version",
        "deprecated",
        "license",
        "providers",
        "extent",
        "links",
    ]
    for k in basic_keys:
        assert k in collection.keys(), f"Error! Missing key: {k} in {collection}"
    assert isinstance(
        collection["stac_version"], str
    ), f"Error! stac_version is not a str: {collection}"

    _validate_extent_spatial_bbox(collection=collection)

    _validate_extent_temporal_interval(collection=collection)
    assert isinstance(
        collection["description"], str
    ), f"Error! description is not a str: {collection}"
    assert isinstance(
        collection["license"], str
    ), f"Error! license is not a str: {collection}"
    # full is a flag that indicates that this collection has full metadata
    if full:
        assert isinstance(
            collection["cube:dimensions"], dict
        ), f"Error! cube:dimensions is not a dict: {collection}"
        cube_dimensions = collection["cube:dimensions"]
        assert isinstance(
            cube_dimensions, dict
        ), f"Error! cube_dimensions is not a dict {cube_dimensions}"
        for description, cube_dim_info in cube_dimensions.items():
            assert isinstance(description, str)
            assert isinstance(cube_dim_info, dict)
            if cube_dim_info["type"] == "spatial":
                if "axis" in cube_dim_info.keys():
                    assert cube_dim_info["axis"] in [
                        DEFAULT_X_DIMENSION,
                        DEFAULT_Y_DIMENSION,
                    ]

    # summaries = collection["summaries"]
    # assert isinstance(summaries, dict), f"Error! Unexpected type: {summaries}"


def validate_stac_object_against_schema(stac_object: Dict, schema_path: Path):
    """validate collection object against selected schemas

    Args:
        collection (Dict): _description_
        selected_schemas (List[str], optional): list of filenames (without extension). Defaults to ["collection"].
    """
    with open(schema_path) as f:
        schema = json.load(f)
        print(json.dumps(stac_object))
        validate(instance=stac_object, schema=schema)


def validate_STAC_Collection(collection: Dict[str, Any]) -> None:
    mandatory_fields = [
        "stac_version",
        "id",
        "description",
        "license",
        "extent",
        "links",
    ]
    assert isinstance(collection, dict)
    assert all(m in collection.keys() for m in mandatory_fields)

    assert isinstance(collection["id"], str), f"Error! id not a str: {collection}"

    _validate_extent_spatial_bbox(collection=collection)
    _validate_extent_temporal_interval(collection=collection)
    assert isinstance(
        collection["stac_version"], str
    ), f"Error! stac_version is not a str: {collection}"
    assert isinstance(
        collection["description"], str
    ), f"Error! description is not a str: {collection}"
    assert isinstance(
        collection["license"], str
    ), f"Error! license is not a str: {collection}"


def _validate_extent_temporal_interval(collection: Dict):
    temporal_interval = collection["extent"]["temporal"]["interval"]
    assert isinstance(temporal_interval, list)
    if len(temporal_interval) > 0:
        for inner_temporal_interval in temporal_interval:
            if len(inner_temporal_interval) > 0:
                for t in inner_temporal_interval:
                    assert isinstance(t, str) or t is None, f"Error! not a str {t}"


def _validate_extent_spatial_bbox(collection: Dict):
    bbox = collection["extent"]["spatial"]["bbox"]
    assert isinstance(bbox, list), f"Error! bbox is not a list: {bbox}"

    for bbox_item in bbox:
        assert isinstance(
            bbox_item, list
        ), f"Error! bbox item is not a list: {bbox_item}"
        for coord in bbox_item:
            assert isinstance(coord, float) or isinstance(
                coord, int
            ), f"Error! coord is neither a float nor an int: {coord}"


def create_dataset_metadatas(
    collection: str = "my-fake-dataset",
) -> List[DatasetMetadata]:
    collection = "my-fake-dataset"
    collection_id = "fake dataset"
    list_datasets = [
        DatasetMetadata(
            dataset_id=collection,
            latitude_min=-90,
            latitude_max=90,
            longitude_max=180,
            longitude_min=-180,
            name=collection_id,
            description_short="short description",
            license="unknown",
            level=11,
            temporal_max=1683916723,
            temporal_min=1683916713,
            spatial_resolution_of_raw_data=None,
            temporal_resolution_description=None,
            temporal_resolution=None,
        )
    ]
    return list_datasets


def create_data_layers(dataset_id: str, num_layers: int = 1):
    layers = list()
    layer_id = f"layer_{uuid.uuid4().hex}"
    for i in range(num_layers):
        layers.append(
            LayerMetadata(
                layer_id=layer_id,
                description_short="description",
                name="layer_name",
                dataset_id=dataset_id,
            )
        )
    return layers


def _remove_files_in_dir(dir_path: Path, prefix: str, suffix: str):
    files = _find_files_in_dir(dir_path=dir_path, prefix=prefix, suffix=suffix)
    for f in files:
        f.unlink()


def _find_files_in_dir(dir_path: Path, prefix: str, suffix: str) -> List[Path]:
    file_list = list()
    assert dir_path.exists()
    assert dir_path.is_dir()
    p = dir_path.glob("**/*")
    files = [x for x in p if x.is_file()]
    for f in files:
        parts = f.parts
        filename = parts[-1]
        if filename.startswith(prefix) and filename.endswith(suffix):
            file_list.append(f)
    return file_list


def save_openeo_response(
    prefix: str, data: bytes, content_type: str
) -> Tuple[str, Path]:
    """save openeo response based on the file format specified in the headers

    Args:
        prefix (str): prefix of the filename
        resp (requests.Response): response from OpenEO

    Raises:
        ValueError: _description_

    Returns:
        Tuple[str, Path]: file format and path to file
    """
    test_data_dir = Path(TEST_DATA_ROOT)
    if content_type == "image/tiff; application=geotiff":
        file_format = GTIFF
        path = test_data_dir / f"{prefix}{uuid.uuid4().hex}.tiff"

    elif "netcdf" in content_type.lower():
        file_format = NETCDF
        path = test_data_dir / f"{prefix}{uuid.uuid4().hex}.nc"

    elif "zip" in content_type.lower() or content_type == "application/octet-stream":
        file_format = ZIP
        path = test_data_dir / f"{prefix}{uuid.uuid4().hex}.zip"
    elif "parquet" in content_type.lower():
        file_format = PARQUET
        path = test_data_dir / f"{prefix}{uuid.uuid4().hex}.parquet"

    else:
        raise ValueError(f"Error! content type {content_type} is not supported")
    with open(path, "wb") as s:
        s.write(data)
    assert path.exists(), f"Error! unable to save file {path}"
    return file_format, path


def open_raster(
    path: Path,
    file_format: str = NETCDF,
) -> xr.DataArray:
    """loads netcdf file as xarray Dataset, convert it to DataArray and checks if the name and
      size of dimensions
    are valid

    Args:
        path (Path): full path to netcdf file
        expected_dims (Dict[str, int]): expected dimensions and size
    """
    if file_format.upper() == NETCDF:
        # using decode_coords to avoid creating a dimension for spatial_ref
        # https://stackoverflow.com/questions/67306598/rioxarray-or-xarray-converts-spatial-ref-coordinate-to-variable-after-reprojec
        ds = xr.open_dataset(path, decode_coords="all")
        # ds = xr.open_dataset(path)
        assert isinstance(ds, xr.Dataset)
        if "spatial_ref" in ds.variables:
            crs = CRS.from_wkt(ds["spatial_ref"].attrs["crs_wkt"])
            ds = ds.drop_vars(["spatial_ref"])
        da = ds.to_array(dim=DEFAULT_BANDS_DIMENSION)
        da.rio.write_crs(crs, inplace=True)
    elif file_format.upper() == GTIFF:
        da = rioxarray.open_rasterio(path)  # type: ignore
        assert isinstance(da, xr.DataArray), f"Error! not a DataArray: {da}"
    elif file_format.upper() == ZIP:
        test_data_dir = path.parent
        _remove_files_in_dir(
            dir_path=test_data_dir, prefix=GEOTIFF_PREFIX, suffix="tif"
        )
        zipfile = ZipFile(path)
        zipfile.extractall(path=test_data_dir)
        raster_files = _find_files_in_dir(
            dir_path=test_data_dir, prefix=GEOTIFF_PREFIX, suffix="tif"
        )
        assert len(raster_files) > 0
        ds = xr.open_mfdataset(
            raster_files, concat_dim=DEFAULT_TIME_DIMENSION, combine="nested"
        )
        da = ds["band_data"]
    else:
        raise ValueError(f"Error! Unsupported type: {file_format}")
    return da


def plot_array(
    arr: xr.DataArray, time_index: Dict[str, int], band_index: Dict[str, int]
):
    arr.isel(time_index.update(band_index)).plot()
    # slice_arr.plot()
    plt.show()


def open_array(
    file_format: str, path: Path, band_names: List[str] = []
) -> xr.DataArray:
    """open downloaded file(s) as xr.DataArray

    Args:
        file_format (str): supported extension
        path (str): full path


    Returns:
        xr.DataArray: _description_
    """
    if file_format == NETCDF:
        # using decode_coords to avoid creating a dimension for spatial_ref
        # https://stackoverflow.com/questions/67306598/rioxarray-or-xarray-converts-spatial-ref-coordinate-to-variable-after-reprojec
        ds = xr.open_dataset(path, decode_coords="all")
        # ds = xr.open_dataset(path)
        assert isinstance(ds, xr.Dataset)
        if "spatial_ref" in ds.variables:
            crs = CRS.from_wkt(ds["spatial_ref"].attrs["crs_wkt"])
            ds = ds.drop_vars(["spatial_ref"])
        else:
            crs = ds.rio.crs
        for band_name in band_names:
            variable_names = list(ds.keys())
            assert (
                band_name in variable_names
            ), f"Error! data variable {band_name} is missing: {variable_names}"
        da = ds.to_array(dim=DEFAULT_BANDS_DIMENSION)
        da.rio.write_crs(crs, inplace=True)
    elif file_format == GTIFF:
        da = rioxarray.open_rasterio(path)  # type: ignore
        assert isinstance(da, xr.DataArray), f"Error! not a DataArray: {da}"
    elif file_format == ZIP:
        test_data_dir = path.parent
        _remove_files_in_dir(
            dir_path=test_data_dir, prefix=GEOTIFF_PREFIX, suffix="tif"
        )
        zipfile = ZipFile(path)
        zipfile.extractall(path=test_data_dir)
        raster_files = _find_files_in_dir(
            dir_path=test_data_dir, prefix=GEOTIFF_PREFIX, suffix="tif"
        )
        assert len(raster_files) > 0
        ds = xr.open_mfdataset(
            raster_files, concat_dim=DEFAULT_TIME_DIMENSION, combine="nested"
        )
        da = ds["band_data"]
        da = da.rename({"band": DEFAULT_BANDS_DIMENSION})
    else:
        raise ValueError(f"Error! Unsupported type: {file_format}")

    return da


def validate_downloaded_file(
    path: Path,
    expected_dimension_size: Dict[str, int],
    band_names: List[str],
    temporal_extent: Optional[Tuple[str, str]],
    spatial_extent: Optional[Dict[str, Union[float, int, str]]],
    expected_crs: CRS,
    file_format: str = NETCDF,
):
    """loads netcdf file as xarray Dataset, convert it to DataArray and checks if the name and
      size of dimensions
    are valid

    Args:
        path (Path): full path to netcdf file
        expected_dims (Dict[str, int]): expected dimensions and size
    """
    if file_format in [PARQUET]:
        cube = geopandas.read_parquet(path=path)
        assert isinstance(
            cube, geopandas.GeoDataFrame
        ), f"Error! {type(cube)} is not geopandas.Geodataframe"
        assert temporal_extent is not None
        assert spatial_extent is not None
        validate_vector_datacube(
            vector_cube=cube,
            bands=band_names,
            expected_dimensions=expected_dimension_size,
            temporal_extent=temporal_extent,
            spatial_extent=spatial_extent,
        )
    else:
        da = open_array(file_format=file_format, path=path, band_names=band_names)
        validate_raster_datacube(
            cube=da,
            expected_dim_size=expected_dimension_size,
            expected_attrs={},
            expected_crs=expected_crs,
        )


# def validate_vector_datacube(
#     cube: geopandas.GeoDataFrame,
#     band_names: List[str],
#     expected_dim_size: Dict[str, Any],
# ):
#     dimension_names = list(expected_dim_size.keys())
#     columns = band_names + dimension_names
#     for col in set(columns):
#         assert col in cube.columns, f"Error! {col=} is not in {cube.columns}"
#         size = len(cube[col].unique())
#         if col in expected_dim_size.keys():
#             assert (
#                 expected_dim_size[col] == size
#             ), f"Error! {expected_dim_size=} {size=}"


def validate_raster_datacube(
    cube: xr.DataArray,
    expected_dim_size: Dict[str, int],
    expected_attrs: Dict[str, str],
    expected_crs: CRS,
    band_names: List[str] = [],
):
    """validate raster datacube

    Args:
        cube (xr.DataArray): raster data cube
        expected_dims (Dict[str, int]): expected dimension label and size
        expected_attrs (Dict[str, str]): exptected attributes key and values
        expected_crs (str): expected reference system
    """
    assert isinstance(
        cube, xr.DataArray
    ), f"Error! {type(cube)} is not a xarray.DataArray"
    # validate dimensions label and size
    for name, size in expected_dim_size.items():
        if size >= 0:
            cube_dimensions = cube.dims
            assert (
                name in cube_dimensions
            ), f"Error! The dimension called {name} is not a valid dimension name. Valid dimensions are {cube_dimensions}"
            actual_size = len(cube[name])
            assert (
                actual_size == size
            ), f"Error! the size of dimension {name} is invalid: actual size is {actual_size}, but the expected size is {size}: {cube.sizes}"
    # validate attributes key and values
    for key, value in expected_attrs.items():
        assert cube.attrs[key] == value

    # validate reference system
    assert (
        cube.rio.crs == expected_crs
    ), f"Error! Invalid crs = {cube.rio.crs}, but the expected CRS is {expected_crs}"

    for required_band in band_names:
        found = False
        for actual_band in cube[DEFAULT_BANDS_DIMENSION].values:
            if actual_band == required_band:
                found = True
        assert found


def generate_file(
    bands: List[str],
    filename: str,
    temporal_extent: Optional[Tuple[pd.Timestamp, pd.Timestamp]],
    file_format: str,
    directory: str = "./tensorlakehouse_openeo_driver/tests/unit/unit_test_data",
    extra_dims: Dict[str, List] = {},
    x_dim_name: str = DEFAULT_X_DIMENSION,
    y_dim_name: str = DEFAULT_Y_DIMENSION,
    time_dim_name: str = DEFAULT_TIME_DIMENSION,
):
    """generate files for testing purposes

    Args:
        bands (List[str]): _description_
        filename (str): _description_
        temporal_extent (Tuple[pd.Timestamp, pd.Timestamp]): _description_
        file_format (str): _description_
        directory (str, optional): _description_. Defaults to "./tensorlakehouse_openeo_driver/tests/unit/unit_test_data".
        x_dim_name (str, optional): _description_. Defaults to DEFAULT_X_DIMENSION.
        y_dim_name (str, optional): _description_. Defaults to DEFAULT_Y_DIMENSION.
        time_dim_name (str, optional): _description_. Defaults to DEFAULT_TIME_DIMENSION.
    """
    da = generate_xarray(
        bands=bands,
        latmin=51.0,
        latmax=52.0,
        lonmin=-1.0,
        lonmax=0.0,
        temporal_extent=temporal_extent,
        freq="D",
        num_periods=None,
        extra_dimensions=extra_dims,
    )
    dims_map = dict()
    if x_dim_name not in da.dims:
        dims_map[DEFAULT_X_DIMENSION] = x_dim_name
    if y_dim_name not in da.dims:
        dims_map[DEFAULT_Y_DIMENSION] = y_dim_name
    if time_dim_name not in da.dims and temporal_extent is not None:
        dims_map[DEFAULT_TIME_DIMENSION] = time_dim_name
    if len(dims_map) > 0:
        da = da.rename(dims_map)
    path = Path(directory) / filename
    if file_format == NETCDF:
        da.to_dataset(path)
    elif file_format == GRIB2:
        ds = da.to_dataset(dim=DEFAULT_BANDS_DIMENSION)
        to_grib(ds, path)
    logger.info(f"Save file: {path}")


def main():
    for filename in [
        "mock_extra_dim_2000_01_01.grib2",
        "mock_extra_dim_2000_01_02.grib2",
    ]:
        generate_file(
            bands=["gh", "t", "r", "u", "v"],
            filename=filename,
            file_format=GRIB2,
            extra_dims={"isobaricInhPa": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]},
            x_dim_name="longitude",
            y_dim_name="latitude",
            temporal_extent=None,
        )


if __name__ == "__main__":
    main()
