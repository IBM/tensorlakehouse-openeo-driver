FEATURE_COLLECTION_JSON = {
    "type": "FeatureCollection",
    "features": [
        {
            "stac_version": "1.0.0",
            "stac_extensions": [],
            "type": "Feature",
            "id": "bc9979b519614bcfaa8fb0f86331a324",
            "bbox": [
                -125.40851200000002,
                23.311743999999997,
                -62.62502400000002,
                52.5408,
            ],
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [-125.40851200000002, 23.311743999999997],
                        [-125.40851200000002, 52.5408],
                        [-62.62502400000002, 52.5408],
                        [-62.62502400000002, 23.311743999999997],
                        [-125.40851200000002, 23.311743999999997],
                    ]
                ],
            },
            "properties": {
                "created": "2023-06-08T00:19:27.602506Z",
                "updated": "2023-06-08T00:19:27.602506Z",
                "datetime": "2022-09-12T02:00:00Z",
                "cube:variables": {
                    "2t": {"type": "data", "dimensions": ["x", "y", "time"]},
                    "tp": {"type": "data", "dimensions": ["x", "y", "time"]},
                },
                "cube:dimensions": {
                    "x": {
                        "axis": "x",
                        "extent": [-125.40851200000002, -62.62502400000002],
                    },
                    "y": {"axis": "y", "extent": [23.311743999999997, 52.5408]},
                    "time": {
                        "step": "P0DT0H0M0S",
                        "extent": [
                            "2022-09-12T02:00:00+00:00",
                            "2022-09-12T02:00:00+00:00",
                        ],
                    },
                },
            },
            "collection": "Global weather (ERA5)",
            "links": [
                {
                    "rel": "self",
                    "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-dev-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/collections/Global weather (ERA5)/items/bc9979b519614bcfaa8fb0f86331a324",
                    "type": "application/geo+json",
                },
                {
                    "rel": "parent",
                    "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-dev-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/collections/Global weather (ERA5)",
                    "type": "application/json",
                },
                {
                    "rel": "collection",
                    "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-dev-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/collections/Global weather (ERA5)",
                    "type": "application/json",
                },
                {
                    "rel": "root",
                    "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-dev-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/",
                    "type": "application/json",
                    "title": "stac-fastapi",
                },
            ],
            "assets": {
                "data": {
                    "href": "https://s3.us-east.cloud-object-storage.appdomain.cloud/era5-cropscape-zarr/era5.zarr",
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
            "id": "3216ddef265341f9af3103009fcc4760",
            "bbox": [
                -125.40851200000002,
                23.311743999999997,
                -62.62502400000002,
                52.5408,
            ],
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [-125.40851200000002, 23.311743999999997],
                        [-125.40851200000002, 52.5408],
                        [-62.62502400000002, 52.5408],
                        [-62.62502400000002, 23.311743999999997],
                        [-125.40851200000002, 23.311743999999997],
                    ]
                ],
            },
            "properties": {
                "created": "2023-06-08T00:19:27.138142Z",
                "updated": "2023-06-08T00:19:27.138142Z",
                "datetime": "2022-09-12T01:00:00Z",
                "cube:variables": {
                    "2t": {"type": "data", "dimensions": ["x", "y", "time"]},
                    "tp": {"type": "data", "dimensions": ["x", "y", "time"]},
                },
                "cube:dimensions": {
                    "x": {
                        "axis": "x",
                        "extent": [-125.40851200000002, -62.62502400000002],
                    },
                    "y": {"axis": "y", "extent": [23.311743999999997, 52.5408]},
                    "time": {
                        "step": "P0DT0H0M0S",
                        "extent": [
                            "2022-09-12T01:00:00+00:00",
                            "2022-09-12T01:00:00+00:00",
                        ],
                    },
                },
            },
            "collection": "Global weather (ERA5)",
            "links": [
                {
                    "rel": "self",
                    "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-dev-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/collections/Global weather (ERA5)/items/3216ddef265341f9af3103009fcc4760",
                    "type": "application/geo+json",
                },
                {
                    "rel": "parent",
                    "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-dev-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/collections/Global weather (ERA5)",
                    "type": "application/json",
                },
                {
                    "rel": "collection",
                    "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-dev-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/collections/Global weather (ERA5)",
                    "type": "application/json",
                },
                {
                    "rel": "root",
                    "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-dev-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/",
                    "type": "application/json",
                    "title": "stac-fastapi",
                },
            ],
            "assets": {
                "data": {
                    "href": "https://s3.us-east.cloud-object-storage.appdomain.cloud/era5-cropscape-zarr/era5.zarr",
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
            "id": "001f81ed7e414876a01b19d46c587ef7",
            "bbox": [
                -125.40851200000002,
                23.311743999999997,
                -62.62502400000002,
                52.5408,
            ],
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [-125.40851200000002, 23.311743999999997],
                        [-125.40851200000002, 52.5408],
                        [-62.62502400000002, 52.5408],
                        [-62.62502400000002, 23.311743999999997],
                        [-125.40851200000002, 23.311743999999997],
                    ]
                ],
            },
            "properties": {
                "created": "2023-06-08T00:19:26.604913Z",
                "updated": "2023-06-08T00:19:26.604913Z",
                "datetime": "2022-09-12T00:00:00Z",
                "cube:variables": {
                    "2t": {"type": "data", "dimensions": ["x", "y", "time"]},
                    "tp": {"type": "data", "dimensions": ["x", "y", "time"]},
                },
                "cube:dimensions": {
                    "x": {
                        "axis": "x",
                        "extent": [-125.40851200000002, -62.62502400000002],
                    },
                    "y": {"axis": "y", "extent": [23.311743999999997, 52.5408]},
                    "time": {
                        "step": "P0DT0H0M0S",
                        "extent": [
                            "2022-09-12T00:00:00+00:00",
                            "2022-09-12T00:00:00+00:00",
                        ],
                    },
                },
            },
            "collection": "Global weather (ERA5)",
            "links": [
                {
                    "rel": "self",
                    "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-dev-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/collections/Global weather (ERA5)/items/001f81ed7e414876a01b19d46c587ef7",
                    "type": "application/geo+json",
                },
                {
                    "rel": "parent",
                    "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-dev-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/collections/Global weather (ERA5)",
                    "type": "application/json",
                },
                {
                    "rel": "collection",
                    "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-dev-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/collections/Global weather (ERA5)",
                    "type": "application/json",
                },
                {
                    "rel": "root",
                    "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-dev-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/",
                    "type": "application/json",
                    "title": "stac-fastapi",
                },
            ],
            "assets": {
                "data": {
                    "href": "https://s3.us-east.cloud-object-storage.appdomain.cloud/era5-cropscape-zarr/era5.zarr",
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
            "id": "cad355ceb0844c2696fc3813bc0ab8b6",
            "bbox": [
                -125.40851200000002,
                23.311743999999997,
                -62.62502400000002,
                52.5408,
            ],
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [-125.40851200000002, 23.311743999999997],
                        [-125.40851200000002, 52.5408],
                        [-62.62502400000002, 52.5408],
                        [-62.62502400000002, 23.311743999999997],
                        [-125.40851200000002, 23.311743999999997],
                    ]
                ],
            },
            "properties": {
                "created": "2023-06-08T00:19:26.144911Z",
                "updated": "2023-06-08T00:19:26.144911Z",
                "datetime": "2022-09-11T23:00:00Z",
                "cube:variables": {
                    "2t": {"type": "data", "dimensions": ["x", "y", "time"]},
                    "tp": {"type": "data", "dimensions": ["x", "y", "time"]},
                },
                "cube:dimensions": {
                    "x": {
                        "axis": "x",
                        "extent": [-125.40851200000002, -62.62502400000002],
                    },
                    "y": {"axis": "y", "extent": [23.311743999999997, 52.5408]},
                    "time": {
                        "step": "P0DT0H0M0S",
                        "extent": [
                            "2022-09-11T23:00:00+00:00",
                            "2022-09-11T23:00:00+00:00",
                        ],
                    },
                },
            },
            "collection": "Global weather (ERA5)",
            "links": [
                {
                    "rel": "self",
                    "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-dev-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/collections/Global weather (ERA5)/items/cad355ceb0844c2696fc3813bc0ab8b6",
                    "type": "application/geo+json",
                },
                {
                    "rel": "parent",
                    "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-dev-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/collections/Global weather (ERA5)",
                    "type": "application/json",
                },
                {
                    "rel": "collection",
                    "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-dev-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/collections/Global weather (ERA5)",
                    "type": "application/json",
                },
                {
                    "rel": "root",
                    "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-dev-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/",
                    "type": "application/json",
                    "title": "stac-fastapi",
                },
            ],
            "assets": {
                "data": {
                    "href": "https://s3.us-east.cloud-object-storage.appdomain.cloud/era5-cropscape-zarr/era5.zarr",
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
            "id": "dc257491b9bc4df4bf44247b8a69220d",
            "bbox": [
                -125.40851200000002,
                23.311743999999997,
                -62.62502400000002,
                52.5408,
            ],
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [-125.40851200000002, 23.311743999999997],
                        [-125.40851200000002, 52.5408],
                        [-62.62502400000002, 52.5408],
                        [-62.62502400000002, 23.311743999999997],
                        [-125.40851200000002, 23.311743999999997],
                    ]
                ],
            },
            "properties": {
                "created": "2023-06-08T00:19:25.664027Z",
                "updated": "2023-06-08T00:19:25.664027Z",
                "datetime": "2022-09-11T22:00:00Z",
                "cube:variables": {
                    "2t": {"type": "data", "dimensions": ["x", "y", "time"]},
                    "tp": {"type": "data", "dimensions": ["x", "y", "time"]},
                },
                "cube:dimensions": {
                    "x": {
                        "axis": "x",
                        "extent": [-125.40851200000002, -62.62502400000002],
                    },
                    "y": {"axis": "y", "extent": [23.311743999999997, 52.5408]},
                    "time": {
                        "step": "P0DT0H0M0S",
                        "extent": [
                            "2022-09-11T22:00:00+00:00",
                            "2022-09-11T22:00:00+00:00",
                        ],
                    },
                },
            },
            "collection": "Global weather (ERA5)",
            "links": [
                {
                    "rel": "self",
                    "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-dev-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/collections/Global weather (ERA5)/items/dc257491b9bc4df4bf44247b8a69220d",
                    "type": "application/geo+json",
                },
                {
                    "rel": "parent",
                    "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-dev-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/collections/Global weather (ERA5)",
                    "type": "application/json",
                },
                {
                    "rel": "collection",
                    "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-dev-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/collections/Global weather (ERA5)",
                    "type": "application/json",
                },
                {
                    "rel": "root",
                    "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-dev-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/",
                    "type": "application/json",
                    "title": "stac-fastapi",
                },
            ],
            "assets": {
                "data": {
                    "href": "https://s3.us-east.cloud-object-storage.appdomain.cloud/era5-cropscape-zarr/era5.zarr",
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
            "id": "f73f71c1e8cf4e499e8352bc697be667",
            "bbox": [
                -125.40851200000002,
                23.311743999999997,
                -62.62502400000002,
                52.5408,
            ],
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [-125.40851200000002, 23.311743999999997],
                        [-125.40851200000002, 52.5408],
                        [-62.62502400000002, 52.5408],
                        [-62.62502400000002, 23.311743999999997],
                        [-125.40851200000002, 23.311743999999997],
                    ]
                ],
            },
            "properties": {
                "created": "2023-06-08T00:19:25.185504Z",
                "updated": "2023-06-08T00:19:25.185504Z",
                "datetime": "2022-09-11T21:00:00Z",
                "cube:variables": {
                    "2t": {"type": "data", "dimensions": ["x", "y", "time"]},
                    "tp": {"type": "data", "dimensions": ["x", "y", "time"]},
                },
                "cube:dimensions": {
                    "x": {
                        "axis": "x",
                        "extent": [-125.40851200000002, -62.62502400000002],
                    },
                    "y": {"axis": "y", "extent": [23.311743999999997, 52.5408]},
                    "time": {
                        "step": "P0DT0H0M0S",
                        "extent": [
                            "2022-09-11T21:00:00+00:00",
                            "2022-09-11T21:00:00+00:00",
                        ],
                    },
                },
            },
            "collection": "Global weather (ERA5)",
            "links": [
                {
                    "rel": "self",
                    "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-dev-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/collections/Global weather (ERA5)/items/f73f71c1e8cf4e499e8352bc697be667",
                    "type": "application/geo+json",
                },
                {
                    "rel": "parent",
                    "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-dev-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/collections/Global weather (ERA5)",
                    "type": "application/json",
                },
                {
                    "rel": "collection",
                    "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-dev-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/collections/Global weather (ERA5)",
                    "type": "application/json",
                },
                {
                    "rel": "root",
                    "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-dev-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/",
                    "type": "application/json",
                    "title": "stac-fastapi",
                },
            ],
            "assets": {
                "data": {
                    "href": "https://s3.us-east.cloud-object-storage.appdomain.cloud/era5-cropscape-zarr/era5.zarr",
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
            "id": "ddbabf00830b4b82a8667b72a325f4bf",
            "bbox": [
                -125.40851200000002,
                23.311743999999997,
                -62.62502400000002,
                52.5408,
            ],
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [-125.40851200000002, 23.311743999999997],
                        [-125.40851200000002, 52.5408],
                        [-62.62502400000002, 52.5408],
                        [-62.62502400000002, 23.311743999999997],
                        [-125.40851200000002, 23.311743999999997],
                    ]
                ],
            },
            "properties": {
                "created": "2023-06-08T00:19:24.671397Z",
                "updated": "2023-06-08T00:19:24.671397Z",
                "datetime": "2022-09-11T20:00:00Z",
                "cube:variables": {
                    "2t": {"type": "data", "dimensions": ["x", "y", "time"]},
                    "tp": {"type": "data", "dimensions": ["x", "y", "time"]},
                },
                "cube:dimensions": {
                    "x": {
                        "axis": "x",
                        "extent": [-125.40851200000002, -62.62502400000002],
                    },
                    "y": {"axis": "y", "extent": [23.311743999999997, 52.5408]},
                    "time": {
                        "step": "P0DT0H0M0S",
                        "extent": [
                            "2022-09-11T20:00:00+00:00",
                            "2022-09-11T20:00:00+00:00",
                        ],
                    },
                },
            },
            "collection": "Global weather (ERA5)",
            "links": [
                {
                    "rel": "self",
                    "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-dev-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/collections/Global weather (ERA5)/items/ddbabf00830b4b82a8667b72a325f4bf",
                    "type": "application/geo+json",
                },
                {
                    "rel": "parent",
                    "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-dev-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/collections/Global weather (ERA5)",
                    "type": "application/json",
                },
                {
                    "rel": "collection",
                    "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-dev-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/collections/Global weather (ERA5)",
                    "type": "application/json",
                },
                {
                    "rel": "root",
                    "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-dev-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/",
                    "type": "application/json",
                    "title": "stac-fastapi",
                },
            ],
            "assets": {
                "data": {
                    "href": "https://s3.us-east.cloud-object-storage.appdomain.cloud/era5-cropscape-zarr/era5.zarr",
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
            "id": "f2a36306d3134fe29d4267248a04ff66",
            "bbox": [
                -125.40851200000002,
                23.311743999999997,
                -62.62502400000002,
                52.5408,
            ],
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [-125.40851200000002, 23.311743999999997],
                        [-125.40851200000002, 52.5408],
                        [-62.62502400000002, 52.5408],
                        [-62.62502400000002, 23.311743999999997],
                        [-125.40851200000002, 23.311743999999997],
                    ]
                ],
            },
            "properties": {
                "created": "2023-06-08T00:19:24.175157Z",
                "updated": "2023-06-08T00:19:24.175157Z",
                "datetime": "2022-09-11T19:00:00Z",
                "cube:variables": {
                    "2t": {"type": "data", "dimensions": ["x", "y", "time"]},
                    "tp": {"type": "data", "dimensions": ["x", "y", "time"]},
                },
                "cube:dimensions": {
                    "x": {
                        "axis": "x",
                        "extent": [-125.40851200000002, -62.62502400000002],
                    },
                    "y": {"axis": "y", "extent": [23.311743999999997, 52.5408]},
                    "time": {
                        "step": "P0DT0H0M0S",
                        "extent": [
                            "2022-09-11T19:00:00+00:00",
                            "2022-09-11T19:00:00+00:00",
                        ],
                    },
                },
            },
            "collection": "Global weather (ERA5)",
            "links": [
                {
                    "rel": "self",
                    "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-dev-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/collections/Global weather (ERA5)/items/f2a36306d3134fe29d4267248a04ff66",
                    "type": "application/geo+json",
                },
                {
                    "rel": "parent",
                    "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-dev-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/collections/Global weather (ERA5)",
                    "type": "application/json",
                },
                {
                    "rel": "collection",
                    "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-dev-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/collections/Global weather (ERA5)",
                    "type": "application/json",
                },
                {
                    "rel": "root",
                    "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-dev-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/",
                    "type": "application/json",
                    "title": "stac-fastapi",
                },
            ],
            "assets": {
                "data": {
                    "href": "https://s3.us-east.cloud-object-storage.appdomain.cloud/era5-cropscape-zarr/era5.zarr",
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
            "id": "056ad79b1da347b9a9228f96c0c2bd91",
            "bbox": [
                -125.40851200000002,
                23.311743999999997,
                -62.62502400000002,
                52.5408,
            ],
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [-125.40851200000002, 23.311743999999997],
                        [-125.40851200000002, 52.5408],
                        [-62.62502400000002, 52.5408],
                        [-62.62502400000002, 23.311743999999997],
                        [-125.40851200000002, 23.311743999999997],
                    ]
                ],
            },
            "properties": {
                "created": "2023-06-08T00:19:23.702121Z",
                "updated": "2023-06-08T00:19:23.702121Z",
                "datetime": "2022-09-11T18:00:00Z",
                "cube:variables": {
                    "2t": {"type": "data", "dimensions": ["x", "y", "time"]},
                    "tp": {"type": "data", "dimensions": ["x", "y", "time"]},
                },
                "cube:dimensions": {
                    "x": {
                        "axis": "x",
                        "extent": [-125.40851200000002, -62.62502400000002],
                    },
                    "y": {"axis": "y", "extent": [23.311743999999997, 52.5408]},
                    "time": {
                        "step": "P0DT0H0M0S",
                        "extent": [
                            "2022-09-11T18:00:00+00:00",
                            "2022-09-11T18:00:00+00:00",
                        ],
                    },
                },
            },
            "collection": "Global weather (ERA5)",
            "links": [
                {
                    "rel": "self",
                    "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-dev-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/collections/Global weather (ERA5)/items/056ad79b1da347b9a9228f96c0c2bd91",
                    "type": "application/geo+json",
                },
                {
                    "rel": "parent",
                    "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-dev-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/collections/Global weather (ERA5)",
                    "type": "application/json",
                },
                {
                    "rel": "collection",
                    "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-dev-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/collections/Global weather (ERA5)",
                    "type": "application/json",
                },
                {
                    "rel": "root",
                    "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-dev-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/",
                    "type": "application/json",
                    "title": "stac-fastapi",
                },
            ],
            "assets": {
                "data": {
                    "href": "https://s3.us-east.cloud-object-storage.appdomain.cloud/era5-cropscape-zarr/era5.zarr",
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
            "id": "43c3b119152945aa935bc1e92a1910bb",
            "bbox": [
                -125.40851200000002,
                23.311743999999997,
                -62.62502400000002,
                52.5408,
            ],
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [-125.40851200000002, 23.311743999999997],
                        [-125.40851200000002, 52.5408],
                        [-62.62502400000002, 52.5408],
                        [-62.62502400000002, 23.311743999999997],
                        [-125.40851200000002, 23.311743999999997],
                    ]
                ],
            },
            "properties": {
                "created": "2023-06-08T00:19:23.240867Z",
                "updated": "2023-06-08T00:19:23.240867Z",
                "datetime": "2022-09-11T17:00:00Z",
                "cube:variables": {
                    "2t": {"type": "data", "dimensions": ["x", "y", "time"]},
                    "tp": {"type": "data", "dimensions": ["x", "y", "time"]},
                },
                "cube:dimensions": {
                    "x": {
                        "axis": "x",
                        "extent": [-125.40851200000002, -62.62502400000002],
                    },
                    "y": {"axis": "y", "extent": [23.311743999999997, 52.5408]},
                    "time": {
                        "step": "P0DT0H0M0S",
                        "extent": [
                            "2022-09-11T17:00:00+00:00",
                            "2022-09-11T17:00:00+00:00",
                        ],
                    },
                },
            },
            "collection": "Global weather (ERA5)",
            "links": [
                {
                    "rel": "self",
                    "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-dev-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/collections/Global weather (ERA5)/items/43c3b119152945aa935bc1e92a1910bb",
                    "type": "application/geo+json",
                },
                {
                    "rel": "parent",
                    "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-dev-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/collections/Global weather (ERA5)",
                    "type": "application/json",
                },
                {
                    "rel": "collection",
                    "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-dev-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/collections/Global weather (ERA5)",
                    "type": "application/json",
                },
                {
                    "rel": "root",
                    "href": "http://stac-fastapi-sqlalchemy-geodn-discovery.cimf-dev-9ca4d14d48413d18ce61b80811ba4308-0000.us-south.containers.appdomain.cloud/",
                    "type": "application/json",
                    "title": "stac-fastapi",
                },
            ],
            "assets": {
                "data": {
                    "href": "https://s3.us-east.cloud-object-storage.appdomain.cloud/era5-cropscape-zarr/era5.zarr",
                    "type": "application/zip+zarr",
                    "roles": ["data"],
                    "title": "Total precipitation",
                }
            },
        },
    ],
    "links": [{}],
    "timeStamp": "2023-06-09T17:08:06.776057",
    "numberMatched": 10,
    "numberReturned": 10,
}
