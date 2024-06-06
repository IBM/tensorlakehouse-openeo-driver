# STAC format supported by tensorlakehouse openEO

## Items

Remarks:
- Internally, bands are dimensions. They are always labeled `band`.

Resources
- [STAC item spec](https://github.com/radiantearth/stac-spec/blob/master/item-spec/item-spec.md)

Example:
```
{
    "collection":"Sentinel 2 LULC",
    "id":"22K_20220101-20230101.tif",
    "assets": {
        "data": {
            "href": "s3://sentinel2-10m-lulc/02C_20220101-20230101.tif",
            "type": "image/tiff; application=geotiff; profile=cloud-optimized",
            "title": "22K_20220101-20230101.tif",
            "roles": ["data"],
            "description": ""
        }
    },
    "bbox": [-102.3,42.5,-102.1,42.8],
    "geometry": {
        "type": "Polygon",
        "coordinates": [
            [
                [-102.3,42.5], [-102.3,42.8], [-102.1,42.8], [-102.1,42.5], [-102.3, 42.5]
            ]
        ]
    },
    "properties": {
        "datetime": "2022-01-01T00:00:00.000Z",
        "cube:dimensions": {
            "x": {
                "axis": "x",
                "extent": [-102.3, 102.1],
        		"step":0.0001,
		        "reference_system":4326,
		        "type":"spatial"
            },
            "y": {
                "axis": "y",
                "extent": [42.5, 42.8],
        		"step":0.0001,
        		"reference_system":4326,
		        "type":"spatial"
            },
            "time": {
                "extent": ["2022-01-01T00:00:00+00:00", "2022-01-01T00:00:00+00:00"],
	        	"type":"temporal"
            }
        },
        "cube:variables": {
            "lulc": {
                "dimensions": ["x", "y", "time"],
                "type": "data",
                "unit": "",
                "values": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
            }
        }
    },
    "links": [],
    "type": "Feature",
    "stac_version":"1.0.0"
}
```

| Field | Type | Description | Code use |
| --- | --- | --- | --- |
| collection | string | REQUIRED. id of containing STAC Collection. | Yes |
| id | string | REQUIRED. Suggested: Object name for COS. hBase ID schema for hBase. | Yes |
| assets | Map\<string, Asset Object\>| REQUIRED. Dictionary of asset objects that can be downloaded, each with a unique key. Asset's keys must be either `data` or `<band-names>` e.g., `B02`. Assets that are not data type are currently ignored. | load_collection |
| bbox | [number] | REQUIRED if geometry is not null. In the [full specification](https://datatracker.ietf.org/doc/html/rfc7946#section-5) this is a n-dimensional bounding box (i.e. for all dimensions). We restrict to the horizontal plain in WGS 84. In other words: [min. longitude, min. latitude, max. longitude, max. latitude] which is equal to [west, south, east, north]. This is the bounding box of the asset, so the coordinates should be the edge of the raster; not the center coordinate of the pixels at the edges. Includes all pixels, even NaN ones. | STAC search |
| geometry | GeoJSON Geometry | REQUIRED if bounding box is not present. Footprint of the asset as GeoJSON in WGS 84 (lat/lon). The footprint includes all pixels, even NaN ones. | STAC search |
| properties | PropertiesObject | REQUIRED. A dictionary of additional metadata for the Item. | |
| links | [LinkObject] | List of links to resources. Specification strongly recommends a link with `rel` set to `self`. Can be left empty during manual generation. | |
| type | string | REQUIRED. MUST be set to `Feature`.| |
| stac_version | string | REQUIRED. The STAC version the Item implements. Currently: 1.0.0 | |


### Additional information

#### hBase ID schema
For hBase data, we suggest the following ID schema. This assumes using one item / timestamp.
```
layer={layer_id}/time={YYYYmmddTHHMMSS}/{dim_name_1}={dim_value_1}/{dim_name_2}={dim_value_2}/...
```

#### Properties

| Field | Type | Description | Code use |
| --- | --- | --- | --- |
| datetime | string\|null | REQUIRED. Can be null if `start_datetime` and `end_datetime` are set. Format `YYYY-mm-ddTHH:MM:SSZ`. | STAC search |
| start_datetime | string | Format `YYYY-mm-ddTHH:MM:SSZ`. | STAC search |
| end_datetime | string | Format `YYYY-mm-ddTHH:MM:SSZ`. | STAC search |
| cube:dimensions | CubeDimensionsObject | REQUIRED. `x`, `y` and `time` are mandatory. `band` and `variable` are not allowed. For spatial dimensions, `step` and `reference_system` are required. | load_collection |
| cube:variables | CubeDimensionsObject | REQUIRED. Lists the variables appearing in an item as well as their dependence on dimensions. | load_collection |

#### GeoJSON
Example GeoJSON polygon:
```
"geometry": {
        "type": "Polygon",
        "coordinates": [
            [
                [-180.0, -90.0], [-180.0, 90.0], [180.0, 90.0], [180.0, -90.0], [-180.0, -90.0]
            ]
        ]
    }
```


## Collections

Resources
- [STAC collection spec](https://github.com/radiantearth/stac-spec/blob/master/item-spec/item-spec.md)
- [OpenEO collection metadata](https://api.openeo.org/#tag/EO-Data-Discovery/operation/describe-collection)

| Field | Type | Description | Code use |
| --- | --- | --- | --- |
| id | string | REQUIRED. | Y |
| title | string| REQUIRED. Title of the collection. We use sentence case. (Technically STAC does not require this, but we do.) | |
| description | string | REQUIRED. Description of the dataset. | |
| extent | ExtentObject | REQUIRED. Spatial and temporal extent. | |
| license | string | License information; ideallu via [SPDX license identifier](https://spdx.org/licenses/). | |
| type | string | REQUIRED. MUST be set to `Collection`. | |
| stac_version | string | REQUIRED. The STAC version this Collection implements. Currently: 1.0.0 | |
| stac_extensions | [JSON\|Reference] | List of STAC extensions. E.g. `["https://stac-extensions.github.io/datacube/v2.2.0/schema.json"]`| |
| keywords | [string] | Array of keywords. | |
| version | string | Version of the collection. Use requires to add `https://stac-extensions.github.io/version/v1.2.0/schema.json` as a `stac_extension`. | |
| deprecated | boolean | Default `false`. | |
| links | [LinkObject] | REQUIRED. | |
| cube:dimensions | CubeDimensionsObject | REQUIRED. `x`, `y` and `time` are mandatory. `band` and `variable` are not allowed. For spatial dimensions, `step` and `reference_system` are required. | load_collection |
| summaries | | REQUIRED. | data discovery endpoints |
| assets | [AssetObject] |  | |

### Additional information

#### ExtentObject

Extent objects generally take the form
```
"spatial": {
    "bbox": [
        [min. longitude, min. latitude, max. longitude, max. latitude]
    ]
},
"temporal": {
    "interval": [
        [start, end]
    ]
}
```
Regarding the spatial extent, it is possible to specify multiple regions via a list of bounding boxes. Hence the double list `[[]]` above. In this case, the first bounding box represents the overall extent. All subsequent boxes clusters. We will presumably not make use of that. All coordinates are specified in WGS 84.

Similarly, the temporal extent can be given by an overall extent (first list item) and subsequent clusters. Once again, we ill not make use of this. Individual extends consist of a `start` and `end`, either of which can be `null`. The timestamp format is `YYYY-mm-ddTHH:MM:SSZ`.
