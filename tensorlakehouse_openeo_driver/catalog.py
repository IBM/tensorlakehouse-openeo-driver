import os
from typing import Any, Dict, List, Optional, Union

from pystac_client import Client, CollectionClient
from pystac import Item
from openeo_driver.backend import CollectionCatalog, LoadParameters
from openeo_driver.utils import EvalEnv
from tensorlakehouse_openeo_driver.constants import (
    GEODN_DATASERVICE_ENDPOINT,
    GEODN_DATASERVICE_USER,
    GEODN_DATASERVICE_PASSWORD,
    GEODN_DISCOVERY_USERNAME,
    GEODN_DISCOVERY_PASSWORD,
    STAC_URL,
    DEFAULT_X_DIMENSION,
    DEFAULT_Y_DIMENSION,
    DEFAULT_TIME_DIMENSION,
)
from tensorlakehouse_openeo_driver.dataset import DatasetMetadata
from tensorlakehouse_openeo_driver.driver_data_cube import GeoDNDataCube
from tensorlakehouse_openeo_driver.geodn_discovery import GeoDNDiscovery
from dataservice import query
from tensorlakehouse_openeo_driver.layer import LayerMetadata
from datetime import datetime
import pandas as pd
import logging
import xarray as xr
from tensorlakehouse_openeo_driver.model.datacube_variable import DataCubeVariable

from tensorlakehouse_openeo_driver.model.dimension import (
    BandDimension,
    Dimension,
    HorizontalSpatialDimension,
    TemporalDimension,
)

assert os.path.isfile("logging.conf")
logging.config.fileConfig(fname="logging.conf", disable_existing_loggers=False)
logger = logging.getLogger("geodnLogger")


class GeoDNCollectionCatalog(CollectionCatalog):
    STAC_VERSION = "1.0.0"

    CUBE_VARIABLES = "cube:variables"
    DATA = "data"

    def __init__(self):
        super().__init__(all_metadata=list())
        self._access_token = None
        self.discovery = GeoDNDiscovery(
            client_id=GEODN_DISCOVERY_USERNAME, password=GEODN_DISCOVERY_PASSWORD
        )
        self._stac_catalog = None

    @property
    def headers(self):
        """Gets the request headers."""
        headers = {"Content-Type": "application/json"}
        if self._access_token is not None:
            headers["Authorization"] = f"Bearer {self._access_token}"
        return headers

    @property
    def stac_client(self):
        if self._stac_catalog is None:
            self._stac_catalog = Client.open(STAC_URL)
        return self._stac_catalog

    def get_all_metadata(self) -> List[Dict]:
        """
        GET /collections/
        get metadata from GeoDN.Discovery and adapt it to the openeo-driver format, which is
        a dict that has the following (sub-)fields:
            - id (string; required) - A unique identifier for the collection, which MUST match the specified pattern.
            - title (string; optional) - A short descriptive one-line title for the collection.
            - cube:dimensions (object; optional) - Uniquely named dimensions of the data cube. The keys of the object are the dimension names. For interoperability, it is RECOMMENDED to use the following dimension names if there is only a single dimension with the specified criteria
            - summaries (object; optional): Summaries are either a unique set of all available values or statistics. Statistics by default only specify the range (minimum and maximum values), but can optionally be accompanied by additional statistical values.
            - extent
                - spatial (object; required) - The potential spatial extents of the features in the collection.
                    - bbox - Array of Array of 4 elements (numbers) or Array of 6 elements (numbers) non-empty One or more bounding boxes that describe the spatial extent of the dataset. The first bounding box describes the overall spatial extent of the data. All subsequent bounding boxes describe more precise bounding boxes, e.g. to identify clusters of data. Clients only interested in the overall spatial extent will only need to access the first item in each array.
                - temporal (object; required) - The potential temporal extents of the features in the collection
                    - interval
            - stac_version (string; required) - The version of the STAC specification, which MAY not be equal to the STAC API version. Supports versions 0.9.x and 1.x.x.
            - stac_extensions (array of string; optional) - A list of implemented STAC extensions. The list contains URLs to the JSON Schema files it can be validated against. For STAC < 1.0.0-rc.1 shortcuts such as sar can be used instead of the schema URL.
            - links (required, array of objects) - Warning: this metadata is ignored by openeo-python-driver. Links related to this list of resources, for example links for pagination or alternative formats such as a human-readable HTML version
            - description (string; required) - Detailed multi-line description to explain the collection. CommonMark 0.29 syntax MAY be used for rich text representation.
            - license (string; required) - License(s) of the data as a SPDX License identifier

        source: https://openeo.org/documentation/1.0/developers/api/reference.html#tag/EO-Data-Discovery/operation/list-collections

        """
        logger.debug(f"Connecting to STAC service URL={STAC_URL}")

        all_collections = self.stac_client.get_collections()
        all_metadata = list()
        for collection_client in all_collections:
            openeo_coll_metadata = self._convert_collection_client_to_openeo(
                pystac_collection=collection_client
            )
            all_metadata.append(openeo_coll_metadata)
        logger.debug(all_metadata)
        return all_metadata

    def get_collection_metadata(self, collection_id: str) -> dict:
        """get collection metadata for the specified collection_id
        implements GET /collection/{collectionId} endpoint
        Args:
            collection_id (str): collection ID

        Returns:
            dict: _description_

        """
        logger.debug(f"GeoDNCollectionCatalog - Connecting to STAC service URL={STAC_URL}")
        logger.debug(f"GeoDNCollectionCatalog - Searching collection: {collection_id}")
        collection = self.stac_client.get_collection(collection_id=collection_id)
        openeo_collection = self._convert_collection_client_to_openeo(
            pystac_collection=collection, full=True
        )
        return openeo_collection

    def get_collection_items(self, collection_id: str, parameters: dict = {}) -> Dict[str, Any]:
        """
        Optional STAC API endpoint `GET /collections/{collectionId}/items`
        """
        # max number of items returned
        logger.debug(f"Connecting to STAC service URL={STAC_URL}")
        # collection = self.stac_client.get_collection(collection_id=collection_id)
        limit = int(parameters.get("limit", 100))
        bbox_str: Optional[str] = parameters.get("bbox")
        if bbox_str is not None:
            bbox = list(map(float, bbox_str.replace("[", " ").replace("]", " ").split(",")))
        else:
            bbox = None
        datetime_field = parameters.get("datetime")

        fields = {
            "include": [
                "id",
                "bbox",
                "datetime",
                "properties.cube:variables",
                "properties.cube:dimensions",
            ],
            "exclude": [],
        }
        collection_ids = [collection_id]
        logger.debug(
            f"Searching items: collections={collection_ids} bbox={bbox}\
                datetime={datetime_field} fields={fields} limit={limit}"
        )
        result = self.stac_client.search(
            collections=collection_ids,
            bbox=bbox,
            limit=limit,
            datetime=datetime_field,
            fields=fields,
        )
        items = list()

        for item in result.items():
            items.append(GeoDNCollectionCatalog._convert_item_client_to_openeo(pystac_item=item))
        response = {
            "type": "FeatureCollection",
            "features": items,
            "links": [{}],
            "timeStamp": datetime.now().isoformat(),
            "numberMatched": len(items),
            "numberReturned": len(items),
        }

        return response

    @staticmethod
    def _convert_item_client_to_openeo(pystac_item: Item) -> Dict[str, Any]:
        links = list()
        # jsonify each Link object
        for link in pystac_item.links:
            link_field = {
                "rel": str(link.rel) if link.rel is not None else link.rel,
                "href": link.get_href(),
                "type": str(link.media_type) if link.media_type else link.media_type,
            }
            if link.title is not None:
                link_field["title"] = link.title
            links.append(link_field)
        assets = dict()
        # jsonify each Asset object
        for k, v in pystac_item.assets.items():
            a = {
                "href": v.get_absolute_href(),
                "type": str(v.media_type) if v.media_type is not None else v.media_type,
                "roles": v.roles,
            }
            if v.title is not None:
                a["title"] = v.title
            assets[k] = a
        item = {
            "stac_version": GeoDNCollectionCatalog.STAC_VERSION,
            "stac_extensions": pystac_item.stac_extensions,
            "type": "Feature",
            "id": pystac_item.id,
            "bbox": pystac_item.bbox,
            "geometry": pystac_item.geometry,
            "properties": pystac_item.properties,
            "collection": pystac_item.collection_id,
            "links": links,
            "assets": assets,
        }
        return item

    def _convert_collection_client_to_openeo(
        self, pystac_collection: CollectionClient, full: bool = False
    ) -> Dict[str, Any]:
        """
        convert metadata from GeoDN.Discovery to OpenEO format

        Args:
            metadata_item (Dict[Any, Any]): metadata from a single GeoDN.Discovery dataset
            full (bool): if true, then full description of collection metadata must be returned

        Returns:
            Dict[Any, Any]: _description_

        """
        # get list of items associated with this collection
        # set collection
        collection_as_dict = {
            "id": pystac_collection.id,
            "extent": {
                "spatial": pystac_collection.extent.spatial.to_dict(),
                "temporal": pystac_collection.extent.temporal.to_dict(),
            },
            "description": pystac_collection.description,
            "title": pystac_collection.title,
            "license": pystac_collection.license,
            "stac_version": GeoDNCollectionCatalog.STAC_VERSION,
            "stac_extensions": pystac_collection.stac_extensions,
            "type": "Collection",
            "keywords": list(),
            # TODO legacy field? https://github.ibm.com/geodn-discovery/main/issues/146
            "version": "",
            "deprecated": False,
            "providers": [
                {
                    "name": "IBM",
                    "description": "Producers of awesome spatiotemporal assets",
                    "roles": ["producer", "processor"],
                    "url": "https://www.ibm.com",
                }
            ],
            "links": [link_field.to_dict() for link_field in pystac_collection.links],
        }
        if full:
            # feature_collection = self.get_collection_items(
            #     collection_id=collection_client.id, parameters=None
            # )
            # extract cube:dimensions from items' metadata
            summaries_dict = pystac_collection.summaries.to_dict()
            try:
                cube_dimensions_dict = summaries_dict["cube:dimensions"]
            except KeyError as e:
                msg = f"KeyError! summaries_dict={summaries_dict} - {e}"
                logger.error(msg)
                raise KeyError(msg)
            cube_dimensions = self._extract_cube_dimensions(cube_dimensions=cube_dimensions_dict)
            # extract cube:variables from items' metadata
            # cube_variables = self._extract_cube_variables(
            #     feature_collection=feature_collection, dimensions=cube_dimensions
            # )
            collection_as_dict["cube:dimensions"] = (
                GeoDNCollectionCatalog._export_cube_dimensions_group(cube_dimensions)
            )
            # collection_as_dict["cube:variables"] = (
            #     GeoDNCollectionCatalog._export_cube_variables(cube_variables),
            # )
        return collection_as_dict

    def _extract_cube_dimensions(self, cube_dimensions: Dict[str, Any]) -> Dict[str, Dimension]:
        """instantia Dimension objects based on cube:dimensions fields in dict format

        Args:
            cube_dimensions (Dict): cube:dimensions field

        Returns:
            Dict: Dimension objects grouped by description
        """
        cube_dimensions_list: List[Dimension] = list()
        for name, dimension in cube_dimensions.items():
            # description cannot be none

            if dimension["type"] == "bands":
                band_values = cube_dimensions["bands"]["values"]
                cube_dimensions_list.append(BandDimension(values=band_values, description=name))
            elif dimension["type"] == "spatial":
                axis = dimension["axis"]
                extent = dimension["extent"]
                cube_dimensions_list.append(
                    HorizontalSpatialDimension(axis=axis, extent=extent, description=name)
                )
            elif dimension["type"] == "temporal":
                extent = dimension["extent"]
                step = dimension.get("step")
                values = dimension.get("values")
                cube_dimensions_list.append(
                    TemporalDimension(extent=extent, step=step, description=name, values=values)
                )
        return cube_dimensions_list

    @staticmethod
    def _export_cube_dimensions_group(
        group_dimensions: List[Dimension],
    ) -> Dict[str, Dict[str, Any]]:
        """convert a dictionary of Dimension objects to a json-serializable dictionary

        Args:
            group_dimensions (Dict[str, Dimension]): dimensions grouped by description

        Returns:
            Dict[str, Dict[str, Any]]: json-serializable
        """
        cube_dimensions_collection: Dict[str, Dict[str, Any]] = dict()

        cube_dimensions_collection = dict()
        for dimension in group_dimensions:
            cube_dimensions_collection = cube_dimensions_collection | dimension.to_dict()
        return cube_dimensions_collection

    @staticmethod
    def _export_cube_variables(
        cube_variables: Dict[str, DataCubeVariable]
    ) -> Dict[str, Dict[str, Any]]:
        """_summary_

        Args:
            cube_variables (Dict[str, DataCubeVariable]): _description_

        Returns:
            Dict[str, Dict[str, Any]]: _description_
        """
        variables = dict()
        for description, cube_var in cube_variables.items():
            variables[description] = cube_var.to_dict()
        return variables

    def _extract_cube_variables(
        self, feature_collection: Dict, dimensions: Dict[str, Dimension]
    ) -> Dict[str, DataCubeVariable]:
        """generate cube:dimensions field using items' cube:dimensions

        {"2t": {"type": "data", "dimensions": ["x", "y", "time"]}, "tp": {"type": "data", "dimensions": ["x", "y", "time"]}}
        Args:
            feature_collection (Dict): response from STAC GET /collections/{coll}/items

        Returns:
            Dict: cube variables grouped by description
        """
        cube_variables: Dict[str, DataCubeVariable]
        cube_variables = dict()
        items: List[Dict[str, Any]] = feature_collection["features"]
        # for each item
        for item in items:
            properties = item["properties"]
            # get cube:variables metadata
            cube_var: Dict[str, Dict[str, Any]] = properties[GeoDNCollectionCatalog.CUBE_VARIABLES]
            for description, cube_var_metadata in cube_var.items():
                # variable has not been added
                if description not in cube_variables.keys():
                    # get dimension names
                    dimension_names: List[str] = cube_var_metadata["dimensions"]
                    dimension_list = list()
                    # create Dimension objects for each name
                    for dim_name in dimension_names:
                        dimension_list.append(dimensions[dim_name])
                    # create DataCubeVariable
                    cube_variables[description] = DataCubeVariable(
                        dimensions=dimension_list,
                        type=GeoDNCollectionCatalog.DATA,
                        description=description,
                        values=None,
                        extent=None,
                    )
        return cube_variables

    def _get_summaries(self, dataset_metadata: DatasetMetadata) -> Dict[str, Any]:
        """Summaries are either a unique set of all available values or statistics.
            Statistics by default only specify the range (minimum and maximum values),
            but can optionally be accompanied by additional statistical values.
            The range can specify the potential range of values, but it is recommended to be
            as precise as possible. The set of values MUST contain at least one element and it
            is strongly RECOMMENDED to list all values. It is recommended to list as many properties
            as reasonable so that consumers get a full overview of the Collection. Properties
            that are covered by the Collection specification (e.g. providers and license)
            SHOULD NOT be repeated in the summaries.
            https://openeo.org/documentation/1.0/developers/api/reference.html#tag/EO-Data-Discovery/operation/describe-collection

        Args:
            dataset_metadata (DatasetMetadata): _description_
            layer_metadata (List[LayerMetadata]): _description_

        Returns:
            Dict[str, Any]: summaries
        """
        summaries: Dict[str, Any]
        summaries = dict()
        summaries = {
            "level": {
                "max": dataset_metadata.level,
                "min": dataset_metadata.level,
            }
        }
        if dataset_metadata.spatial_resolution_of_raw_data is not None:
            # The nominal Ground Sample Distance for the data, as measured in meters on the ground.
            gsd = GeoDNCollectionCatalog._parse_gsd(
                distance=dataset_metadata.spatial_resolution_of_raw_data,
                level=dataset_metadata.level,
            )
            summaries["gsd"] = {"max": gsd, "min": gsd}
        if dataset_metadata.temporal_resolution is not None:
            # summaries["temporal_resolution"] = dataset_metadata.temporal_resolution
            temp_resolution_iso8601 = GeoDNCollectionCatalog._to_iso8601_duration(
                dataset_metadata.temporal_resolution
            )
            summaries["temporal_resolution"] = {
                "max": temp_resolution_iso8601,
                "min": temp_resolution_iso8601,
            }
        return summaries

    @staticmethod
    def _parse_gsd(distance: str, level: Optional[int] = None) -> str:
        """convert spatial resolution from GeoDN notation to ground sample distance in meters

        "In remote sensing, ground sample distance (GSD) in a digital photo (such as an orthophoto)
        of the ground from air or space is the distance between pixel centers measured on the
        ground. For example, in an image with a one-meter GSD, adjacent pixels image locations
        are 1 meter apart on the ground.[1] GSD is a measure of one limitation to spatial
        resolution or image resolution, that is, the limitation due to sampling.[2]" Wikipedia

        Args:
            distance (str): 250m

        Returns:
            str: _description_
        """
        km = "km"
        meter = "m"
        degree = "degrees"
        # aprox. size in meter of one degree at the equator
        one_degree_size_equator = 111000
        if distance.find(km) >= 0:
            gsd = distance.replace(km, "")
            gsd = float(gsd) * 1000
            return str(gsd)
        elif distance.find(meter) >= 0:
            gsd = distance.replace(meter, "").strip()
            return gsd
        elif distance.find(degree) >= 0 and level is not None:
            gsd = float(distance.replace(degree, "").strip())
            step = GeoDNCollectionCatalog._compute_step(level=level)
            gsd = str(one_degree_size_equator * step)
            return gsd
        return distance

    @staticmethod
    def _parse_raster_bands(
        dataset_metadata: DatasetMetadata, layer_metadata: List[LayerMetadata]
    ) -> List[Dict[str, Any]]:
        """adapt dataset and layers to the Raster bands extensions
        https://github.com/stac-extensions/raster

        Args:
            dataset_metadata (DatasetMetadata): _description_
            layer_metadata (List[LayerMetadata]): _description_

        Returns:
            List[Dict[str, Any]]: List of band metadata
        """
        raster_bands = list()
        for metadata in layer_metadata:
            band = {
                "name": metadata.name,
                "unit": metadata.unit,
                "data_type": metadata.data_type,
                "spatial_resolution": dataset_metadata.spatial_resolution_of_raw_data,
                "nodata": metadata.nodata,
            }
            raster_bands.append(band)
        return raster_bands

    @staticmethod
    def _generate_cube_variables(layer_metadata: List[LayerMetadata]) -> Dict[str, Any]:
        """generate cube variable field which should be normalized and compatible with STAC

        Args:
            dataset_metadata (DatasetMetadata): _description_
            layer_metadata (List[LayerMetadata]): _description_

        Returns:
            Dict[str, Any]: _description_
        """
        var_type = "data"
        # TODO set var type dynamically
        assert var_type in ["data", "auxiliary"]
        # TODO set dimensions dynamically
        dimensions = [
            DEFAULT_X_DIMENSION,
            DEFAULT_Y_DIMENSION,
            DEFAULT_TIME_DIMENSION,
        ]
        cube_vars = dict()
        for layer in layer_metadata:
            # mapping layer's name to band ID
            cube_vars[layer.band] = {
                "dimensions": dimensions,
                "type": var_type,
                "description": layer.description_short,
                "unit": layer.unit,
            }
        return cube_vars

    @staticmethod
    def _compute_step(level: int) -> float:
        """compute GeoDN step using specified level

        https://pairs.res.ibm.com/tutorial/tutorials/api/v01x/raster_data.html?highlight=level

        Args:
            level (int): GeoDN level

        Returns:
            float: step in degrees
        """
        step = (10**-6) * (2 ** (29 - level))
        return step

    @staticmethod
    def _generate_cube_dimensions(
        dataset_metadata: DatasetMetadata, layer_metadata: List[LayerMetadata]
    ) -> Dict[Any, Any]:
        """generate cube:dimensions metadata based on datacube spec
        https://github.com/stac-extensions/datacube

        Args:
            dataset_metadata (DatasetMetadata): dataset metadata
            layer_metadata (List[LayerMetadata]): list of layer metadata

        Returns:
            Dict[Any, Any]: cube dimensions
        """
        layer_names = list()
        for layer in layer_metadata:
            layer_names.append(layer.name)
        step = GeoDNCollectionCatalog._compute_step(level=dataset_metadata.level)
        cube_dimensions = {
            DEFAULT_X_DIMENSION: {
                "type": "spatial",
                "axis": DEFAULT_X_DIMENSION,
                "extent": [
                    dataset_metadata.longitude_min,
                    dataset_metadata.longitude_max,
                ],
                "step": step,
                "reference_system": dataset_metadata.crs,
            },
            DEFAULT_Y_DIMENSION: {
                "type": "spatial",
                "axis": DEFAULT_Y_DIMENSION,
                "extent": [
                    dataset_metadata.latitude_min,
                    dataset_metadata.latitude_max,
                ],
                "step": step,
                "reference_system": dataset_metadata.crs,
            },
            DEFAULT_TIME_DIMENSION: {
                "type": "temporal",
                "axis": DEFAULT_TIME_DIMENSION,
                "extent": [
                    (
                        dataset_metadata.temporal_min.isoformat()
                        if dataset_metadata.temporal_min is not None
                        else None
                    ),
                    (
                        dataset_metadata.temporal_max.isoformat()
                        if dataset_metadata.temporal_max is not None
                        else None
                    ),
                ],
                "step": GeoDNCollectionCatalog._to_iso8601_duration(
                    dataset_metadata.temporal_resolution
                ),
            },
            "bands": {
                "type": "bands",
                "values": layer_names,
            },
        }
        return cube_dimensions

    @staticmethod
    def _to_iso8601_duration(temporal_resolution: Union[str, None]) -> Union[str, None]:
        """
        the space between the temporal instances as ISO 8601 duration, e.g. P1D.
        Use null for irregularly spaced steps.

        https://github.com/stac-extensions/datacube#temporal-dimension-object

        Args:
            temporal_resolution (str): temporal resolution as specified in GeoDN, e.g., 0 years 0 mons 0 days 0 hours 0 mins 86400.00 secs

        Returns:
            str: temporal resolution according to ISO8601
        """
        duration = None
        if temporal_resolution is not None and isinstance(temporal_resolution, str):
            mins_pattern = "mins"
            secs_pattern = "secs"
            mins_index = temporal_resolution.find(mins_pattern)
            secs_index = temporal_resolution.find(secs_pattern)
            if 0 < mins_index < secs_index:
                total_secs = float(
                    temporal_resolution[
                        mins_index + len(mins_pattern) : secs_index  # noqa: ignore E203
                    ]
                )
                delta = pd.Timedelta(total_secs, unit="seconds")
                duration = delta.isoformat()
        return duration

    @staticmethod
    def _parse_license(dataset_metadata: DatasetMetadata) -> str:
        if dataset_metadata.license is not None:
            return dataset_metadata.license
        else:
            return "Unknown"

    @staticmethod
    def _parse_description(dataset_metadata: DatasetMetadata) -> str:
        """convert description from GeoDN to STAC/OpenEO

        Args:
            dataset_metadata (DatasetMetadata):

        Returns:
            str: description
        """
        if dataset_metadata.description_short is not None:
            return dataset_metadata.description_short
        else:
            return dataset_metadata.name

    @staticmethod
    def _parse_temporal_extent(dataset_metadata: DatasetMetadata) -> List[datetime]:
        """get temporal extent

        Args:
            dataset_metadata (Dict[str, str]): _description_

        Returns:
            List[datetime]: temporal min, temporal max

        """
        temporal_extent = list()
        if dataset_metadata.temporal_min is not None:
            temporal_extent.append(dataset_metadata.temporal_min)
        if dataset_metadata.temporal_max is not None:
            temporal_extent.append(dataset_metadata.temporal_max)
        return temporal_extent

    @staticmethod
    def _parse_bbox(metadata: DatasetMetadata) -> List[float]:
        """parse bbox from metadata item, if metadata exists

        Args:
            metadata_item (Dict[str, float]): _description_

        Returns:
            List[float]: longitude min, latitude min, longitude max, latitude max

        """
        # list of keys that store the coordinates and default values if keys are not found
        bbox = [
            metadata.longitude_min,
            metadata.latitude_min,
            metadata.longitude_max,
            metadata.latitude_max,
        ]

        return bbox

    def load_collection(
        self, collection_id: str, load_params: LoadParameters, env: EvalEnv
    ) -> GeoDNDataCube:
        """retrieve data from data provider

        Args:
            collection_id (str): collection (dataset) unique identifier
            load_params (LoadParameters): contains spatial and temporal extents
            env (EvalEnv): _description_

        Returns:
            GeoDNDataCube: _description_
        """
        logger.debug(f"collection_id={collection_id}")
        metadata = self.get_collection_metadata(collection_id=collection_id)
        start_time_str, end_time_str = load_params.temporal_extent
        try:
            starttime = pd.Timestamp(start_time_str).to_pydatetime()
        except ValueError:
            starttime = None
        try:
            endtime = pd.Timestamp(end_time_str).to_pydatetime()
        except ValueError:
            endtime = None
        spatial_extent = load_params.spatial_extent
        level = metadata["summaries"]["level"]
        latmax = float(spatial_extent.get("north"))
        latmin = float(spatial_extent.get("south"))
        lonmax = float(spatial_extent.get("east"))
        lonmin = float(spatial_extent.get("west"))
        bands = load_params.bands
        arrays = list()
        for band in bands:
            logger.debug(f"Getting global timestamps from {GEODN_DATASERVICE_ENDPOINT} band={band}")
            # find available global timestamps
            # TODO can we search available timestamps of the specified area?
            timestamps = query.get_global_timestamps(
                layer_id=band,
                starttime=starttime,
                endtime=endtime,
                dataserviceendpoint=GEODN_DATASERVICE_ENDPOINT,
                username=GEODN_DATASERVICE_USER,
                password=GEODN_DATASERVICE_PASSWORD,
            )
            # retrieve data from GeoDN.Discovery and load it as xr.DataArray
            data_array = query.to_xarray(
                layer_id=band,
                latmax=latmax,
                latmin=latmin,
                lonmax=lonmax,
                lonmin=lonmin,
                timestamps=timestamps,
                level=level,
                dataserviceendpoint=GEODN_DATASERVICE_ENDPOINT,
                username=GEODN_DATASERVICE_USER,
                password=GEODN_DATASERVICE_PASSWORD,
            )
            arrays.append(data_array)
            # dset = data_array.to_dataset()
        dset = xr.merge(arrays)
        return GeoDNDataCube(metadata=metadata, data=dset)
