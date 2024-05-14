import os
from typing import Any, Dict, List, Optional

from pystac_client import Client, CollectionClient
from pystac import Item
from openeo_driver.backend import CollectionCatalog
from tensorlakehouse_openeo_driver.constants import (
    GEODN_DISCOVERY_USERNAME,
    GEODN_DISCOVERY_PASSWORD,
    STAC_URL,
    DEFAULT_X_DIMENSION,
    DEFAULT_Y_DIMENSION,
)
from tensorlakehouse_openeo_driver.geodn_discovery import GeoDNDiscovery
from datetime import datetime
import logging
from tensorlakehouse_openeo_driver.model.datacube_variable import DataCubeVariable

from tensorlakehouse_openeo_driver.model.dimension import (
    BandDimension,
    Dimension,
    HorizontalSpatialDimension,
    TemporalDimension,
    VerticalSpatialDimension,
)

assert os.path.isfile("logging.conf")
logging.config.fileConfig(fname="logging.conf", disable_existing_loggers=False)
logger = logging.getLogger("geodnLogger")


class TensorLakehouseCollectionCatalog(CollectionCatalog):
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
        logger.debug(
            f"TensorLakehouseCollectionCatalog - Connecting to STAC service URL={STAC_URL}"
        )
        logger.debug(
            f"TensorLakehouseCollectionCatalog - Searching collection: {collection_id}"
        )
        collection = self.stac_client.get_collection(collection_id=collection_id)
        openeo_collection = self._convert_collection_client_to_openeo(
            pystac_collection=collection, full=True
        )
        return openeo_collection

    def get_collection_items(
        self, collection_id: str, parameters: dict = {}
    ) -> Dict[str, Any]:
        """
        Optional STAC API endpoint `GET /collections/{collectionId}/items`
        """
        # max number of items returned
        logger.debug(f"Connecting to STAC service URL={STAC_URL}")
        # collection = self.stac_client.get_collection(collection_id=collection_id)
        limit = int(parameters.get("limit", 100))
        bbox_str: Optional[str] = parameters.get("bbox")
        if bbox_str is not None:
            bbox = list(
                map(float, bbox_str.replace("[", " ").replace("]", " ").split(","))
            )
        else:
            bbox = None
        datetime_field = parameters.get("datetime")

        fields = {
            "includes": [
                "id",
                "bbox",
                "properties.cube:variables",
                "properties.cube:dimensions",
                "properties.datetime",
            ],
            "excludes": [],
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
            items.append(
                TensorLakehouseCollectionCatalog._convert_item_client_to_openeo(
                    pystac_item=item
                )
            )
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
            "stac_version": TensorLakehouseCollectionCatalog.STAC_VERSION,
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
            "stac_version": TensorLakehouseCollectionCatalog.STAC_VERSION,
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
            extra_fields = pystac_collection.extra_fields
            try:
                cube_dimensions_dict: Dict[str, Any] = extra_fields["cube:dimensions"]
            except KeyError as e:
                msg = f"KeyError! extra_fields={extra_fields} - {e}"
                logger.error(msg)
                raise KeyError(msg)
            cube_dimensions = self._extract_cube_dimensions(
                cube_dimensions=cube_dimensions_dict
            )
            collection_as_dict["cube:dimensions"] = (
                TensorLakehouseCollectionCatalog._export_cube_dimensions_group(
                    cube_dimensions
                )
            )
        return collection_as_dict

    def _extract_cube_dimensions(
        self, cube_dimensions: Dict[str, Any]
    ) -> List[Dimension]:
        """instantia Dimension objects based on cube:dimensions fields in dict format

        Args:
            cube_dimensions (Dict): cube:dimensions field

        Returns:
            Dict: Dimension objects grouped by description
        """
        assert isinstance(cube_dimensions, dict)
        cube_dimensions_list: List[Dimension] = list()
        for name, dimension in cube_dimensions.items():
            # description cannot be none

            if dimension["type"] == "bands":
                band_values = cube_dimensions["bands"]["values"]
                cube_dimensions_list.append(
                    BandDimension(values=band_values, description=name)
                )
            elif dimension["type"] == "spatial":
                # type, axis and extent keys are required by datacube extension
                # https://github.com/stac-extensions/datacube/tree/main?tab=readme-ov-file#horizontal-spatial-raster-dimension-object
                axis = dimension["axis"]
                if axis.lower() in [DEFAULT_X_DIMENSION, DEFAULT_Y_DIMENSION]:
                    extent = dimension["extent"]
                    cube_dimensions_list.append(
                        HorizontalSpatialDimension(
                            axis=axis, extent=extent, description=name
                        )
                    )
                else:
                    if "extent" in dimension:
                        extent = dimension["extent"]
                    elif "values" in dimension:
                        extent = [min(dimension["values"]), max(dimension["values"])]
                    else:
                        raise Exception(f"Error! Missing extent: {dimension}")
                    cube_dimensions_list.append(
                        VerticalSpatialDimension(
                            axis=axis, extent=extent, description=name
                        )
                    )
            elif dimension["type"] == "temporal":
                extent = dimension["extent"]
                step = dimension.get("step")
                values = dimension.get("values")
                cube_dimensions_list.append(
                    TemporalDimension(
                        extent=extent, step=step, description=name, values=values
                    )
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
            cube_dimensions_collection = (
                cube_dimensions_collection | dimension.to_dict()
            )
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


