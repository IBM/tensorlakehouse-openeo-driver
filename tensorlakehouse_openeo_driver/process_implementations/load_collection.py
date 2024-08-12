from abc import ABC, abstractmethod
from collections import defaultdict
from datetime import datetime
from typing import Any, DefaultDict, Dict, List, Optional, Tuple, Union
from openeo_pg_parser_networkx.pg_schema import (
    BoundingBox,
    TemporalInterval,
)
from pystac_client import Client
import xarray as xr
from tensorlakehouse_openeo_driver.constants import (
    COG_MEDIA_TYPE,
    GRIB2_MEDIA_TYPE,
    JPG2000_MEDIA_TYPE,
    NETCDF_MEDIA_TYPE,
    STAC_DATETIME_FORMAT,
    STAC_URL,
    ZIP_ZARR_MEDIA_TYPE,
    FSTD_MEDIA_TYPE,
    logger,
)
import pandas as pd
import pyproj
from pyproj import Transformer
from tensorlakehouse_openeo_driver.file_reader.cog_file_reader import COGFileReader
from tensorlakehouse_openeo_driver.file_reader.netcdf_file_reader import (
    NetCDFFileReader,
)
from tensorlakehouse_openeo_driver.file_reader.standard_file_reader import (
    FSTDFileReader,
)
from tensorlakehouse_openeo_driver.file_reader.zarr_file_reader import ZarrFileReader
from tensorlakehouse_openeo_driver.file_reader.grib2_file_reader import Grib2FileReader

# from tensorlakehouse_openeo_driver.file_reader.standard_file_reader import (
#     FSTDFileReader,
# )
from openeo_pg_parser_networkx.pg_schema import ParameterReference


class AbstractLoadCollection(ABC):
    @abstractmethod
    def load_collection(
        self,
        id: str,
        spatial_extent: BoundingBox,
        temporal_extent: TemporalInterval,
        bands: List[str],
        dimensions: Dict[str, str],
        properties=None,
    ) -> xr.DataArray:
        raise NotImplementedError()

    @staticmethod
    def _to_epsg4326(
        latmax: float, latmin: float, lonmax: float, lonmin: float, crs_from: pyproj.CRS
    ) -> Tuple[float, float, float, float]:
        """convert

        Args:
            latmax (float): _description_
            latmin (float): _description_
            lonmax (float): _description_
            lonmin (float): _description_
            crs_from (pyproj.CRS): _description_

        Returns:
            _type_: _description_
        """
        epsg4326 = pyproj.CRS.from_epsg(4326)
        transformer = Transformer.from_crs(
            crs_from=crs_from, crs_to=epsg4326, always_xy=True
        )
        east, north = transformer.transform(lonmax, latmax)
        west, south = transformer.transform(lonmin, latmin)
        return west, south, east, north


class LoadCollectionFromCOS(AbstractLoadCollection):
    # some items have "data" as asset description instead of band name (e.g., "B02")
    ASSET_DESCRIPTION_DATA = "data"

    def load_collection(
        self,
        id: str,
        spatial_extent: BoundingBox,
        temporal_extent: TemporalInterval,
        bands: List[str],
        dimensions: Dict[str, str],
        properties: Dict[str, Any] = {},
    ) -> xr.DataArray:
        logger.debug(f"load collection from COS: id={id} bands={bands}")
        bbox_wsg84 = LoadCollectionFromCOS._convert_to_WSG84(
            spatial_extent=spatial_extent
        )
        # convert TemporalInterval to Tuple[datetime, Optional[datetime]]
        start: datetime = pd.Timestamp(temporal_extent.start.to_numpy()).to_pydatetime()
        if temporal_extent.end is not None:
            end: Optional[datetime] = pd.Timestamp(
                temporal_extent.end.to_numpy()
            ).to_pydatetime()
        else:
            end = None
        temporal_ext = (start, end)
        item_search = self._search_items(
            bbox=bbox_wsg84,
            temporal_extent=temporal_extent,
            collection_id=id,
            properties=properties,
        )
        items_by_media_type = LoadCollectionFromCOS._group_items_by_media_type(
            items=item_search, bands=bands
        )
        assert (
            len(items_by_media_type) == 1
        ), "Error! Current implementation supports only loading items that have the same media\
                  type. For instance, if some of the selected items are associated with COG files \
                      and other with parquet files, it will raise an exception"

        media_type = next(iter(items_by_media_type.keys()))
        items = next(iter(items_by_media_type.values()))
        if media_type in [COG_MEDIA_TYPE, JPG2000_MEDIA_TYPE]:
            reader: Union[
                COGFileReader,
                ZarrFileReader,
                NetCDFFileReader,
                Grib2FileReader,
                FSTDFileReader,
            ] = COGFileReader(
                items=items,
                bbox=bbox_wsg84,
                bands=bands,
                temporal_extent=temporal_ext,
                dimension_map=None,
            )

        elif media_type == ZIP_ZARR_MEDIA_TYPE:
            reader = ZarrFileReader(
                items=items,
                bbox=bbox_wsg84,
                bands=bands,
                temporal_extent=temporal_ext,
                dimension_map=None,
            )
        elif media_type == NETCDF_MEDIA_TYPE:
            reader = NetCDFFileReader(
                items=items,
                bbox=bbox_wsg84,
                bands=bands,
                temporal_extent=temporal_ext,
                dimension_map=None,
            )
        elif media_type == GRIB2_MEDIA_TYPE:
            reader = Grib2FileReader(
                items=items,
                bbox=bbox_wsg84,
                bands=bands,
                temporal_extent=temporal_ext,
                dimension_map=None,
            )
        elif media_type == FSTD_MEDIA_TYPE:
            reader = FSTDFileReader(
                items=items,
                bbox=bbox_wsg84,
                bands=bands,
                temporal_extent=temporal_ext,
                dimension_map=None,
            )
        else:
            raise ValueError(f"Error! {media_type=} is not supported")
        data = reader.load_items()
        return data

    @staticmethod
    def _parse_process_graph(process_graph: Dict) -> Tuple:
        """parses process graph, which is part of properties parameter of load_collection process

        Args:
            process_graph (Dict):

        Returns:
            Tuple: _description_
        """
        map_openeo_cql2_operators = {
            "lte": "<=",
            "lt": "<",
            "gte": ">=",
            "gt": ">",
            "eq": "=",
        }
        x = process_graph["arguments"]["x"]
        y = process_graph["arguments"]["y"]
        if isinstance(x, ParameterReference):
            value = y
        else:
            value = x
        process_id: str = process_graph["process_id"]
        assert isinstance(process_id, str)
        operator = map_openeo_cql2_operators[process_id]
        return operator, value

    @staticmethod
    def _convert_properties_to_filter(
        properties: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """convert properties parameter of load_collection process to a filter parameter of
        pystac_client search method

        Args:
            properties (Dict[str, Any]): properties parameter

        Returns:
            Dict[str, Any]: filter parameter
        """
        # TODO this method does not handle all types of filters that can be applied

        # this is the list of conditions/filters that we will pass as filters in the search
        conditions = list()
        # for each property
        for property_name, process_graph in properties.items():
            # for each process graph
            for process_graph_value in process_graph["process_graph"].values():
                # extract operator and value
                operator, value = LoadCollectionFromCOS._parse_process_graph(
                    process_graph=process_graph_value
                )
                # set a condition and append it to the list of conditions
                condition = {
                    "op": operator,
                    "args": [
                        {"property": f"properties.{property_name}"},
                        value,
                    ],
                }
                conditions.append(condition)
        # if the number of conditions appended is zero then there is no filter
        if len(conditions) == 0:
            filter_cql = None
        # if the number of conditions is 1, then pop it from the list
        elif len(conditions) == 1:
            filter_cql = conditions.pop()
        else:
            # if the number of conditions is >= 2, then add 'and' operator
            filter_cql = {"op": "and", "args": conditions}
        return filter_cql

    def _search_items(
        self,
        bbox: Tuple[float, float, float, float],
        temporal_extent: TemporalInterval,
        collection_id: str,
        properties: Dict[str, Any] = {},
        limit: int = 10000,
    ) -> List[Dict[str, Any]]:
        starttime, endtime = LoadCollectionFromCOS._get_start_and_endtime(
            temporal_extent=temporal_extent
        )
        # set datetime using STAC format
        datetime = f"{starttime.strftime(STAC_DATETIME_FORMAT)}/{endtime.strftime(STAC_DATETIME_FORMAT)}"
        logger.debug(f"Connecting to STAC service URL={STAC_URL}")
        stac_catalog = Client.open(STAC_URL)
        filter_cql = LoadCollectionFromCOS._convert_properties_to_filter(
            properties=properties
        )
        fields: Dict[str, List[str]] = {
            "includes": [
                "id",
                "bbox",
                "properties.cube:variables",
                "properties.cube:dimensions",
                "properties.datetime",
            ],
            "excludes": [],
        }

        logger.debug(
            f"Searching STAC items: {bbox=} {datetime=} collections={[collection_id]}\
                  {fields=} {limit=} {filter_cql=}"
        )
        # search items
        result = stac_catalog.search(
            collections=[collection_id],
            bbox=bbox,
            datetime=datetime,
            fields=fields,
            limit=limit,
            filter=filter_cql,
            filter_lang="cql2-json",
        )
        items_as_dicts = list(result.items_as_dicts())
        matched_items = len(items_as_dicts)
        logger.debug(f"{matched_items} items have been found")
        assert (
            matched_items > 0
        ), f"Error! No item has been found, please check the params:\
                collection_id={collection_id} {bbox=} {datetime=} {limit=}\
                {fields=}"

        return items_as_dicts

    @staticmethod
    def _group_items_by_media_type(
        items: List[Dict[str, Any]],
        bands: List[str],
    ) -> Dict[str, List[Dict[str, Any]]]:
        """group items by media type as it defines a method for load files

        Args:
            items (List[Dict[str, Any]]): STAC items
            bands (List[str]): band names

        Returns:
            Dict[str, List[Dict[str, Any]]]: items grouped by media type
        """
        items_by_media_type: DefaultDict[str, List[Dict[str, Any]]] = defaultdict(list)
        for item in items:

            item_properties = item["properties"]
            # get list of available bands, which are stored as cube:variables
            available_bands = list(item_properties["cube:variables"].keys())
            for band in bands:
                if band in available_bands:
                    assets: Dict[str, Any] = item["assets"]
                    # if "data" is the key
                    if LoadCollectionFromCOS.ASSET_DESCRIPTION_DATA in assets.keys():
                        asset = assets[LoadCollectionFromCOS.ASSET_DESCRIPTION_DATA]
                    # if band name is the key
                    elif band in assets.keys():
                        asset = assets[band]
                    else:
                        continue
                    media_type = asset["type"]

                    # create dict entry for a media type composed by a list of items and item:properties
                    items_by_media_type[media_type].append(item)
        return items_by_media_type

    @staticmethod
    def _get_start_and_endtime(
        temporal_extent: TemporalInterval,
    ) -> Tuple[datetime, datetime]:
        """extract start and endtime from TemporalInterval

        Args:
            temporal_extent (TemporalInterval): _description_

        Raises:
            NotImplementedError: _description_
            an: _description_
            ValueError: _description_

        Returns:
            Tuple[datetime, datetime]: start, end
        """
        logger.debug(
            f"Converting datetime start={temporal_extent.start} end={temporal_extent.end}"
        )
        starttime = pd.Timestamp(temporal_extent.start.to_numpy()).to_pydatetime()
        endtime = pd.Timestamp(temporal_extent.end.to_numpy()).to_pydatetime()
        assert starttime <= endtime, f"Error! start > end: {starttime} > {endtime}"
        return starttime, endtime

    @staticmethod
    def _convert_to_WSG84(
        spatial_extent: BoundingBox,
    ) -> Tuple[float, float, float, float]:
        """convert to WSG84 if necessary, because STAC search supports only WSG84 CRS

        https://api.stacspec.org/v1.0.0-beta.3/item-search/#tag/Item-Search/operation/getItemSearch

        Args:
            spatial_extent (BoundingBox): bounding box

        Returns:
            Tuple[float, float, float, float]: min lon, min lat, max lon, max lat
        """
        # get original crs
        pyproj_crs = pyproj.CRS.from_string(spatial_extent.crs)
        # required projection to search on STAC
        epsg4326 = pyproj.CRS.from_epsg(4326)
        # get bounding box
        lonmin, latmin, lonmax, latmax = (
            spatial_extent.west,
            spatial_extent.south,
            spatial_extent.east,
            spatial_extent.north,
        )
        # if original projection is different than epsg4326, convert to it
        if pyproj_crs != epsg4326:
            lonmin, latmin, lonmax, latmax = AbstractLoadCollection._to_epsg4326(
                latmax=latmax,
                latmin=latmin,
                lonmax=lonmax,
                lonmin=lonmin,
                crs_from=pyproj_crs,
            )
        return lonmin, latmin, lonmax, latmax
