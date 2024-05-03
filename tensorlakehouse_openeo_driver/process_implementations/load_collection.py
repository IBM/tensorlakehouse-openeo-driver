from abc import ABC, abstractmethod
from collections import defaultdict
from datetime import datetime
from typing import Any, DefaultDict, Dict, List, Tuple
from openeo_pg_parser_networkx.pg_schema import (
    BoundingBox,
    TemporalInterval,
)
from pystac_client import Client
import xarray as xr
import dataservice.dask
import dataservice.query
from tensorlakehouse_openeo_driver.constants import (
    DEFAULT_BANDS_DIMENSION,
    COG_MEDIA_TYPE,
    GEODN_DATASERVICE_ENDPOINT,
    GEODN_DATASERVICE_PASSWORD,
    GEODN_DATASERVICE_USER,
    GEODN_DISCOVERY_PASSWORD,
    GEODN_DISCOVERY_CRS,
    GEODN_DISCOVERY_USERNAME,
    JPG2000_MEDIA_TYPE,
    STAC_DATETIME_FORMAT,
    STAC_URL,
    DEFAULT_TIME_DIMENSION,
    DEFAULT_X_DIMENSION,
    DEFAULT_Y_DIMENSION,
    logger,
)
import pandas as pd
import pyproj
from pyproj import Transformer
from tensorlakehouse_openeo_driver.geodn_discovery import GeoDNDiscovery
from tensorlakehouse_openeo_driver.layer import LayerMetadata
from tensorlakehouse_openeo_driver.s3_connections.cog_file_reader import COGFileReader


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
        transformer = Transformer.from_crs(crs_from=crs_from, crs_to=epsg4326, always_xy=True)
        east, north = transformer.transform(lonmax, latmax)
        west, south = transformer.transform(lonmin, latmin)
        return west, south, east, north


class LoadCollectionFromHBase(AbstractLoadCollection):
    def __init__(self) -> None:
        super().__init__()
        self.discovery = GeoDNDiscovery(
            client_id=GEODN_DISCOVERY_USERNAME, password=GEODN_DISCOVERY_PASSWORD
        )

    def load_collection(
        self,
        id: str,
        spatial_extent: BoundingBox,
        temporal_extent: TemporalInterval,
        bands: List[str],
        dimensions: Dict[str, str],
        properties=None,
    ) -> xr.DataArray:
        """retrieves data from GeoDN.Discovery based on specified spatial and temporal extent

        Args:
            id (str): collection ID
            spatial_extent (BoundingBox): _description_
            temporal_extent (TemporalInterval): _description_
            bands (Optional[List[str]]): _description_
            properties (_type_, optional): _description_. Defaults to None.

        Raises:
            NotImplementedError: _description_

        Returns:
            xr.DataArray: a data cube which has x, y, bands dimensions and optionally t dimension
        """
        logger.debug(f"Loading collection via dataservice: collection_id={id} bands={bands}")
        # TODO temporary fix because the openeo_pg_parser_networkx converts str to float
        #  for instance, a "9" str is converted to 9.0 float
        if isinstance(id, float):
            id = str(int(id))
        elif isinstance(id, int):
            id = str(id)
        # get collection metadata
        layer_metadata_list = self.get_geodn_layer_metadata(collection_id=id, bands=bands)
        assert (
            len(layer_metadata_list) > 0
        ), f"Error! PAIRS layers have not been found: collection_id={id} bands={bands}"
        # extract coordinates from BoundingBox object
        lonmin, latmin, lonmax, latmax = LoadCollectionFromHBase._get_bounding_box(
            spatial_extent=spatial_extent
        )
        # convert temporal extent to datetime
        starttime = pd.Timestamp(temporal_extent.start.to_numpy()).to_pydatetime()
        endtime = pd.Timestamp(temporal_extent.end.to_numpy()).to_pydatetime()

        logger.debug(f"Getting global timestamps from {GEODN_DATASERVICE_ENDPOINT}")
        arrays = list()
        # for each band, download and create xr.DataArray
        for layer_metadata in layer_metadata_list:
            # find available global timestamps
            logger.debug(f"Getting global timestamps from {GEODN_DATASERVICE_ENDPOINT}")
            # TODO can we search available timestamps of the specified area?
            timestamps = dataservice.query.get_global_timestamps(
                layer_id=layer_metadata.layer_id,
                starttime=starttime,
                endtime=endtime,
                dataserviceendpoint=GEODN_DATASERVICE_ENDPOINT,
                username=GEODN_DATASERVICE_USER,
                password=GEODN_DATASERVICE_PASSWORD,
                count=None,
            )
            assert (
                len(timestamps) > 0
            ), f"Error! no available timestamp between {starttime} and {endtime} for layer {layer_metadata.layer_id}"
            # retrieve data from GeoDN.Discovery and load it as xr.DataArray
            data_array = dataservice.dask.to_xarray(
                layer_id=layer_metadata.layer_id,
                latmax=latmax,
                latmin=latmin,
                lonmax=lonmax,
                lonmin=lonmin,
                timestamps=timestamps,
                level=layer_metadata.level,
                dataserviceendpoint=GEODN_DATASERVICE_ENDPOINT,
                username=GEODN_DATASERVICE_USER,
                password=GEODN_DATASERVICE_PASSWORD,
            )
            arrays.append(data_array)
        # create a xr.Dataset merging xr.DataArray of all bands
        da: xr.DataArray
        da = xr.concat(arrays, pd.Index(bands, name=DEFAULT_BANDS_DIMENSION))
        # dataservice_sdk returns a xarray.DataArray that has dimensions 'lat', 'lon' and 'time'
        dimension_name = {
            "lat": dimensions[DEFAULT_Y_DIMENSION],
            "lon": dimensions[DEFAULT_X_DIMENSION],
            "time": dimensions[DEFAULT_TIME_DIMENSION],
        }

        da = da.rename(dimension_name)
        da.rio.write_crs(GEODN_DISCOVERY_CRS, inplace=True)

        return da

    @staticmethod
    def _get_bounding_box(
        spatial_extent: BoundingBox,
    ) -> Tuple[float, float, float, float]:
        """get bounds

        Args:
            spatial_extent (BoundingBox): _description_

        Returns:
            Tuple[float, float, float, float]: west, south, east, north
        """

        latmax = spatial_extent.north
        latmin = spatial_extent.south
        lonmax = spatial_extent.east
        lonmin = spatial_extent.west
        pyproj_crs = pyproj.CRS.from_string(spatial_extent.crs)
        epsg4326 = pyproj.CRS.from_epsg(4326)

        if pyproj_crs != epsg4326:
            lonmin, latmin, lonmax, latmax = AbstractLoadCollection._to_epsg4326(
                latmax=latmax,
                latmin=latmin,
                lonmax=lonmax,
                lonmin=lonmin,
                crs_from=pyproj_crs,
            )
        assert 90 >= latmax >= latmin >= -90, f"Error! latmax < latmin: {latmax} < {latmin}"

        assert 180 >= lonmax >= lonmin >= -180, f"Error! lonmax < lonmin: {lonmax} < {lonmin}"
        return lonmin, latmin, lonmax, latmax

    @staticmethod
    def _are_the_same(dataset: str, collection: str) -> bool:
        if dataset.lower() == collection.lower():
            return True
        else:
            dataset = dataset.replace(" ", "-").replace("(", "").replace(")", "")
            if dataset.lower() == collection.lower():
                return True

        return False

    def get_geodn_layer_metadata(self, collection_id: str, bands: List[str]) -> List[LayerMetadata]:
        """get GeoDN Discovery layer metadata using collection_id and bands
        mapping between dataset and collection
        Args:
            collection_id (str): _description_
            bands (List[str]): _description_

        Returns:
            List[LayerMetadata]: list of objects that correspond to the specified bands
        """
        dataset_metadata_list = self.discovery.list_datasets()
        filtered_layer_metadata = list()
        # initialize variables to find dataset
        i = 0
        found_dset = False
        while i < len(dataset_metadata_list) and not found_dset:
            dset_metadata = dataset_metadata_list[i]
            i += 1
            # if dataset is found
            if LoadCollectionFromHBase._are_the_same(
                dataset=dset_metadata.collection_id, collection=collection_id
            ):
                # if dset_metadata.collection_id == collection_id:
                # list layers
                all_layer_metadata_list = self.discovery.list_datalayers_from_dataset(
                    dataset_id=dset_metadata.dataset_id, level=dset_metadata.level
                )
                # if bands parameters is None or empty, append all layers
                if bands is None or (isinstance(bands, list) and len(bands) == 0):
                    filtered_layer_metadata.extend(all_layer_metadata_list)
                else:
                    # initialize variables and find corresponding layers
                    for band in bands:
                        j = 0
                        found_layer = False
                        while j < len(all_layer_metadata_list) and not found_layer:
                            layer_metadata = all_layer_metadata_list[j]
                            j += 1
                            if band == layer_metadata.band:
                                filtered_layer_metadata.append(layer_metadata)
                                found_layer = True
                found_dset = True
        return filtered_layer_metadata


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
        properties=None,
    ) -> xr.DataArray:
        logger.debug(f"load collection from COS: id={id} bands={bands}")
        bbox_wsg84 = LoadCollectionFromCOS._convert_to_WSG84(spatial_extent=spatial_extent)
        item_search = self._search_items(
            bbox=bbox_wsg84,
            temporal_extent=temporal_extent,
            collection_id=id,
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
            cog_file_reader = COGFileReader(items=items, bbox=bbox_wsg84, bands=bands)
            data = cog_file_reader.load_items()

        return data

    def _search_items(
        self,
        bbox: Tuple[float, float, float, float],
        temporal_extent: TemporalInterval,
        collection_id: str,
        limit: int = 10000,
    ) -> List[Dict[str, Any]]:
        starttime, endtime = LoadCollectionFromCOS._get_start_and_endtime(
            temporal_extent=temporal_extent
        )
        # set datetime using STAC format
        datetime = (
            f"{starttime.strftime(STAC_DATETIME_FORMAT)}/{endtime.strftime(STAC_DATETIME_FORMAT)}"
        )
        logger.debug(f"Connecting to STAC service URL={STAC_URL}")
        stac_catalog = Client.open(STAC_URL)

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
            f"Searching STAC items: {bbox=} {datetime=} collections={[collection_id]} {fields=} {limit=}"
        )
        # search items
        result = stac_catalog.search(
            collections=[collection_id],
            bbox=bbox,
            datetime=datetime,
            fields=fields,
            limit=limit,
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
        items: List[Dict[str, Any]], bands: List[str]
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
        logger.debug(f"Converting datetime start={temporal_extent.start} end={temporal_extent.end}")
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
