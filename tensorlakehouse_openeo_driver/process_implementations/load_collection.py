from abc import ABC, abstractmethod
from collections import defaultdict
from datetime import datetime
from typing import Any, DefaultDict, Dict, List, Mapping, Tuple, Union
from openeo_pg_parser_networkx.pg_schema import (
    BoundingBox,
    TemporalInterval,
)
import pystac
from pystac_client import Client
import xarray as xr
from tensorlakehouse_openeo_driver.constants import (
    DEFAULT_BANDS_DIMENSION,
    COG_MEDIA_TYPE,
    STAC_DATETIME_FORMAT,
    STAC_URL,
    ZIP_ZARR_MEDIA_TYPE,
    logger,
)
import pandas as pd
import pyproj
from pyproj import Transformer
from tensorlakehouse_openeo_driver.cos_parser import COSConnector


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


class LoadCollectionFromCOS(AbstractLoadCollection):
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
        item_collection = self._search_items(
            bbox=bbox_wsg84,
            temporal_extent=temporal_extent,
            collection_id=id,
        )
        # group items by media type, because zarr items are handled differently than non-zarr items
        (
            item_by_bands,
            most_frequent_epsg,
            most_frequent_resolution,
        ) = LoadCollectionFromCOS._group_items_by_band(item_collection=item_collection, bands=bands)

        # for each group of media type items, load items into xarray
        data_arrays: List[xr.DataArray] = list()
        # concatenate the data arrays of each band alog the band dimension
        for band, items_grouped_by_media in item_by_bands.items():
            single_band_arrays = list()
            # combine the  data array that have the same band
            for k, stac_items in items_grouped_by_media.items():
                media_type, _, _ = k
                # if items are single-asset, 'assets' is a list that has a single band name that will be
                # used to rename 'data'
                if band is not None:
                    assets = [band]
                else:
                    # multi-asset items are loaded in parallel
                    assert bands is not None
                    assets = bands

                # load items from COS as xarray
                arr = LoadCollectionFromCOS._load_items_from_cos(
                    items=stac_items,
                    media_type=media_type,
                    bbox_wsg84=bbox_wsg84,
                    assets=assets,
                    spatial_extent=spatial_extent,
                    epsg=most_frequent_epsg,
                    resolution=most_frequent_resolution,
                )

                single_band_arrays.append(arr)

            data_array = None
            if len(single_band_arrays) > 1:
                data_array = single_band_arrays[0]
                for i in range(1, len(single_band_arrays)):
                    data_array = data_array.combine_first(single_band_arrays[i])
            elif len(single_band_arrays) == 1:
                data_array = single_band_arrays.pop()
            else:
                raise ValueError(
                    f"Error! Unexpected size of single band arrays list for band {band}"
                )
            data_arrays.append(data_array)
        if len(data_arrays) > 1:
            # Reindex each band to match coordinates of first band
            data_arrays_aligned = []
            selection: Mapping[Any, Any] = {DEFAULT_BANDS_DIMENSION: 0}
            for ds in data_arrays:
                ds = ds.isel(**selection).reindex_like(
                    data_arrays[0].isel(**selection), method="nearest"
                )
                data_arrays_aligned.append(ds)

            data_array = xr.concat(data_arrays_aligned, dim=DEFAULT_BANDS_DIMENSION)
        else:
            data_array = data_arrays.pop()

        return data_array

    def _search_items(
        self,
        bbox: Tuple[float, float, float, float],
        temporal_extent: TemporalInterval,
        collection_id: str,
    ):
        starttime, endtime = LoadCollectionFromCOS._get_start_and_endtime(
            temporal_extent=temporal_extent
        )
        # set datetime using STAC format
        datetime = (
            f"{starttime.strftime(STAC_DATETIME_FORMAT)}/{endtime.strftime(STAC_DATETIME_FORMAT)}"
        )
        logger.debug(f"Connecting to STAC service URL={STAC_URL}")
        stac_catalog = Client.open(STAC_URL)

        logger.debug(f"STAC search items: bbox={bbox} datetime={datetime} collections={[id]}")
        fields: Union[Dict[str, list[str]], str, None] = {
            "include": [
                "id",
                "bbox",
                "datetime",
                "properties.cube:variables",
                "properties.cube:dimensions",
            ],
            "exclude": [],
        }
        # search items
        result = stac_catalog.search(
            collections=[collection_id],
            bbox=bbox,
            datetime=datetime,
            fields=fields,
        )
        matched = result.matched()
        logger.debug(f"{matched} items have been found")
        assert matched is not None and matched > 0, f"Error! {matched} items have been found"
        item_collection = result.item_collection()
        return item_collection

    @staticmethod
    def _group_items_by_band(
        item_collection: pystac.ItemCollection,
        bands: List[str],
    ) -> Tuple[DefaultDict[str, Any], int, float]:
        """this method groups items by band because we want to stack data arrays. Within each band
          group, we group items by media type, because the way we load each media type is different
        from each other, thus grouping them facilitates loading them into memory

        Args:
            item_collection (pystac.ItemCollection): set of item objects

        Returns:
            Dict[str, Any]: items grouped by media types
        """
        # asset description
        data_asset_description = "data"
        # filter out items that have repeated asset.href
        unique_asset_href = set()
        # group items by media type (e.g., zarr, tiff)
        items_by_band: DefaultDict[str, Dict] = defaultdict(dict)
        # initialize variable
        item_properties = None
        # store the CRS and resolution of all items
        crs_resolution_list: List[Tuple[int, float]] = list()
        # for each selected item
        for item in item_collection:
            item_properties = item.properties
            # get list of available bands, which are stored as cube:variables
            available_bands = list(item_properties["cube:variables"].keys())
            epsg = COSConnector._get_epsg(item=item)
            assert epsg is not None
            resolution = COSConnector._get_resolution(item=item)
            assert resolution is not None
            crs_resolution_list.append((epsg, resolution))
            for band in bands:
                if band in available_bands:
                    assets = item.get_assets()
                    # if "data" is the key
                    if data_asset_description in assets.keys():
                        asset = assets[data_asset_description]
                    # if band name is the key
                    elif band in assets.keys():
                        asset = assets[band]
                    else:
                        continue
                    # filter out items that have repeated asset.href
                    if asset.href not in unique_asset_href:
                        unique_asset_href.add(asset.href)
                        # if this is a single-asset item, group by mediatype and band
                        if data_asset_description in assets.keys():
                            mediatype_crs_res = (asset.media_type, epsg, resolution)
                        else:
                            # group by mediatype
                            mediatype_crs_res = (asset.media_type, epsg, resolution)
                        # create dict entry for a media type composed by a list of items and item:properties
                        if mediatype_crs_res in items_by_band[band].keys():
                            items_by_band[band][mediatype_crs_res].append(item)
                        else:
                            items_by_band[band][mediatype_crs_res] = [item]

        (
            most_frequent_crs,
            most_frequent_res,
        ) = LoadCollectionFromCOS._get_most_frequent_crs(crs_resolution_list=crs_resolution_list)
        return items_by_band, most_frequent_crs, most_frequent_res

    @staticmethod
    def _get_most_frequent_crs(crs_resolution_list: List[Tuple[int, float]]) -> Tuple[int, float]:
        """if CRS and resolution is a tuple, compute the most frequent tuple

        Args:
            crs_resolution_list (List[Tuple[int, float]]): _description_

        Returns:
            Tuple[int, float]: _description_
        """
        most_frequent: DefaultDict[Tuple[int, float], int] = defaultdict(int)
        for k in crs_resolution_list:
            most_frequent[k] += 1
        k, _ = sorted(list(most_frequent.items()), key=lambda x: x[1], reverse=True)[0]
        return k

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

    @staticmethod
    def _load_items_from_cos(
        items: List[pystac.Item],
        media_type: str,
        bbox_wsg84: Tuple[float, float, float, float],
        assets: List[str],
        spatial_extent: BoundingBox,
        epsg: int,
        resolution: float,
    ) -> xr.DataArray:
        """create an array using specified STAC items using data available on S3/COS

        Args:
            items (List[pystac.Item]): list of STAC items
            media_type (str): file format such as application/x-tar
            bbox (Tuple): west, south, east, nort
            epsg (int): reference system
            resolution (float): spatial resolution as specified by the STAC data cube extension

        Returns:
            xr.DataArray: data
        """
        logger.debug("Loading STAC items from COS")
        # WARNING: assumption that all items that have been found are contained in a single bucket
        # thus we can select an arbitrary item and arbitrary key to extract the bucket
        # https://github.ibm.com/GeoDN-Discovery/main/issues/239
        arbitrary_item = items[0]
        arbitrary_asset_key = next(iter(arbitrary_item.assets.keys()))
        bucket = COSConnector._extract_bucket_name_from_url(
            arbitrary_item.assets[arbitrary_asset_key].href
        )

        conn = COSConnector(bucket=bucket)
        # connect to S3 and load data into memory
        if media_type == ZIP_ZARR_MEDIA_TYPE:
            bbox = (
                spatial_extent.west,
                spatial_extent.south,
                spatial_extent.east,
                spatial_extent.north,
            )
            data_array = conn.load_zarr(items=items, bbox=bbox)
        elif media_type == COG_MEDIA_TYPE:
            data_array = conn.load_items_using_stackstac(
                items=items,
                bbox=bbox_wsg84,
                bands=assets,
                epsg=epsg,
                resolution=resolution,
            )
        else:
            logger.warning(f"Error! media type is not supported: {media_type}")

        return data_array
