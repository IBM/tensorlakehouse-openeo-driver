from collections import defaultdict
import numpy as np
from typing import Any, DefaultDict, Dict, List, Mapping, Optional, Tuple
import stackstac
import xarray as xr
from tensorlakehouse_openeo_driver.constants import (
    DEFAULT_BANDS_DIMENSION,
    DEFAULT_X_DIMENSION,
    DEFAULT_Y_DIMENSION,
)
from tensorlakehouse_openeo_driver.file_reader.cloud_storage_file_reader import (
    CloudStorageFileReader,
)
import os
import logging
import pandas as pd
from rasterio.session import AWSSession
from tensorlakehouse_openeo_driver import geospatial_utils
from datetime import datetime

assert os.path.isfile("logging.conf")
logging.config.fileConfig(fname="logging.conf", disable_existing_loggers=False)
logger = logging.getLogger("geodnLogger")


class COGFileReader(CloudStorageFileReader):
    def __init__(
        self,
        items: List[Dict[str, Any]],
        bands: List[str],
        bbox: Tuple[float, float, float, float],
        temporal_extent: Tuple[datetime, Optional[datetime]],
        dimension_map: Optional[Dict[str, str]],
    ) -> None:
        super().__init__(
            items=items,
            bbox=bbox,
            bands=bands,
            temporal_extent=temporal_extent,
            dimension_map=dimension_map,
        )

    def load_items(
        self,
    ) -> xr.DataArray:
        """load STAC items that match the criteria specified by end-user as xarray object

        Args:
            items (List[Dict[str, Any]]): list of items matched
            bands (List[str]): band names
            bbox (Tuple[float, float, float, float]): west, south, east, north - WSG84 reference system

        Returns:
            xr.DataArray: datacube
        """
        # group items by media type, because zarr items are handled differently than non-zarr items
        (
            item_by_bands,
            most_frequent_epsg,
            most_frequent_resolution,
        ) = COGFileReader._group_items_by_band(items=self.items, bands=self.bands)

        # for each group of media type items, load items into xarray
        data_arrays: List[xr.DataArray] = list()
        # concatenate the data arrays of each band alog the band dimension
        for band, items_grouped_by_crs_resolution in item_by_bands.items():
            single_band_arrays = list()
            # combine the  data array that have the same band
            for stac_items in items_grouped_by_crs_resolution.values():
                # if items are single-asset, 'assets' is a list that has a single band name that will be
                # used to rename 'data'
                if band is not None:
                    assets = [band]
                else:
                    # multi-asset items are loaded in parallel
                    assert self.bands is not None
                    assets = self.bands

                # load items from COS as xarray
                arr = self._load_items_using_stackstac(
                    items=stac_items,
                    bbox=self.bbox,
                    bands=assets,
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

    def _load_items_using_stackstac(
        self,
        items: List[Dict[str, Any]],
        bbox: Tuple[float, float, float, float],
        bands: List[str],
        epsg: int,
        resolution: float,
    ) -> xr.DataArray:
        """load STAC items into memory as xarray objects

        Args:
            items (List[Item]): list of STAC items
            bbox (Tuple[float, float, float, float]): bounding box (west, south, east, north)
            resolution (float): spatial resolution or step.  Careful: this must be given in
                the output CRS's units! For example, with epsg=4326 (meaning lat-lon),
                the units are degrees of latitude/longitude, not meters. Giving resolution=20 in
                that case would mean each pixel is 20ºx20º (probably not what you wanted).
                You can also give pair of (x_resolution, y_resolution).
            epsg (int): reference system (e.g., 4326)

        Returns:
            xr.DataArray: _description_
        """

        dict_items = []
        bucket = None
        time_dim = None
        x_dim = None
        y_dim = None
        for index, item in enumerate(items):
            # select the list of assets that will be loaded
            if index == 0:
                # convert stackstac default dimension names to openEO default

                time_dim = CloudStorageFileReader._get_dimension_name(
                    item=item, dim_type="temporal"
                )
                x_dim = CloudStorageFileReader._get_dimension_name(
                    item=item, axis=DEFAULT_X_DIMENSION
                )
                y_dim = CloudStorageFileReader._get_dimension_name(
                    item=item, axis=DEFAULT_Y_DIMENSION
                )
                assets_item: Dict = item["assets"]
                arbitrary_asset_key = next(iter(assets_item.keys()))
                url = assets_item[arbitrary_asset_key]["href"]
                bucket = CloudStorageFileReader._extract_bucket_name_from_url(url=url)

                if CloudStorageFileReader.DATA in assets_item.keys():
                    assets = [CloudStorageFileReader.DATA]
                else:
                    assets = bands
            item_prop = item["properties"]

            mydatetime = item_prop.get("datetime")
            pddt = pd.Timestamp(mydatetime)
            item["properties"]["datetime"] = pddt.isoformat(sep="T", timespec="seconds")
            dict_items.append(item)

        assert isinstance(time_dim, str), f"Error! Unexpected time_dim={time_dim}"
        # create boto3 session using credentials
        assert isinstance(bucket, str)
        session = self._create_boto3_session()
        logger.debug(f"load_items_using_stackstac - connecting to {self.endpoint=}")
        # accessing non-AWS s3 https://github.com/rasterio/rasterio/pull/1779
        aws_session = AWSSession(
            session=session,
            endpoint_url=self.endpoint,
        )
        # setting gdal_env param is based on this https://github.com/gjoseph92/stackstac#roadmap
        data_array = stackstac.stack(
            dict_items,
            epsg=epsg,
            resolution=resolution,
            bounds_latlon=bbox,
            rescale=False,
            fill_value=np.nan,
            properties=["datetime"],
            assets=assets,
            gdal_env=stackstac.DEFAULT_GDAL_ENV.updated(
                always=dict(session=aws_session)
            ),
            band_coords=False,
            sortby_date="asc",
        )
        if "band" in data_array.dims and "band" != DEFAULT_BANDS_DIMENSION:
            data_array = data_array.rename({"band": DEFAULT_BANDS_DIMENSION})
        # if time_dim in data_array.dims and "time" != TIME:
        # data_array = data_array.rename({"time": TIME})
        # drop coords that are not required to avoid merging conflicts
        for coord in list(data_array.coords.keys()):
            if coord not in [x_dim, y_dim, DEFAULT_BANDS_DIMENSION, time_dim]:
                data_array = data_array.reset_coords(names=coord, drop=True)
        data_array = geospatial_utils.remove_repeated_time_coords(
            data_array=data_array, time_dim=time_dim
        )

        data_array.rio.write_crs(epsg, inplace=True)

        # if "data" is the coordinate of the band, rename it to band name (e.g., B02)
        if (
            data_array.coords[DEFAULT_BANDS_DIMENSION].values[0]
            == CloudStorageFileReader.DATA
            and len(bands) == 1
        ):
            data_array = data_array.assign_coords({DEFAULT_BANDS_DIMENSION: bands})
        assert isinstance(
            data_array, xr.DataArray
        ), f"Error! data_array is not xarray.DataArray: {type(data_array)}"

        return data_array

    @staticmethod
    def _group_items_by_band(
        items: List[Dict[str, Any]],
        bands: List[str],
    ) -> Tuple[DefaultDict, int, float]:
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
        items_by_band: DefaultDict = defaultdict(dict)
        # initialize variable
        item_properties = None
        # store the CRS and resolution of all items
        crs_resolution_list = list()
        # for each selected item
        for item in items:
            item_properties = item["properties"]
            # get list of available bands, which are stored as cube:variables
            available_bands = list(item_properties["cube:variables"].keys())
            epsg = CloudStorageFileReader._get_epsg(item=item)
            resolution = CloudStorageFileReader._get_resolution(item=item)
            crs_resolution_list.append((epsg, resolution))
            for band in bands:
                if band in available_bands:
                    assets: Dict[str, Any] = item["assets"]
                    # if "data" is the key
                    if data_asset_description in assets.keys():
                        asset = assets[data_asset_description]
                    # if band name is the key
                    elif band in assets.keys():
                        asset = assets[band]
                    else:
                        continue
                    # filter out items that have repeated asset.href
                    if asset["href"] not in unique_asset_href:
                        unique_asset_href.add(asset["href"])
                        # if this is a single-asset item, group by mediatype and band
                        crs_resolution = (epsg, resolution)

                        # create dict entry for a media type composed by a list of items and item:properties
                        if crs_resolution in items_by_band[band].keys():
                            items_by_band[band][crs_resolution].append(item)
                        else:
                            items_by_band[band][crs_resolution] = [item]

        (
            most_frequent_crs,
            most_frequent_res,
        ) = COGFileReader._get_most_frequent_crs(
            crs_resolution_list=crs_resolution_list
        )
        return items_by_band, most_frequent_crs, most_frequent_res

    @staticmethod
    def _get_most_frequent_crs(
        crs_resolution_list: List[Tuple[Optional[int], Optional[float]]]
    ) -> Tuple[int, float]:
        """CRS and resolution is a tuple, so compute the most frequent tuple

        Args:
            crs_resolution_list (List[Tuple[int, float]]): list of tuple composed by CRS and
                resolution

        Returns:
            Tuple[int, float]: most frequent CRS and resolution
        """
        assert len(crs_resolution_list) > 0
        most_frequent: DefaultDict = defaultdict(int)
        for k in crs_resolution_list:
            most_frequent[k] += 1
        # list of tuples reverse sorted by the number of occurrences
        crs_resolution_sorted = sorted(
            list(most_frequent.items()), key=lambda x: x[1], reverse=True
        )
        # first item is the most frequent
        k, _ = crs_resolution_sorted[0]
        epsg, resolution = k
        assert isinstance(epsg, int)
        assert isinstance(resolution, float)
        return epsg, resolution
