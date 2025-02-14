from collections import defaultdict
from typing import Any, DefaultDict, Dict, List, Mapping, Optional, Tuple
from pystac import Item
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
from datetime import datetime
from odc.stac import stac_load, configure_rio
from tensorlakehouse_openeo_driver.file_reader.raster_file_reader import (
    RasterFileReader,
)
import statistics

assert os.path.isfile("logging.conf")
logging.config.fileConfig(fname="logging.conf", disable_existing_loggers=False)
logger = logging.getLogger("geodnLogger")


class COGFileReader(RasterFileReader):
    def __init__(
        self,
        items: List[Item],
        bands: List[str],
        bbox: Tuple[float, float, float, float],
        temporal_extent: Tuple[datetime, Optional[datetime]],
        properties: Optional[Dict[str, Any]],
    ) -> None:
        super().__init__(
            items=items,
            bbox=bbox,
            bands=bands,
            temporal_extent=temporal_extent,
            properties=properties,
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
            items_by_crs_and_res,
            most_frequent_epsg,
            most_frequent_resolution,
        ) = COGFileReader._group_items_by_crs_and_resolution(
            items=self.items, bands=self.bands
        )

        # for each group of media type items, load items into xarray
        data_arrays: List[xr.DataArray] = list()
        # concatenate the data arrays of each band alog the band dimension
        items: List[Item]
        for bands, items_same_crs_res in items_by_crs_and_res.items():
            # create a list of dataarrays that have same bands but different CRS/resolution
            diff_crs_res_arr: List[xr.DataArray] = list()
            for k, items in items_same_crs_res.items():
                crs, res = k
                # load items from COS as xarray
                data_array: xr.DataArray = self._load_items_using_odc_stac(
                    items=items,
                    bbox=self.bbox,
                    bands=bands,
                    epsg=most_frequent_epsg,
                    resolution=most_frequent_resolution,
                )

                diff_crs_res_arr.append(data_array)
            # combine all dataarrays that have same bands but different CRS/resolution
            if len(diff_crs_res_arr) > 1:
                data_array = diff_crs_res_arr[0]
                for i in range(1, len(diff_crs_res_arr)):
                    data_array = data_array.combine_first(diff_crs_res_arr[i])
            elif len(diff_crs_res_arr) == 1:
                data_array = diff_crs_res_arr.pop()
            else:
                raise ValueError(
                    f"Error! Unexpected size of single band arrays list for {bands=}"
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

    @staticmethod
    def _group_items_by_crs_and_resolution(
        items: List[Item],
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
        items_by_crs_res: DefaultDict = defaultdict(dict)
        # initialize variable
        item_properties = None
        # store the CRS and resolution of all items
        crs_resolution_list = list()
        # for each selected item
        for item in items:
            item_properties = item.properties
            # get list of available bands, which are stored as cube:variables
            available_bands = list(item_properties["cube:variables"].keys())
            epsg = CloudStorageFileReader._get_epsg(item=item)
            resolution = CloudStorageFileReader._get_resolution(item=item)
            crs_resolution_list.append((epsg, resolution))
            if "data" in item.assets.keys():
                selected_bands = tuple(available_bands)
            else:
                selected_bands = tuple([b for b in bands if b in available_bands])

            if len(selected_bands) > 0:
                crs_resolution = (epsg, resolution)
                if crs_resolution not in items_by_crs_res[selected_bands].keys():

                    items_by_crs_res[selected_bands][crs_resolution] = [item]
                else:
                    items_by_crs_res[selected_bands][crs_resolution].append(item)

        most_frequent_crs = COGFileReader._get_most_frequent_epsg(items=items)
        most_frequent_res = COGFileReader._get_most_frequent_resolution(items=items)
        return items_by_crs_res, most_frequent_crs, most_frequent_res

    def _load_items_using_odc_stac(
        self,
        items: List[Item],
        bbox: Tuple[float, float, float, float],
        bands: List[str],
        epsg: int,
        resolution: float,
    ) -> xr.DataArray:
        """load STAC items that match the criteria specified by end-user as xarray object

        Returns:
            xr.DataArray: datacube
        """

        logger.debug(f"_load_items_using_odc_stac - connecting to {self.endpoint=}")
        # setting gdal env vars https://gdal.org/en/latest/user/configoptions.html
        os.environ["AWS_ACCESS_KEY_ID"] = self.access_key_id
        os.environ["AWS_SECRET_ACCESS_KEY"] = self.secret_access_key
        os.environ["AWS_S3_ENDPOINT"] = self.endpoint
        session = self._create_boto3_session()
        configure_rio(cloud_defaults=True, aws={"session": session})

        # get epsg from arbitray item
        epsg = COGFileReader._get_most_frequent_epsg(items=self.items)
        assert isinstance(epsg, int)
        resolution = COGFileReader._get_most_frequent_resolution(items=self.items)
        assert isinstance(resolution, float)
        # some items have 'data' as asset key while others have band name. If these items
        # have band names, then check if the required bands are a subset of the bands
        arbitrary_item = items[0]
        asset_keys: set = set(list(arbitrary_item.assets.keys()))
        # asset_keys = set(arbitrary_item.assets.keys())
        if "data" in asset_keys:
            asset_key_as_bands = list(asset_keys)
        else:
            asset_key_as_bands = bands
        logger.debug(
            f"COGFileReader::_load_items_using_odc_stac - {bbox=} {asset_key_as_bands=} {resolution=} {epsg=}"
        )
        ds = stac_load(
            items=items,
            bbox=bbox,
            # bands=None,
            bands=asset_key_as_bands,
            crs=epsg,
            resolution=resolution,
            chunks={},  # <-- use Dask
        )
        # if asset key is 'data' and only one bands is required, then rename data to band name
        if (
            "data" in list(ds)
            and len(items[0].properties["cube:variables"].keys()) == 1
        ):
            ds = ds.rename_vars({"data": bands[0]})
        # convert
        arr = ds.to_array(dim=DEFAULT_BANDS_DIMENSION)
        x_dim = self._get_dimension_name(item=arbitrary_item, axis=DEFAULT_X_DIMENSION)
        y_dim = self._get_dimension_name(item=arbitrary_item, axis=DEFAULT_Y_DIMENSION)
        # ODC sets latitude/longitude as default coordinates, so we need to rename them
        # reference: https://github.com/opendatacube/odc-stac/issues/136#issuecomment-1860094091
        if "latitude" in arr.sizes.keys() and "latitude" != y_dim:
            arr = arr.rename({"latitude": y_dim})
        if "longitude" in arr.sizes.keys() and "longitude" != x_dim:
            arr = arr.rename({"longitude": x_dim})

        return arr

    @staticmethod
    def _get_most_frequent_resolution(items: List[Item]) -> float:
        resolution_list = list()
        for item in items:
            resolution = CloudStorageFileReader._get_resolution(item=item)
            assert resolution is not None
            resolution_list.append(float(resolution))
        return statistics.mode(resolution_list)

    @staticmethod
    def _get_most_frequent_epsg(items: List[Item]) -> int:
        epsg_list = list()
        for item in items:
            epsg = CloudStorageFileReader._get_epsg(item=item)
            assert isinstance(epsg, int), f"Error! Unexpected {epsg=} of {item=}"
            epsg_list.append(epsg)
        return statistics.mode(epsg_list)
