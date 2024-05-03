import ibm_boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import numpy as np
import pandas as pd
from pystac import Item
import pystac
import stackstac
import xarray as xr
import s3fs
from rasterio.session import AWSSession
import logging
import logging.config
from tensorlakehouse_openeo_driver.constants import (
    DEFAULT_BANDS_DIMENSION,
    CREDENTIALS,
    DEFAULT_X_DIMENSION,
    DEFAULT_Y_DIMENSION,
)
from boto3.session import Session
from urllib.parse import urlparse
from tensorlakehouse_openeo_driver.geospatial_utils import (
    clip,
    filter_by_time,
    get_dimension_name,
    remove_repeated_time_coords,
)


assert os.path.isfile("logging.conf")
logging.config.fileConfig(fname="logging.conf", disable_existing_loggers=False)
logger = logging.getLogger("geodnLogger")


class COSConnector:
    DATA = "data"

    def __init__(self, bucket: str) -> None:
        assert bucket is not None
        assert isinstance(bucket, str)
        assert (
            bucket in CREDENTIALS.keys()
        ), f"Error! Missing credentials to access COS bucket: {bucket}"
        bucket_credentials = CREDENTIALS[bucket]
        self._access_key_id = bucket_credentials["access_key_id"]
        self._secret = bucket_credentials["secret_access_key"]
        self._endpoint = bucket_credentials["endpoint"]
        self._region_name = bucket_credentials["region"]
        self.bucket = bucket

    def _make_ibm_boto3_client(self):
        client = ibm_boto3.client(
            "s3",
            endpoint_url=f"https://{self._endpoint}",
            aws_access_key_id=self._access_key_id,
            aws_secret_access_key=self._secret,
            verify=False,
            config=Config(tcp_keepalive=True),
        )
        return client

    def _create_boto3_session(self) -> Session:
        session = Session(
            aws_access_key_id=self._access_key_id,
            aws_secret_access_key=self._secret,
            region_name=self._region_name,
        )
        return session

    @staticmethod
    def _get_dimension_description(item: pystac.Item, axis: str) -> Optional[str]:
        item_prop = item.properties
        cube_dims: Dict[str, Any] = item_prop["cube:dimensions"]
        for key, value in cube_dims.items():
            if value.get("axis") is not None and value.get("axis") == axis:
                return key
        return None

    def load_zarr(
        self,
        items: List[Item],
        # item_properties: ItemProperties,
        bbox: Tuple[float, float, float, float],
    ) -> xr.DataArray:
        """connects to S3 and loads data into memory

        based on code snippet available https://nasa-openscapes.github.io/2021-Cloud-Hackathon/tutorials/05_Data_Access_Direct_S3.html#read-in-a-single-hls-file

        Args:
            items (List[Item]): list of STAC items that will be loaded
            item_properties (ItemProperties): item's properties

        Returns:
            xr.DataArray: data loaded from zarr file
        """
        urls = set()
        datetimes = list()
        # assumption: multiple items are mapped to a single zarr file
        # check if this is true
        for item in items:
            assets = item.get_assets(role=COSConnector.DATA)
            asset = assets[COSConnector.DATA]
            urls.add(asset.href)
            datetimes.append(pd.Timestamp(item.datetime))
        assert (
            len(urls) == 1
        ), f"Error! no support for loading data from multiple ZARR files: {urls}. "
        url = urls.pop()
        # create S3Map object to load zarr into memory
        store = self._create_s3map(url=url)
        item = items[0]
        assert isinstance(item, dict)
        epsg = COSConnector._get_epsg(item=item)
        assert isinstance(epsg, int), f"Error! Invalid {epsg=}"
        # open zarr as xarray.Dataset
        ds = xr.open_zarr(store)
        # temporary data as it has not been clipped
        temp_data = ds.to_array(dim=DEFAULT_BANDS_DIMENSION)
        temporal_dim = get_dimension_name(item=item, dim_type="temporal")
        assert isinstance(temporal_dim, str)
        # get name of x axis dim
        x_dim = get_dimension_name(item=item, axis=DEFAULT_X_DIMENSION)
        assert isinstance(x_dim, str)
        # get name of y axis dim
        y_dim = get_dimension_name(item=item, axis=DEFAULT_Y_DIMENSION)
        assert isinstance(y_dim, str)
        # clipping data
        temp_data = clip(data=temp_data, bbox=bbox, y_dim=y_dim, x_dim=x_dim, crs=epsg)

        data = filter_by_time(data=temp_data, timestamps=datetimes, temporal_dim=temporal_dim)
        return data

    @staticmethod
    def _extract_bucket_name_from_url(url: str) -> str:
        """parse url and get the bucket as str

        Args:
            url (str): link to file on COS

        Returns:
            str: bucket name
        """
        # the first char of the path is a slash, so we need to skip it to get the bucket name
        url_parsed = urlparse(url=url)
        if url_parsed.scheme is not None and url_parsed.scheme.lower() == "s3":
            assert isinstance(
                url_parsed.hostname, str
            ), f"Error! Unexpected hostname: {url_parsed.hostname}"
            return url_parsed.hostname
        else:
            begin_bucket_name = 1
            end_bucket_name = url_parsed.path.find("/", begin_bucket_name)
            assert end_bucket_name > begin_bucket_name, f"Error! Unable to find bucket name: {url}"
            bucket = url_parsed.path[begin_bucket_name:end_bucket_name]
            return bucket

    @staticmethod
    def _get_object(url: str) -> str:
        """parse url and get the object (aka key, path) as str

        Args:
            url (str): link to file on COS

        Returns:
            str: object name
        """
        begin_bucket_name = 1
        url_parsed = urlparse(url=url)
        slash_index = url_parsed.path.find("/", begin_bucket_name) + 1
        assert slash_index > begin_bucket_name, f"Error! Unable to find object name: {url}"
        object_name = url_parsed.path[slash_index:]
        return object_name

    @staticmethod
    def _get_epsg(item: pystac.Item) -> Optional[int]:
        item_prop = item.properties
        cube_dims: Dict[str, Any] = item_prop["cube:dimensions"]
        for value in cube_dims.values():
            if value.get("reference_system") is not None:
                reference_system = value.get("reference_system")
                assert isinstance(reference_system, int)
                return reference_system
        return None

    @staticmethod
    def _get_resolution(item: pystac.Item) -> Optional[int]:
        item_prop = item.properties
        cube_dims: Dict[str, Any] = item_prop["cube:dimensions"]
        for value in cube_dims.values():
            if value.get("step") is not None:
                step = value.get("step")
                assert isinstance(step, int), f"Error! Invalid step: {step}"
                return step
        return None

    def load_items_using_stackstac(
        self,
        items: List[Item],
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
        # create boto3 session using credentials
        session = self._create_boto3_session()
        # accessing non-AWS s3 https://github.com/rasterio/rasterio/pull/1779
        aws_session = AWSSession(
            session=session,
            endpoint_url=self._endpoint,
        )
        # select an arbitrary item based on the assumption that all items have the same 'assets'
        # structure, i.e., either use 'data' or band names
        arbitrary_item = items[0]
        # select the list of assets that will be loaded
        if COSConnector.DATA in arbitrary_item.assets.keys():
            assets = [COSConnector.DATA]
        else:
            assets = bands

        dict_items = []
        for i in items:
            dict_item = i.to_dict()
            mydatetime = i.properties.get("datetime")
            pddt = pd.Timestamp(mydatetime)
            dict_item["properties"]["datetime"] = pddt.isoformat(sep="T", timespec="seconds")
            dict_items.append(dict_item)

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
            gdal_env=stackstac.DEFAULT_GDAL_ENV.updated(always=dict(session=aws_session)),
            band_coords=False,
            sortby_date="asc",
        )
        # convert stackstac default dimension names to openEO default
        if "band" in data_array.dims and "band" != DEFAULT_BANDS_DIMENSION:
            data_array = data_array.rename({"band": DEFAULT_BANDS_DIMENSION})
        time_dim = get_dimension_name(item=arbitrary_item, dim_type="temporal")
        assert isinstance(time_dim, str), f"Error! unexpected type of time_dim {time_dim}"
        x_dim = get_dimension_name(item=arbitrary_item, axis=DEFAULT_X_DIMENSION)
        y_dim = get_dimension_name(item=arbitrary_item, axis=DEFAULT_Y_DIMENSION)
        # if time_dim in data_array.dims and "time" != TIME:
        # data_array = data_array.rename({"time": TIME})
        # drop coords that are not required to avoid merging conflicts
        for coord in list(data_array.coords.keys()):
            if coord not in [x_dim, y_dim, DEFAULT_BANDS_DIMENSION, time_dim]:
                data_array = data_array.reset_coords(names=coord, drop=True)
        data_array = remove_repeated_time_coords(data_array=data_array, time_dim=time_dim)

        data_array.rio.write_crs(epsg, inplace=True)

        # if "data" is the coordinate of the band, rename it to band name (e.g., B02)
        if (
            data_array.coords[DEFAULT_BANDS_DIMENSION].values[0] == COSConnector.DATA
            and len(bands) == 1
        ):
            data_array = data_array.assign_coords({DEFAULT_BANDS_DIMENSION: bands})
        assert isinstance(
            data_array, xr.DataArray
        ), f"Error! data_array is not xarray.DataArray: {type(data_array)}"

        return data_array

    @staticmethod
    def _convert_https_to_s3(url: str) -> str:
        """convert a https url to s3

        Args:
            url (str): link to data on COS using https scheme

        Returns:
            str: link to data on COS using s3 scheme
        """

        bucket = COSConnector._extract_bucket_name_from_url(url=url)
        object = COSConnector._get_object(url=url)
        url = f"s3://{bucket}/{object}"
        return url

    def _create_s3map(self, url: str) -> s3fs.S3Map:
        """create S3Map object based on specified URL

        Args:
            url (str): link to the asset
        """
        if self._endpoint.lower().startswith("https://"):
            endpoint_url = self._endpoint
        else:
            endpoint_url = f"https://{self._endpoint}"
        fs = s3fs.S3FileSystem(
            endpoint_url=endpoint_url,
            key=self._access_key_id,
            secret=self._secret,
        )
        parsed = urlparse(url=url)
        if parsed.scheme != "s3":
            url = COSConnector._convert_https_to_s3(url=url)
        # return url
        store = s3fs.S3Map(root=url, s3=fs)
        return store

    def download_file(self, object: str, path: Path) -> None:
        """download file from COS
        Args:
            bucket (str): bucket
            object (str): aka key or path
            path (str): full path to file
        Returns:
            None
        """

        logger.debug(f"Downloading from bucket={self.bucket} object={object} and saving as {path}")
        # instantiate s3 client
        # Initialize the COS client
        cos = self._make_ibm_boto3_client()

        # store into file
        cos.download_file(self.bucket, object, path)
        assert path.exists(), f"Error! File {path} does not exist"

    def upload_fileobj(self, key: str, path: Path):
        """upload file to COS

        based on https://ibm.github.io/ibm-cos-sdk-python/reference/services/s3.html#S3.Object.upload_fileobj

        Args:
            key (str): filename
            path (Path): local path
        """
        logger.debug(f"Upload file to COS: key={key} path={path} bucket={self.bucket}")
        s3 = ibm_boto3.resource(
            "s3",
            endpoint_url=f"https://{self._endpoint}",
            aws_access_key_id=self._access_key_id,
            aws_secret_access_key=self._secret,
            verify=False,
            config=Config(tcp_keepalive=True),
        )
        bucket_obj = s3.Bucket(self.bucket)
        obj = bucket_obj.Object(key)

        with open(path, "rb") as data:
            obj.upload_fileobj(data)

    def create_presigned_link(self, key: str, expiration: int = 3600) -> Optional[str]:
        """Generate a presigned URL for the S3 object
        based on https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-presigned-urls.html
        Args:
            key (str): full path to object
            expiration (int, optional): _description_. Defaults to 3600.

        Returns:
            Optional[str]: pre-signed url
        """

        s3_client = self._make_ibm_boto3_client()
        try:
            response = s3_client.generate_presigned_url(
                "get_object",
                Params={"Bucket": self.bucket, "Key": key},
                ExpiresIn=expiration,
            )
        except ClientError as e:
            logging.error(e)
            return None

        # The response contains the presigned URL
        assert isinstance(response, str), f"Error! Unexpected type: {response=}"
        return response
