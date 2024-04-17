import ibm_boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import numpy as np
import pandas as pd
import pystac
import stackstac
import xarray as xr
import s3fs
from rasterio.session import AWSSession
import logging
import logging.config
from openeo_geodn_driver.constants import (
    DEFAULT_BANDS_DIMENSION,
    CREDENTIALS,
    DEFAULT_X_DIMENSION,
    DEFAULT_Y_DIMENSION,
)
from boto3.session import Session
from urllib.parse import urlparse
from openeo_geodn_driver.geospatial_utils import (
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

    @staticmethod
    def get_credentials_by_bucket(bucket: str) -> Dict[str, str]:
        """get the credentials to access the specified bucket

        Args:
            bucket (str): input bucket name

        Returns:
            Dict[str, str]: a dict that contains endpoint, access_key_id, secret_access_key, region,
                endpoint
        """

        assert bucket is not None
        assert isinstance(bucket, str)
        assert (
            bucket in CREDENTIALS.keys()
        ), f"Error! Missing credentials to access COS bucket: {bucket}"
        bucket_credentials: Dict = CREDENTIALS[bucket]
        assert all(
            k in bucket_credentials.keys()
            for k in ["access_key_id", "secret_access_key", "region", "endpoint"]
        )
        return bucket_credentials

    def _make_ibm_boto3_client(self, endpoint: str, access_key_id: str, secret: str):
        client = ibm_boto3.client(
            "s3",
            endpoint_url=f"https://{endpoint}",
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret,
            verify=False,
            config=Config(tcp_keepalive=True),
        )
        return client

    def _create_boto3_session(
        self, access_key_id: str, secret: str, region_name: str
    ) -> Session:
        session = Session(
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret,
            region_name=region_name,
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
        items: List[Dict[str, Any]],
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
            assets = dict()
            for k, asset in item["assets"].items():
                roles = asset["roles"]
                if COSConnector.DATA in roles:
                    assets[k] = asset
            asset = assets[COSConnector.DATA]
            urls.add(asset.href)
            datetimes.append(pd.Timestamp(item["properties"]["datetime"]))
        assert (
            len(urls) == 1
        ), f"Error! no support for loading data from multiple ZARR files: {urls}. "
        url = urls.pop()
        # create S3Map object to load zarr into memory
        bucket = COSConnector._extract_bucket_name_from_url(url=url)
        creds = COSConnector.get_credentials_by_bucket(bucket=bucket)
        store = self._create_s3map(
            url=url,
            endpoint=creds["endpoint"],
            access_key_id=creds["access_key_id"],
            secret=creds["secret_access_key"],
        )
        epsg = COSConnector._get_epsg(item=items[0])
        # open zarr as xarray.Dataset
        ds = xr.open_zarr(store)
        data: xr.DataArray = ds.to_array(dim=DEFAULT_BANDS_DIMENSION)
        temporal_dim = get_dimension_name(item=items[0], dim_type="temporal")
        x_dim = get_dimension_name(item=items[0], axis=DEFAULT_X_DIMENSION)
        y_dim = get_dimension_name(item=items[0], axis=DEFAULT_Y_DIMENSION)

        data = clip(data=data, bbox=bbox, y_dim=y_dim, x_dim=x_dim, crs=epsg)

        data = filter_by_time(
            data=data, timestamps=datetimes, temporal_dim=temporal_dim
        )
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
        if (
            url_parsed.scheme is not None
            and url_parsed.scheme.lower() == "s3"
            and isinstance(url_parsed.hostname, str)
        ):
            return url_parsed.hostname
        else:
            begin_bucket_name = 1
            end_bucket_name = url_parsed.path.find("/", begin_bucket_name)
            assert (
                end_bucket_name > begin_bucket_name
            ), f"Error! Unable to find bucket name: {url}"
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
        assert (
            slash_index > begin_bucket_name
        ), f"Error! Unable to find object name: {url}"
        object_name = url_parsed.path[slash_index:]
        return object_name

    @staticmethod
    def _get_epsg(item: Dict[str, Any]) -> Optional[int]:
        item_prop = item["properties"]
        cube_dims: Dict[str, Any] = item_prop["cube:dimensions"]
        epsg = None
        for value in cube_dims.values():
            if value.get("reference_system") is not None:
                epsg = value.get("reference_system")
        return epsg

    @staticmethod
    def _get_resolution(item: Dict[str, Any]) -> Optional[float]:
        item_prop = item["properties"]
        cube_dims: Dict[str, Any] = item_prop["cube:dimensions"]
        resolution = None
        for value in cube_dims.values():
            if value.get("step") is not None:
                resolution = float(np.abs(value.get("step")))
        return resolution

    def load_items_using_stackstac(
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

                time_dim = get_dimension_name(item=item, dim_type="temporal")
                x_dim = get_dimension_name(item=item, axis=DEFAULT_X_DIMENSION)
                y_dim = get_dimension_name(item=item, axis=DEFAULT_Y_DIMENSION)
                assets_item: Dict = item["assets"]
                arbitrary_asset_key = next(iter(assets_item.keys()))
                url = assets_item[arbitrary_asset_key]["href"]
                bucket = COSConnector._extract_bucket_name_from_url(url=url)

                if COSConnector.DATA in assets_item.keys():
                    assets = [COSConnector.DATA]
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
        credentials = COSConnector.get_credentials_by_bucket(bucket=bucket)
        session = self._create_boto3_session(
            access_key_id=credentials["access_key_id"],
            secret=credentials["secret_access_key"],
            region_name=credentials["region"],
        )
        endpoint = credentials["endpoint"]
        logger.debug(f"load_items_using_stackstac - connecting to {endpoint=}")
        # accessing non-AWS s3 https://github.com/rasterio/rasterio/pull/1779
        aws_session = AWSSession(
            session=session,
            endpoint_url=endpoint,
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
        data_array = remove_repeated_time_coords(
            data_array=data_array, time_dim=time_dim
        )

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

    def _create_s3map(
        self, url: str, endpoint: str, access_key_id: str, secret: str
    ) -> s3fs.S3Map:
        """create S3Map object based on specified URL

        Args:
            url (str): link to the asset
        """
        if endpoint.lower().startswith("https://"):
            endpoint_url = endpoint
        else:
            endpoint_url = f"https://{endpoint}"
        fs = s3fs.S3FileSystem(
            endpoint_url=endpoint_url,
            key=access_key_id,
            secret=secret,
        )
        parsed = urlparse(url=url)
        if parsed.scheme != "s3":
            url = COSConnector._convert_https_to_s3(url=url)
        # return url
        store = s3fs.S3Map(root=url, s3=fs)
        return store

    def upload_fileobj(
        self,
        bucket: str,
        key: str,
        path: Path,
        endpoint: str,
        access_key_id: str,
        secret: str,
    ):
        """upload file to COS

        based on https://ibm.github.io/ibm-cos-sdk-python/reference/services/s3.html#S3.Object.upload_fileobj

        Args:
            key (str): filename
            path (Path): local path
        """
        logger.debug(f"Upload file to COS: {key=} {path=} {bucket=}")
        s3 = ibm_boto3.resource(
            "s3",
            endpoint_url=f"https://{endpoint}",
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret,
            verify=False,
            config=Config(tcp_keepalive=True),
        )
        bucket_obj = s3.Bucket(bucket)
        obj = bucket_obj.Object(key)

        with open(path, "rb") as data:
            obj.upload_fileobj(data)
        logger.debug(f"File {key=} has been uploaded")

    def create_presigned_link(
        self, bucket: str, key: str, expiration: int = 3600
    ) -> str:
        """Generate a presigned URL for the S3 object
        based on https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-presigned-urls.html
        Args:
            key (str): full path to object
            expiration (int, optional): _description_. Defaults to 3600.

        Returns:
            Optional[str]: pre-signed url
        """
        logger.debug(f"Create presigned link: {bucket=} {key=}")
        creds = COSConnector.get_credentials_by_bucket(bucket=bucket)
        s3_client = self._make_ibm_boto3_client(
            endpoint=creds["endpoint"],
            access_key_id=creds["access_key_id"],
            secret=creds["secret_access_key"],
        )
        try:
            response = s3_client.generate_presigned_url(
                "get_object",
                Params={"Bucket": bucket, "Key": key},
                ExpiresIn=expiration,
            )
        except ClientError as e:
            logging.error(e)
            raise e

        # The response contains the presigned URL
        assert isinstance(response, str), f"Error! Unexpected response: {response}"
        return response


# def main():
#     bucket = "openeo-geodn-driver-output"
#     path = Path(
#         "/Users/ltizzei/Projects/Orgs/GeoDN-Discovery/openeo-geodn-driver/examples/test/openeo_data.tif"
#     )
#     assert path.exists()
#     from datetime import datetime
#     import uuid

#     now = datetime.now().strftime("%Y%m%dT%H%M%S")
#     random_str = uuid.uuid4().hex
#     new_object_name = f"{now}-{random_str}-output.tif"
#     cos = COSConnector(bucket=bucket)
#     cos.upload_fileobj(key=new_object_name, path=path)
#     resp = cos.create_presigned_link(key=new_object_name)
#     print(resp)


# if __name__ == "__main__":
#     main()
