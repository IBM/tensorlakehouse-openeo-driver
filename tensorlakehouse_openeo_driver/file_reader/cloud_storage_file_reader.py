from shapely.geometry.polygon import Polygon
import os
from typing import Any, Dict, List, Optional, Tuple
import numpy as np
import pystac
import s3fs
import logging
import logging.config
from tensorlakehouse_openeo_driver.constants import (
    CREDENTIALS,
)
from boto3.session import Session
from urllib.parse import urlparse
from datetime import datetime

assert os.path.isfile("logging.conf")
logging.config.fileConfig(fname="logging.conf", disable_existing_loggers=False)
logger = logging.getLogger("geodnLogger")


class CloudStorageFileReader:
    DATA = "data"

    def __init__(
        self,
        items: List[Dict[str, Any]],
        bands: List[str],
        bbox: Tuple[float, float, float, float],
        temporal_extent: Tuple[datetime, Optional[datetime]],
        dimension_map: Optional[Dict[str, str]],
    ) -> None:
        """

        Args:
            items (List[Dict[str, Any]]): items that match the criteria set by the user and grouped by the media type
            bands (List[str]): bands specified by the user
            bbox (Tuple[float, float, float, float]): bounding box specified by the user (west, south, north, east)
            temporal_extent (Tuple[datetime, datetime]): start and end.
        Returns:
            S3FileReader: S3FileReader instance
        """
        assert isinstance(items, list)
        assert len(items) > 0
        self.items = items
        self.bbox = bbox
        self.bands = bands
        if temporal_extent is not None and len(temporal_extent) > 0:
            # if temporal_extent is not empty tuple, then the first item cannot be None
            assert isinstance(temporal_extent[0], datetime)
            # the second item can be None for open intervals
            if temporal_extent[1] is not None:
                assert isinstance(temporal_extent[1], datetime)
                assert temporal_extent[0] <= temporal_extent[1]
        self.temporal_extent = temporal_extent
        self.dimension_map = dimension_map
        assets: Dict = items[0]["assets"]
        asset_values = next(iter(assets.values()))
        href = asset_values["href"]
        self.bucket = CloudStorageFileReader._extract_bucket_name_from_url(url=href)
        credentials = CloudStorageFileReader._get_credentials_by_bucket(
            bucket=self.bucket
        )

        self._endpoint = credentials["endpoint"]
        self.access_key_id = credentials["access_key_id"]
        self.secret_access_key = credentials["secret_access_key"]
        self.region = credentials["region"]

    @property
    def endpoint(self) -> str:
        return self._endpoint.lower()

    @property
    def start_datetime(self) -> datetime:
        return self.temporal_extent[0]

    @property
    def end_datetime(self) -> Optional[datetime]:
        return self.temporal_extent[1]

    def get_polygon(self) -> Polygon:
        """convert the bbox associated with this instance of the s3reader to a polygon

        Returns:
            Polygon: a polygon that is equivalent to the bbox set by the user
        """
        xmin, ymin, xmax, ymax = self.bbox
        poly = Polygon([[xmin, ymin], [xmin, ymax], [xmax, ymax], [xmax, ymin]])
        return poly

    def _create_boto3_session(
        self,
    ) -> Session:
        session = Session(
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key,
            region_name=self.region,
        )
        return session

    @staticmethod
    def _get_credentials_by_bucket(bucket: str) -> Dict[str, str]:
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

    @staticmethod
    def _get_dimension_description(item: pystac.Item, axis: str) -> Optional[str]:
        item_prop = item.properties
        cube_dims: Dict[str, Any] = item_prop["cube:dimensions"]
        for key, value in cube_dims.items():
            if value.get("axis") is not None and value.get("axis") == axis:
                return key
        return None

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

    @staticmethod
    def _convert_https_to_s3(url: str) -> str:
        """convert a https url to s3

        Args:
            url (str): link to data on COS using https scheme

        Returns:
            str: link to data on COS using s3 scheme
        """

        bucket = CloudStorageFileReader._extract_bucket_name_from_url(url=url)
        object = CloudStorageFileReader._get_object(url=url)
        url = f"s3://{bucket}/{object}"
        return url

    def create_s3filesystem(
        self,
    ) -> s3fs.S3FileSystem:
        """create a s3filesystem object

        Args:
            endpoint (str): endpoint to s3
            access_key_id (str): key
            secret (str): secret

        Returns:
            s3fs.S3FileSystem: _description_
        """
        if self.endpoint.startswith("https://"):
            endpoint_url = self.endpoint
        else:
            endpoint_url = f"https://{self.endpoint}"
        fs = s3fs.S3FileSystem(
            anon=False,
            endpoint_url=endpoint_url,
            key=self.access_key_id,
            secret=self.secret_access_key,
        )
        return fs

    @staticmethod
    def _get_dimension_name(
        item: Dict[str, Any],
        axis: Optional[str] = None,
        dim_type: Optional[str] = None,
    ) -> str:
        """get dimension name of the specified axis or the specified dim_type. Otherwise, it throws an
        exception

        Args:
            item (Dict[str, Any]): STAC item
            axis (Optional[str], optional): axis name (e.g., x, y)
            dim_type (Optional[str], optional): dimension type (e.g., temporal, spatial)

        Raises:
            ValueError: _description_
            ValueError: _description_
            ValueError: _description_

        Returns:
            str: dimension name
        """
        item_properties = item["properties"]
        cube_dims = item_properties["cube:dimensions"]
        assert isinstance(cube_dims, dict), f"Error! Unexpected type: {cube_dims}"
        assert axis is not None or dim_type is not None
        found = None
        i = 0
        dim_list = list(cube_dims.items())
        dimension_name = None
        while i < len(dim_list) and not found:
            k, v = dim_list[i]
            i += 1
            original_axis = v.get("axis")
            if axis is not None and original_axis is not None and original_axis == axis:
                dimension_name = k
                found = True
            if (
                dim_type is not None
                and v.get("type") is not None
                and v.get("type") == dim_type
            ):
                dimension_name = k
                found = True
        if found and isinstance(dimension_name, str):
            return dimension_name
        else:
            raise ValueError(
                f"Error! Unable to dimension name - axis={axis} dim_type={dim_type}"
            )
