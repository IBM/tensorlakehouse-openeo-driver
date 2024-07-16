import ibm_boto3
from ibm_botocore.config import Config
from botocore.exceptions import ClientError
import os
from pathlib import Path
import logging
import logging.config

from urllib.parse import urlparse

from tensorlakehouse_openeo_driver.util.object_storage_util import (
    get_credentials_by_bucket,
)


assert os.path.isfile("logging.conf")
logging.config.fileConfig(fname="logging.conf", disable_existing_loggers=False)
logger = logging.getLogger("geodnLogger")


class COSConnector:
    DATA = "data"

    def __init__(self, bucket: str) -> None:
        self.bucket = bucket

        credentials = get_credentials_by_bucket(bucket=bucket)
        self._endpoint = credentials["endpoint"]
        self.access_key_id = credentials["access_key_id"]
        self.secret_access_key = credentials["secret_access_key"]

    @property
    def endpoint(self) -> str:
        return self._endpoint.lower()

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

    def get_bucket_contents_v2(self, max_keys):
        """list bucket contents and

        https://cloud.ibm.com/apidocs/cos/cos-compatibility?code=python#listobjects

        Args:
            max_keys (_type_): _description_

        Raises:
            e: _description_

        Returns:
            _type_: _description_
        """
        bucket_name = self.bucket
        print("Retrieving bucket contents from: {0}".format(bucket_name))
        try:
            # create client object
            cos_cli = ibm_boto3.client(
                "s3",
                endpoint_url=f"https://{self.endpoint}",
                aws_access_key_id=self.access_key_id,
                aws_secret_access_key=self.secret_access_key,
                verify=False,
                config=Config(tcp_keepalive=True),
            )
            more_results = True
            next_token = ""

            while more_results:
                response = cos_cli.list_objects_v2(
                    Bucket=bucket_name, MaxKeys=max_keys, ContinuationToken=next_token
                )
                files = response["Contents"]
                for file in files:
                    print("Item: {0} ({1} bytes).".format(file["Key"], file["Size"]))

                if response["IsTruncated"]:
                    next_token = response["NextContinuationToken"]
                    print("...More results in next batch!\n")
                else:
                    more_results = False
                    next_token = ""

        except ClientError as be:
            print("CLIENT ERROR: {0}\n".format(be))
        except Exception as e:
            print("Unable to retrieve bucket contents: {0}".format(e))

    def get_bucket_contents(self, max_keys: int = 10):
        """
        https://cloud.ibm.com/apidocs/cos/cos-compatibility?code=python#listobjects
        """
        print("Retrieving bucket contents from: {0}".format(self.bucket))
        try:
            s3 = ibm_boto3.resource(
                "s3",
                endpoint_url=f"https://{self.endpoint}",
                aws_access_key_id=self.access_key_id,
                aws_secret_access_key=self.secret_access_key,
                verify=False,
                config=Config(tcp_keepalive=True),
            )
            files = s3.Bucket(self.bucket).objects.all()

            i = 0
            file_list = list(files)
            while i < len(file_list) and i < max_keys:
                file = file_list[i]
                i += 1
                print("Item: {0}".format(file.key))
        except ClientError as be:
            print("CLIENT ERROR: {0}\n".format(be))
        except Exception as e:
            print("Unable to retrieve bucket contents: {0}".format(e))

    def upload_fileobj(
        self,
        key: str,
        path: Path,
    ):
        """upload file to COS

        based on https://ibm.github.io/ibm-cos-sdk-python/reference/services/s3.html#S3.Object.upload_fileobj

        Args:
            key (str): filename
            path (Path): local path
        """
        logger.debug(f"Upload file to COS: {key=} {path=} {self.bucket=}")
        s3 = ibm_boto3.resource(
            "s3",
            endpoint_url=f"https://{self.endpoint}",
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key,
            verify=False,
            config=Config(tcp_keepalive=True),
        )
        bucket_obj = s3.Bucket(self.bucket)
        obj = bucket_obj.Object(key)

        with open(path, "rb") as data:
            obj.upload_fileobj(data)
        logger.debug(f"File {key=} has been uploaded")

    def create_presigned_link(self, key: str, expiration: int = 3600) -> str:
        """Generate a presigned URL for the S3 object
        based on https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-presigned-urls.html
        Args:
            key (str): full path to object
            expiration (int, optional): _description_. Defaults to 3600.

        Returns:
            Optional[str]: pre-signed url
        """
        logger.debug(f"Create presigned link: {self.bucket=} {key=}")
        s3_client = self._make_ibm_boto3_client(
            endpoint=self.endpoint,
            access_key_id=self.access_key_id,
            secret=self.secret_access_key,
        )
        try:
            response = s3_client.generate_presigned_url(
                "get_object",
                Params={"Bucket": self.bucket, "Key": key},
                ExpiresIn=expiration,
            )
        except ClientError as e:
            logging.error(e)
            raise e

        # The response contains the presigned URL
        assert isinstance(response, str), f"Error! Unexpected response: {response}"
        return response
