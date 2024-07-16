import os
from typing import Dict
import logging
import logging.config

assert os.path.isfile("logging.conf")
logging.config.fileConfig(fname="logging.conf", disable_existing_loggers=False)
logger = logging.getLogger("geodnLogger")


def get_credentials_by_bucket(bucket: str) -> Dict[str, str]:
    """get the credentials to access the specified bucket

    Args:
        bucket (str): input bucket name

    Returns:
        Dict[str, str]: a dict that contains endpoint, access_key_id, secret_access_key, region,
            endpoint
    """
    # make sure the bucket is valid
    assert bucket is not None
    assert isinstance(bucket, str)
    # create the environment variable name, which is based on the bucket name
    core_var_name = convert_bucket_to_envvar(bucket=bucket)
    prefix = "TLH_"
    access_key_id_env_var = f"{prefix}{core_var_name}_ACCESS_KEY_ID"
    secret_access_key_env_var = f"{prefix}{core_var_name}_SECRET_ACCESS_KEY"
    endpoint_env_var = f"{prefix}{core_var_name}_ENDPOINT"
    try:
        # get the credential values
        access_key_id = os.environ[access_key_id_env_var]
        secret_access_key = os.environ[secret_access_key_env_var]
        endpoint = os.environ[endpoint_env_var]
    except KeyError as e:
        msg = f"Error! Environment variable that grants access to bucket {bucket} has not been set. {e}"
        logger.error(msg=msg)
        raise KeyError(msg)
    credentials = {
        "access_key_id": access_key_id,
        "secret_access_key": secret_access_key,
        "endpoint": endpoint,
    }
    return credentials


def parse_region(endpoint: str) -> str:
    """extract region from endpoint

    Args:
        endpoint (str): e.g., s3.us-south.cloud-object-storage.appdomain.cloud

    Returns:
        str: region, e.g., us-south
    """
    fields = endpoint.split(".")
    assert len(fields) > 0, f"Error! Unexpected endpoint: {endpoint}"
    region = fields[1]
    assert isinstance(region, str), f"Error! Unexpected region type: {region=}"
    return region


def convert_bucket_to_envvar(bucket: str) -> str:
    """convert bucket name to env var name, i.e., remove non-alpha-numeric characters except for
    underline

    Args:
        bucket (str): _description_

    Returns:
        str: core part of env var
    """
    env_var = bucket.upper()
    env_var = "".join([i if str.isalnum(i) or i == "_" else "" for i in env_var])
    return env_var


if __name__ == "__main__":
    convert_bucket_to_envvar(bucket="")
