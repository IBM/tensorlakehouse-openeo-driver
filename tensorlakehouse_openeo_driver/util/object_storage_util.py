import os
from typing import Dict, Optional
import logging
import logging.config

assert os.path.isfile("logging.conf")
logging.config.fileConfig(fname="logging.conf", disable_existing_loggers=False)
logger = logging.getLogger("geodnLogger")


def get_credentials_by_bucket(bucket: str) -> Dict[str, Optional[str]]:
    """get the credentials to access the specified bucket. This method maps the bucket to a
    cos instance, then it gets the credentials to access this instance

    Args:
        bucket (str): input bucket name

    Returns:
        Dict[str, str]: a dict that contains endpoint, access_key_id, secret_access_key, region,
            endpoint
    """
    # make sure the bucket variable is valid
    assert bucket is not None
    assert isinstance(bucket, str)
    # create the environment variable name, which is based on the bucket name
    envvar = remove_invalid_characters(name=bucket)
    endpoint_env_var_name = f"{envvar}_ENDPOINT".upper()
    cos_instance_env_var_name = f"{envvar}_INSTANCE".upper()
    # if these env variables are set, it means that credentials are required
    if cos_instance_env_var_name in os.environ and endpoint_env_var_name in os.environ:
        # get COS instance name
        cos_instance = os.environ[cos_instance_env_var_name].upper()
        cos_instance = remove_invalid_characters(name=cos_instance)
        # get endpoint
        endpoint = os.environ[endpoint_env_var_name]
        # create env variable names based on COS instance name
        access_key_id_env_var = f"{cos_instance}_ACCESS_KEY_ID"
        secret_access_key_env_var = f"{cos_instance}_SECRET_ACCESS_KEY"
        logger.debug(
            f"Accessing env variables: {access_key_id_env_var=} {secret_access_key_env_var=}"
        )
        try:
            # get the credential values
            access_key_id = os.getenv(access_key_id_env_var)
            secret_access_key = os.getenv(secret_access_key_env_var)
        except KeyError as e:
            msg = f"KeyError! At least one of these variables ({access_key_id_env_var=}, {secret_access_key_env_var=}), which grant access to the {bucket} bucket,  has not been set. Message={e}"
            logger.error(msg=msg)
            raise KeyError(msg)
        # get endpoint value
    else:
        # if the dataset does not require credentials
        access_key_id = secret_access_key = endpoint = None
    # grouping credentials as dict
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


def remove_invalid_characters(name: str) -> str:
    """environment variables must have alpha-numeric characters and underscore. This function
    remove what is invalid

    Args:
        name (str): name of the bucket or instance

    Returns:
        str: core part of env var
    """
    assert isinstance(name, str), f"Error! {name=} is not a str"
    env_var = "".join([i if str.isalnum(i) or i == "_" else "" for i in name])
    return env_var


if __name__ == "__main__":
    buckets = ["sentinel-2", "sentinel-1", "hls", "sentinel2-l2a-jp2"]
    for bucket in buckets:
        env_var = get_credentials_by_bucket(bucket=bucket)
        assert isinstance(env_var, dict)
        for v in env_var.values():
            assert v is not None
