import logging
import logging.config
import os
from pathlib import Path
from tensorlakehouse_openeo_driver.util.credentials_manager import decode_credential

# set URL of STAC service, which provides collections and items
STAC_URL = os.environ["STAC_URL"]
assert STAC_URL is not None
assert isinstance(STAC_URL, str)

LOGGING_CONF_PATH = Path(__file__).parent.parent / "logging.conf"
assert LOGGING_CONF_PATH.exists()
logging.config.fileConfig(fname=LOGGING_CONF_PATH, disable_existing_loggers=False)
logger = logging.getLogger("geodnLogger")

# aka PAIRS API key
GEODN_DISCOVERY_PASSWORD = os.getenv("GEODN_DISCOVERY_PASSWORD")
GEODN_DISCOVERY_USERNAME = os.getenv("GEODN_DISCOVERY_USERNAME")
# aka DATASERVICE endpoint
GEODN_DATASERVICE_ENDPOINT_DEFAULT = "https://pairs.res.ibm.com/pairsdataservice"
GEODN_DATASERVICE_ENDPOINT = os.getenv(
    "GEODN_DATASERVICE_ENDPOINT", GEODN_DATASERVICE_ENDPOINT_DEFAULT
)
GEODN_DATASERVICE_USER = os.getenv("GEODN_DATASERVICE_USER", "")
GEODN_DATASERVICE_PASSWORD = os.getenv("GEODN_DATASERVICE_PASSWORD", "")

# appId credentials
APPID_ISSUER = os.getenv("APPID_ISSUER")
OPENEO_AUTH_CLIENT_ID = os.getenv("OPENEO_AUTH_CLIENT_ID")
OPENEO_AUTH_CLIENT_SECRET = os.getenv("OPENEO_AUTH_CLIENT_SECRET")
APPID_USERNAME = os.getenv("APPID_USERNAME")
APPID_PASSWORD = os.getenv("APPID_PASSWORD")


TENSORLAKEHOUSE_OPENEO_DRIVER_PORT = os.getenv(
    "TENSORLAKEHOUSE_OPENEO_DRIVER_PORT", 9091
)
DASK_SCHEDULER_ADDRESS = os.getenv("DASK_SCHEDULER_ADDRESS", "http://127.0.0.1:8787")

TENSORLAKEHOUSE_OPENEO_DRIVER_ROOT_DIR = Path(__file__).parent.parent.resolve()
TEST_DATA_ROOT = (
    TENSORLAKEHOUSE_OPENEO_DRIVER_ROOT_DIR
    / "tensorlakehouse_openeo_driver"
    / "tests"
    / "test_data"
)
if not TEST_DATA_ROOT.exists():
    TEST_DATA_ROOT.mkdir()


TENSORLAKEHOUSE_OPENEO_DRIVER_DATA_DIR = TENSORLAKEHOUSE_OPENEO_DRIVER_ROOT_DIR / "data"
if not TENSORLAKEHOUSE_OPENEO_DRIVER_DATA_DIR.exists():
    TENSORLAKEHOUSE_OPENEO_DRIVER_DATA_DIR.mkdir()


# RasterCube/DataArray dimensions
# how stackstac name these dimensions https://stackstac.readthedocs.io/en/latest/api/main/stackstac.stack.html#stackstac.stack
DEFAULT_X_DIMENSION = "x"
DEFAULT_Y_DIMENSION = "y"
DEFAULT_Z_DIMENSION = "z"
DEFAULT_TIME_DIMENSION = "t"
DEFAULT_BANDS_DIMENSION = "bands"
STACKSTAC_TIME = "time"

# stac datetime format for items search
STAC_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S.000Z"

# this env var sets the system under test
OPENEO_URL = os.getenv("OPENEO_URL")
if (
    OPENEO_URL is not None
    and isinstance(OPENEO_URL, str)
    and not OPENEO_URL.endswith("/")
):
    OPENEO_URL += "/"
OPENEO_USERNAME = os.getenv("OPENEO_USERNAME", None)
OPENEO_PASSWORD = os.getenv("OPENEO_PASSWORD", None)

CREDENTIALS = decode_credential(os.environ["CREDENTIALS"])

# list of media types for STAC
# https://github.com/radiantearth/stac-spec/blob/master/best-practices.md#working-with-media-types
ZIP_ZARR_MEDIA_TYPE = "application/zip+zarr"
NETCDF_MEDIA_TYPE = "application/netcdf"
COG_MEDIA_TYPE = "image/tiff; application=geotiff; profile=cloud-optimized"
JPG2000_MEDIA_TYPE = "image/jp2"
GEOTIFF_MEDIA_TYPE = "image/tiff; application=geotiff"

# default reference system
EPSG_4326 = "EPSG:4326"

# internal names of formats to save data
NETCDF = "NETCDF"
JSON = "JSON"
GEOJSON = "GEOJSON"
GTIFF = "GTIFF"
ZIP = "ZIP"
GEOTIFF_PREFIX = "openeo_output_"
FILE_DATETIME_FORMAT = "%Y-%m-%dT%H-%M-%SZ"

broker_url = os.getenv("BROKER_URL", "redis://:@0.0.0.0:6379/")
result_backend = os.getenv("RESULT_BACKEND", "redis://:@0.0.0.0:6379/")

assert broker_url is not None, "Error! BROKER_URL is None"
assert result_backend is not None, "Error! RESULT_BACKEND is None"

REDIS_CERT_NAME = os.getenv("REDIS_CERT_NAME")
REDIS_CERT = os.getenv("REDIS_CERT")
