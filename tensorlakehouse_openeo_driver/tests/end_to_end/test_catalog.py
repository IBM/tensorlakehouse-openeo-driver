from typing import Dict
import pytest
import pandas as pd
import requests
from tensorlakehouse_openeo_driver.tests.unit.unit_test_util import (
    validate_OpenEO_collection,
)
from tensorlakehouse_openeo_driver.constants import OPENEO_URL
import certifi
from tensorlakehouse_openeo_driver.constants import logger

COLLECTION_ID_ERA5 = "Global weather (ERA5)"
COLLECTION_IBM_EIS_GA_1_ESA = "ibm-eis-ga-1-esa-sentinel-2-l2a"
COLLECTION_ID_HLSS30 = "HLSS30"

HEADERS = {"Content-type": "application/json"}


@pytest.mark.parametrize(
    "collection_id",
    [COLLECTION_ID_HLSS30],
)
def test_get_collection_metadata(collection_id: str):
    url = f"{OPENEO_URL}collections/{collection_id}"
    certificate = certifi.where()
    resp = requests.get(url=url, headers=HEADERS, verify=certificate)
    assert resp.status_code == 200
    collection = resp.json()
    resp_coll_id = collection["id"]
    assert (
        resp_coll_id == collection_id
    ), f"Error! Invalid collection_id: {collection_id} != {resp_coll_id}"
    validate_OpenEO_collection(collection=collection, full=True)


def test_get_all_metadata():
    url = f"{OPENEO_URL}collections"
    logger.debug(f"url={url} headers={HEADERS}")
    resp = requests.get(url=url, headers=HEADERS, verify=False)
    assert resp.status_code == 200, f"Error! URL={url} headers={HEADERS}"
    response = resp.json()
    assert isinstance(response, dict)
    collections = response["collections"]
    assert len(collections) > 0
    for collection in collections:
        validate_OpenEO_collection(collection=collection)


@pytest.mark.parametrize(
    "collection_id, params",
    [
        (
            COLLECTION_ID_HLSS30,
            {
                "limit": 100,
                "bbox": "[-99.0, 28.0, -98.0, 28.5]",
                "datetime": "2022-10-06/2022-10-09",
            },
        ),
        (
            COLLECTION_ID_ERA5,
            {
                "limit": 100,
                "bbox": "[-122.0, 34.0, -120.0, 36.0]",
                "datetime": "2020-09-01/2020-09-02",
            },
        ),
    ],
)
def test_get_collection_items(collection_id: str, params: Dict[str, int]):
    url = f"{OPENEO_URL}collections/{collection_id}/items"
    resp = requests.get(url=url, params=params, headers=HEADERS, verify=False)
    assert resp.status_code == 200, f"Error! URL={url} headers={HEADERS}"
    response = resp.json()
    assert isinstance(response, dict), f"Error! Unexpected response: {type(response)}"
    items = response["features"]
    print(items)
    assert len(items) > 0
    for item in items:
        properties = item.get("properties")
        assert isinstance(properties, dict), f"not a dict {properties}"
        assert all(
            f in properties.keys() for f in ["cube:dimensions", "cube:variables", "datetime"]
        )
        datetime = properties.get("datetime")
        assert isinstance(datetime, str), f"Error! not a str {datetime}"
        pd.Timestamp(datetime)
