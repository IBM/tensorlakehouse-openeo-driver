from typing import Any, Dict, List
from tensorlakehouse_openeo_driver.catalog import GeoDNCollectionCatalog
import pytest
import pandas as pd
from tensorlakehouse_openeo_driver.constants import STAC_URL
from tensorlakehouse_openeo_driver.tests.unit.unit_test_util import (
    validate_OpenEO_collection,
)
from tensorlakehouse_openeo_driver.stac import STAC

COLLECTION_ID_ERA5 = "Global weather (ERA5)"


@pytest.fixture(scope="module")
def catalog():
    return GeoDNCollectionCatalog()


@pytest.mark.parametrize(
    "catalog, collection_id",
    [
        ("catalog", "HLSS30"),
        ("catalog", "HLSL30"),
        ("catalog", "Global weather (ERA5)"),
    ],
    indirect=["catalog"],
)
def test_get_collection_metadata(catalog, collection_id: str):
    stac = STAC(STAC_URL)
    if stac.is_collection_available(collection_id=collection_id):
        collection = catalog.get_collection_metadata(collection_id=collection_id)
        assert isinstance(collection, dict)
        assert all(f in collection.keys() for f in ["cube:dimensions"])
        validate_OpenEO_collection(collection=collection)
    else:
        pytest.skip(f"Warning! {collection_id} collection is not available")


@pytest.mark.parametrize(
    "catalog",
    ["catalog"],
    indirect=["catalog"],
)
def test_get_all_metadata(catalog: GeoDNCollectionCatalog):
    collections = catalog.get_all_metadata()
    for collection in collections:
        assert isinstance(collection, dict)
        validate_OpenEO_collection(collection=collection)


@pytest.mark.parametrize(
    "collection_id, params, catalog",
    [
        (COLLECTION_ID_ERA5, {"max_items": 10}, "catalog"),
        (COLLECTION_ID_ERA5, {"max_items": 10, "bbox": [-91, 40, -90, 41]}, "catalog"),
        ("HLSS30", {"max_items": 10}, "catalog"),
        ("HLSL30", {"max_items": 10}, "catalog"),
    ],
    indirect=["catalog"],
)
def test_get_collection_items(
    collection_id: str, params: Dict[str, Any], catalog: GeoDNCollectionCatalog
):
    stac = STAC(STAC_URL)
    if stac.is_collection_available(collection_id=collection_id):
        feature_collection = catalog.get_collection_items(
            collection_id=collection_id, parameters=params
        )
        assert isinstance(feature_collection, dict)
        items: List[Dict[str, Any]] = feature_collection["features"]
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
    else:
        pytest.skip(f"Warning! {collection_id} collection is not available")
