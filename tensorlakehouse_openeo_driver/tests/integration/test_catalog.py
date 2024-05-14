from typing import Any, Dict, List
from tensorlakehouse_openeo_driver.catalog import TensorLakehouseCollectionCatalog
import pytest
import pandas as pd
from tensorlakehouse_openeo_driver.constants import STAC_URL
from tensorlakehouse_openeo_driver.tests.unit.unit_test_util import (
    validate_OpenEO_collection,
)
from tensorlakehouse_openeo_driver.stac import STAC
from stac_validator import stac_validator

COLLECTION_ID_ERA5 = "Global weather (ERA5)"


@pytest.fixture(scope="module")
def catalog():
    return TensorLakehouseCollectionCatalog()


@pytest.mark.parametrize(
    "catalog, collection_id",
    [
        ("catalog", "HLSS30"),
        ("catalog", "HLSL30"),
        ("catalog", "Global weather (ERA5)"),
        ("catalog", "dev_ne_10m_admin_0_countries"),
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
def test_get_all_metadata(catalog: TensorLakehouseCollectionCatalog):
    collections = catalog.get_all_metadata()
    for collection in collections:
        assert isinstance(collection, dict)
        validate_OpenEO_collection(collection=collection)


@pytest.mark.parametrize(
    "collection_id, params, catalog",
    [
        (COLLECTION_ID_ERA5, {"max_items": 10}, "catalog"),
        (COLLECTION_ID_ERA5, {"bbox": [-91, 40, -90, 41]}, "catalog"),
        (
            "HLSS30",
            {
                "bbox": "[-124.1527514, 37.8593897, -122.8890464, 38.8433101]",
                "datetime": "2021-08-14T19:13:54Z/2021-08-16T19:13:54Z",
                "limit": 100,
            },
            "catalog",
        ),
    ],
    indirect=["catalog"],
)
def test_get_collection_items(
    collection_id: str,
    params: Dict[str, Any],
    catalog: TensorLakehouseCollectionCatalog,
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
            validator = stac_validator.StacValidate(extensions=True)
            validator.validate_dict(item)
            for m in validator.message:
                assert m["valid_stac"], f"Error! message={m}"
            properties = item.get("properties")
            assert isinstance(properties, dict), f"not a dict {properties}"
            assert all(
                f in properties.keys() for f in ["cube:dimensions", "cube:variables"]
            ), f"Error! {collection_id} is invalid! {properties}"
            assert (
                properties.get("datetime") is not None
                or properties.get("start_datetime") is not None
            )
            if properties.get("datetime") is not None:
                dt_str = properties.get("datetime")
                assert isinstance(dt_str, str), f"Error! not a str {dt_str}"
                # this must not raise an exception
                pd.Timestamp(dt_str)
            else:
                start_datetime_str = properties.get("start_datetime")
                assert isinstance(
                    start_datetime_str, str
                ), f"Error! not a str {start_datetime_str}"
                pd.Timestamp(start_datetime_str)
    else:
        pytest.skip(f"Warning! {collection_id} collection is not available")
