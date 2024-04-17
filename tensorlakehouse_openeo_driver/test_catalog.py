from pystac import Collection
from pystac_client import Client
from openeo_geodn_driver.geodn_backend import GeoDNCollectionCatalog
from unittest.mock import patch
import pytest
from openeo_geodn_driver.tests.unit.unit_test_util import (
    MockPystacClient,
    get_collection_items,
)


def test_get_all_metadata():
    with patch.object(
        Client,
        "open",
        return_value=MockPystacClient(),
    ):
        collection_items = get_collection_items(collection_id="", parameters=None)
        with patch.object(
            GeoDNCollectionCatalog,
            "get_collection_items",
            return_value=collection_items,
        ):
            catalog = GeoDNCollectionCatalog()
            metadata = catalog.get_all_metadata()
            assert isinstance(metadata, list)
            assert len(metadata) > 0
            mandatory_fields = ["id", "extent", "description", "title", "license"]
            for m in metadata:
                assert all(f in m.keys() for f in mandatory_fields)


@pytest.mark.parametrize("collection_id", ["dset_id"])
def test_get_collection_metadata(collection_id: str):
    with patch.object(
        Client,
        "open",
        return_value=MockPystacClient(),
    ):
        catalog = GeoDNCollectionCatalog()
        collection_metadata = catalog.get_collection_metadata(
            collection_id=collection_id
        )
        assert isinstance(collection_metadata, dict)
        mandatory_fields = [
            "id",
            "extent",
            "description",
            "title",
            "license",
            "cube:dimensions",
        ]
        assert all(f in collection_metadata.keys() for f in mandatory_fields)
