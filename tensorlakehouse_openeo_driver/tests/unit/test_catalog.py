from pystac_client import Client
from tensorlakehouse_openeo_driver.tensorlakehouse_backend import TensorLakehouseCollectionCatalog
from unittest.mock import patch
import pytest
from tensorlakehouse_openeo_driver.tests.unit.unit_test_util import (
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
            TensorLakehouseCollectionCatalog,
            "get_collection_items",
            return_value=collection_items,
        ):
            catalog = TensorLakehouseCollectionCatalog()
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
        catalog = TensorLakehouseCollectionCatalog()
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
