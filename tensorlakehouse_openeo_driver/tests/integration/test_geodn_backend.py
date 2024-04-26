"""
integration tests
"""

from tensorlakehouse_openeo_driver.geodn_backend import GeoDNCollectionCatalog
import pytest
from tensorlakehouse_openeo_driver.tests.unit.unit_test_util import validate_STAC_Collection


@pytest.mark.skip("Test case is broken. It should validate intermediary metadata dict")
@pytest.mark.parametrize("collection_id", ["177"])
def test_get_collection_metadata(collection_id: str):
    catalog = GeoDNCollectionCatalog()
    metadata = catalog.get_collection_metadata(collection_id=collection_id)
    assert isinstance(metadata, dict)
    validate_STAC_Collection(metadata_item=metadata)


@pytest.mark.skip("Test case is broken. It should validate intermediary metadata dict")
def test_get_all_metadata():
    catalog = GeoDNCollectionCatalog()
    metadata = catalog.get_all_metadata()
    assert isinstance(metadata, list)
    assert len(metadata) > 0
    for metadata_item in metadata:
        validate_STAC_Collection(metadata_item=metadata_item)
