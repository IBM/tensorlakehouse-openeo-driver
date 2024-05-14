from tensorlakehouse_openeo_driver.dataset import DatasetMetadata
from tensorlakehouse_openeo_driver.geodn_discovery import GeoDNDiscovery
import pytest
from tensorlakehouse_openeo_driver.constants import (
    GEODN_DISCOVERY_PASSWORD,
    GEODN_DISCOVERY_USERNAME,
)
from tensorlakehouse_openeo_driver.layer import LayerMetadata


@pytest.fixture
def geodn_data():
    return GeoDNDiscovery(
        password=GEODN_DISCOVERY_PASSWORD,
        client_id=GEODN_DISCOVERY_USERNAME,
    )


@pytest.mark.parametrize(
    "dataset_id, geodn_data",
    [(177, "geodn_data")],
    indirect=["geodn_data"],
)
def test_get_dataset(dataset_id: str, geodn_data: GeoDNDiscovery):
    dataset = geodn_data.get_dataset(dataset_id=dataset_id)
    assert isinstance(dataset, DatasetMetadata)


def test_list_datasets(geodn_data: GeoDNDiscovery):
    datasets = geodn_data.list_datasets()
    assert isinstance(datasets, list)
    for d in datasets:
        assert isinstance(d, DatasetMetadata)


@pytest.mark.parametrize(
    "dataset_id, geodn_data", [("177", "geodn_data")], indirect=["geodn_data"]
)
def test_list_datalayers_from_dataset(dataset_id: str, geodn_data: GeoDNDiscovery):
    data = geodn_data.list_datalayers_from_dataset(dataset_id=dataset_id)
    assert isinstance(data, list)
    assert len(data) > 0
    for layer in data:
        assert isinstance(layer, LayerMetadata)
