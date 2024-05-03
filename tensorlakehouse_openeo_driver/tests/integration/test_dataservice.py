from tensorlakehouse_openeo_driver.constants import (
    DEFAULT_BANDS_DIMENSION,
    DEFAULT_TIME_DIMENSION,
    DEFAULT_X_DIMENSION,
    DEFAULT_Y_DIMENSION,
)
from tensorlakehouse_openeo_driver.process_implementations.load_collection import (
    LoadCollectionFromHBase,
)
import pytest
from openeo_pg_parser_networkx.pg_schema import (
    BoundingBox,
)
import pandas as pd


class MockTemporalInterval:
    def __init__(self, start: pd.Timestamp, end: pd.Timestamp) -> None:
        self.start = start
        self.end = end


INPUT_PARAMS = [
    (
        "CEH gridded hourly rainfall for Great Britain",
        BoundingBox(west=-0.48, south=53.709, east=-0.22, north=53.812),
        MockTemporalInterval(
            start=pd.Timestamp("2007-01-01T11:00:00Z"),
            end=pd.Timestamp("2007-01-07T11:00:00Z"),
        ),
        ["CEH rainfall for Great Britain"],
        None,
    ),
    (
        "global-weather-era5",
        BoundingBox(west=-0.48, south=53.709, east=-0.22, north=53.812),
        MockTemporalInterval(
            start=pd.Timestamp("2007-01-01T11:00:00Z"),
            end=pd.Timestamp("2007-01-07T11:00:00Z"),
        ),
        ["Temperature"],
        None,
    ),
]


@pytest.mark.parametrize(
    "collection_id, spatial_extent, temporal_extent, bands, properties", INPUT_PARAMS
)
def test_load_collection_from_hbase(
    collection_id, spatial_extent, temporal_extent, bands, properties
):
    loader = LoadCollectionFromHBase()
    data = loader.load_collection(
        id=collection_id,
        spatial_extent=spatial_extent,
        temporal_extent=temporal_extent,
        bands=bands,
        properties=properties,
        dimensions={
            DEFAULT_TIME_DIMENSION: DEFAULT_TIME_DIMENSION,
            DEFAULT_X_DIMENSION: DEFAULT_X_DIMENSION,
            DEFAULT_Y_DIMENSION: DEFAULT_Y_DIMENSION,
        },
    )
    data = data.compute()
    for band_index in range(data[DEFAULT_BANDS_DIMENSION].size):
        avg_list = list()
        for t_index in range(data[DEFAULT_TIME_DIMENSION].size):
            mean_data = data.isel(
                {DEFAULT_TIME_DIMENSION: t_index, DEFAULT_BANDS_DIMENSION: band_index}
            ).mean()
            avg_list.append(float(mean_data.values))
        assert len(set(avg_list)) > 1, f"Error! {avg_list}"
