from tensorlakehouse_openeo_driver.constants import (
    DEFAULT_BANDS_DIMENSION,
    DEFAULT_TIME_DIMENSION,
    DEFAULT_X_DIMENSION,
    DEFAULT_Y_DIMENSION,
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
