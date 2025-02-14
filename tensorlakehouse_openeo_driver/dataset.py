from typing import Optional
import pandas as pd
from datetime import datetime


class DatasetMetadata:
    def __init__(
        self,
        dataset_id: str,
        latitude_min: Optional[float],
        latitude_max: Optional[float],
        longitude_min: Optional[float],
        longitude_max: Optional[float],
        name: str,
        level: int,
        temporal_min: Optional[int] = None,
        temporal_max: Optional[int] = None,
        temporal_resolution: Optional[str] = None,
        temporal_resolution_description: Optional[str] = None,
        spatial_resolution_of_raw_data: Optional[str] = None,
        description_short: Optional[str] = None,
        license: Optional[str] = None,
        crs: str = "EPSG:4326",
    ) -> None:
        assert dataset_id is not None
        self.dataset_id = str(dataset_id)

        self.latitude_min = latitude_min
        self.latitude_max = latitude_max
        if self.latitude_min is not None and self.latitude_max is not None:
            assert self.latitude_min <= self.latitude_max

        self.longitude_min = longitude_min
        self.longitude_max = longitude_max
        if self.longitude_min and self.longitude_max is not None:
            assert self.longitude_min <= self.longitude_max

        self._temporal_min = temporal_min
        self._temporal_max = temporal_max
        self.temporal_resolution = temporal_resolution
        self.temporal_resolution_description = temporal_resolution_description
        self.level = int(level)
        self.spatial_resolution_of_raw_data = spatial_resolution_of_raw_data
        self.name = name
        self.description_short = description_short
        self.license = license
        self.crs = crs

    @property
    def temporal_min(self) -> Optional[datetime]:
        if self._temporal_min is not None:
            dt = pd.to_datetime(self._temporal_min, unit="ms")
            assert isinstance(dt, datetime)
            return dt
        else:
            return None

    @property
    def temporal_max(self) -> Optional[datetime]:
        if self._temporal_max is not None:
            dt = pd.to_datetime(self._temporal_max, unit="ms")
            assert isinstance(dt, datetime)
            return dt
        else:
            return None

    @property
    def collection_id(self) -> str:
        return self.name
