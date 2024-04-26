from typing import Optional
import pandas as pd
from datetime import datetime


class DatasetMetadata:
    def __init__(
        self,
        dataset_id: str,
        latitude_min: float,
        latitude_max: float,
        longitude_min: float,
        name: str,
        level: int,
        longitude_max: float,
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

        self.latitude_min = float(latitude_min)
        self.latitude_max = float(latitude_max)
        assert self.latitude_min <= self.latitude_max

        self.longitude_min = longitude_min
        self.longitude_max = longitude_max
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
        self.crs = "EPSG:4326"

    @property
    def temporal_min(self) -> Optional[datetime]:
        if self._temporal_min is not None:
            return pd.to_datetime(self._temporal_min, unit="ms")
        else:
            return None

    @property
    def temporal_max(self) -> Optional[datetime]:
        if self._temporal_max is not None:
            return pd.to_datetime(self._temporal_max, unit="ms")
        else:
            return None

    @property
    def collection_id(self) -> str:
        return self.name
