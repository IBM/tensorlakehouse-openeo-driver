from typing import Any, Dict, List, Optional, Tuple
import xarray as xr
from tensorlakehouse_openeo_driver.constants import (
    DEFAULT_X_DIMENSION,
    DEFAULT_Y_DIMENSION,
)

import os
from datetime import datetime
import logging
from tensorlakehouse_openeo_driver.file_reader.cloud_storage_file_reader import (
    CloudStorageFileReader,
)

assert os.path.isfile("logging.conf")
logging.config.fileConfig(fname="logging.conf", disable_existing_loggers=False)
logger = logging.getLogger("geodnLogger")


class ZarrFileReader(CloudStorageFileReader):
    def __init__(
        self,
        items: List[Dict[str, Any]],
        bands: List[str],
        bbox: Tuple[float, float, float, float],
        temporal_extent: Tuple[datetime, Optional[datetime]],
        dimension_map: Optional[Dict[str, str]],
    ) -> None:
        super().__init__(
            items=items,
            bbox=bbox,
            bands=bands,
            temporal_extent=temporal_extent,
            dimension_map=dimension_map,
        )

    def load_items(
        self,
    ) -> xr.DataArray:
        """create a raster datacube by loading zarr store

        Returns:
            xr.DataArray: raster datacube
        """
        assert (
            len(self.items) == 1
        ), f"Error! Number of items must be 1, got: {len(self.items)}"
        item = self.items[0]
        assets: Dict[str, Any] = item["assets"]
        assert isinstance(assets, dict)
        asset_value = next(iter(assets.values()))
        href = asset_value["href"]
        dataset = xr.open_dataset(
            href,
            engine="zarr",
            storage_options={
                "key": self.access_key_id,
                "secret": self.secret_access_key,
                "client_kwargs": {"endpoint_url": self._endpoint},
            },
        )

        t_axis_name = CloudStorageFileReader._get_dimension_name(
            item=item, dim_type="temporal"
        )
        x_axis_name = CloudStorageFileReader._get_dimension_name(
            item=item, axis=DEFAULT_X_DIMENSION
        )
        y_axis_name = CloudStorageFileReader._get_dimension_name(
            item=item, axis=DEFAULT_Y_DIMENSION
        )

        # filter by temporal_extent
        dataset = dataset.loc[
            {t_axis_name: slice(self.temporal_extent[0], self.temporal_extent[1])}
        ][self.bands]

        # Filter by spatial extent
        dataset = dataset.loc[{x_axis_name: slice(self.bbox[0], self.bbox[2])}]
        dataset = dataset.loc[{y_axis_name: slice(self.bbox[1], self.bbox[3])}]

        dataarray = dataset.to_array()

        return dataarray
