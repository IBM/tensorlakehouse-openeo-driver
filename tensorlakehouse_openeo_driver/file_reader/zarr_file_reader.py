from typing import Any, Dict, List, Optional, Tuple
import xarray as xr
from tensorlakehouse_openeo_driver.constants import (
    DEFAULT_BANDS_DIMENSION,
    DEFAULT_X_DIMENSION,
    DEFAULT_Y_DIMENSION,
)

import os
from datetime import datetime
import logging
from tensorlakehouse_openeo_driver.file_reader.cloud_storage_file_reader import (
    CloudStorageFileReader,
)
from tensorlakehouse_openeo_driver.geospatial_utils import (
    filter_by_time,
    reproject_bbox,
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
        properties: Optional[Dict[str, Any]],
    ) -> None:
        super().__init__(
            items=items,
            bbox=bbox,
            bands=bands,
            temporal_extent=temporal_extent,
            properties=properties,
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
        s3_link = self._convert_https_to_s3(url=href)
        fs = self.create_s3filesystem()
        store = fs.get_mapper(s3_link)
        # store = s3fs.S3Map(root=s3_link, s3=fs)
        dataset = xr.open_zarr(store=store)

        t_axis_name = CloudStorageFileReader._get_dimension_name(
            item=item, dim_type="temporal"
        )
        x_axis_name = CloudStorageFileReader._get_dimension_name(
            item=item, axis=DEFAULT_X_DIMENSION
        )
        y_axis_name = CloudStorageFileReader._get_dimension_name(
            item=item, axis=DEFAULT_Y_DIMENSION
        )

        array = dataset[self.bands]
        array = array.to_array(dim=DEFAULT_BANDS_DIMENSION)
        # filter by temporal_extent
        if t_axis_name is not None:
            array = filter_by_time(
                data=array,
                temporal_extent=self.temporal_extent,
                temporal_dim=t_axis_name,
            )

        # Filter by spatial extent
        crs_code = CloudStorageFileReader._get_epsg(item=item)
        assert isinstance(
            crs_code, int
        ), f"Error! crs_code is not an int: {type(crs_code)}"
        reprojected_bbox = reproject_bbox(
            bbox=self.bbox, src_crs=4326, dst_crs=crs_code
        )
        epsilon = 1e-8
        array = array.loc[
            {x_axis_name: slice(reprojected_bbox[0], reprojected_bbox[2] + epsilon)}
        ]
        array = array.loc[
            {y_axis_name: slice(reprojected_bbox[1], reprojected_bbox[3] + epsilon)}
        ]
        assert isinstance(array, xr.DataArray)
        return array
