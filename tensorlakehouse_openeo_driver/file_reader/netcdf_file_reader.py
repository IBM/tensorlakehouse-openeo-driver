from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from tensorlakehouse_openeo_driver.constants import (
    DEFAULT_X_DIMENSION,
    DEFAULT_Y_DIMENSION,
)
from tensorlakehouse_openeo_driver.file_reader.cloud_storage_file_reader import (
    CloudStorageFileReader,
)
import xarray as xr

from tensorlakehouse_openeo_driver.geospatial_utils import (
    clip,
    filter_by_time,
    reproject_bbox,
)


class NetCDFFileReader(CloudStorageFileReader):

    def __init__(
        self,
        items: List[Dict[str, Any]],
        bands: List[str],
        bbox: Tuple[float, float, float, float],
        temporal_extent: Tuple[datetime, Optional[datetime]],
        dimension_map: Optional[Dict[str, str]],
    ) -> None:
        super().__init__(items, bands, bbox, temporal_extent, dimension_map)

    def _concat_bucket_and_path(self, path) -> str:
        url = f"s3://{self.bucket}/{path}"
        return url

    def load_items(self) -> xr.DataArray:
        """load items that are associated with netcdf files

        Returns:
            xr.DataArray: raster data cube
        """
        s3fs = self.create_s3filesystem()
        da = None
        item = self.items[0]
        assets: Dict[str, Any] = item["assets"]
        asset_value = next(iter(assets.values()))
        href = asset_value["href"]

        s3_file_obj = s3fs.open(href, mode="rb")
        ds = xr.open_dataset(s3_file_obj, engine="h5netcdf")
        x_dim = CloudStorageFileReader._get_dimension_name(
            item=item, axis=DEFAULT_X_DIMENSION
        )
        y_dim = CloudStorageFileReader._get_dimension_name(
            item=item, axis=DEFAULT_Y_DIMENSION
        )
        time_dim = CloudStorageFileReader._get_dimension_name(
            item=item, dim_type="temporal"
        )
        crs_code = CloudStorageFileReader._get_epsg(item=item)
        assert isinstance(crs_code, int), f"Error! Invalid type: {crs_code=}"
        ds = ds[self.bands]
        da = ds.to_array()
        # filter by area of interest
        reprojected_bbox = reproject_bbox(
            bbox=self.bbox, src_crs=4326, dst_crs=crs_code
        )
        da = clip(
            data=da, bbox=reprojected_bbox, x_dim=x_dim, y_dim=y_dim, crs=crs_code
        )
        # remove timestamps that have not been selected by end-user
        da = filter_by_time(
            data=da, temporal_extent=self.temporal_extent, temporal_dim=time_dim
        )

        return da
