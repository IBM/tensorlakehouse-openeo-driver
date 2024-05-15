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

from tensorlakehouse_openeo_driver.geospatial_utils import clip, filter_by_time


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
        """_summary_

        https://nasa-openscapes.github.io/2021-Cloud-Workshop-AGU/how-tos/Earthdata_Cloud__Single_File__Direct_S3_Access_NetCDF4_Example.html

        Returns:
            xr.DataArray: _description_
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
        crs = CloudStorageFileReader._get_epsg(item=item)
        # clip area of interest
        ds = ds[self.bands]
        da = ds.to_array()

        da = clip(data=da, bbox=self.bbox, x_dim=x_dim, y_dim=y_dim, crs=crs)
        # remove timestamps that have not been selected by end-user
        da = filter_by_time(
            data=da, temporal_extent=self.temporal_extent, temporal_dim=time_dim
        )

        return da


def main():
    items = [
        {
            "assets": {
                "data": {
                    # "href": "s3://openeo-geodn-driver-output/tasmax_rcp85_land-cpm_uk_2.2km_01_day_20781201-20791130.nc"
                    "href": "s3://openeo-geodn-driver-output/20240509T130344-7963dc30-40a4-421a-9a57-a1695d02c84f.nc"
                }
            },
            "properties": {
                "start_datetime": "2023-08-30T15:38:21Z",
                "cube:dimensions": {
                    "x": {
                        "axis": "x",
                        "step": 30.0,
                        "type": "spatial",
                        "extent": [699960.0, 809760.0],
                        "reference_system": 32618,
                    },
                    "y": {
                        "axis": "y",
                        "step": -30.0,
                        "type": "spatial",
                        "extent": [5000040.0, 4890240.0],
                        "reference_system": 4326,
                    },
                    "time": {
                        "type": "temporal",
                        "extent": ["2023-08-30T15:38:21Z", "2023-08-30T15:50:51Z"],
                    },
                },
            },
        }
    ]
    import pandas as pd

    temporal_extent = (
        pd.Timestamp("2007-11-01T00:00:00Z").to_pydatetime(),
        pd.Timestamp("2007-12-01T00:00:00Z").to_pydatetime(),
    )
    reader = NetCDFFileReader(
        items=items,
        bbox=[-3.0, 53.0, 0.2, 54.0],
        temporal_extent=temporal_extent,
        bands=["Total precipitation"],
        dimension_map={},
    )
    da = reader.load_items()
    print(da)


if __name__ == "__main__":
    main()
