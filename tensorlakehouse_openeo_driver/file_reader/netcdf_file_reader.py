from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from pystac import Asset, Item
from tensorlakehouse_openeo_driver.constants import (
    DEFAULT_BANDS_DIMENSION,
    DEFAULT_X_DIMENSION,
    DEFAULT_Y_DIMENSION,
)
from tensorlakehouse_openeo_driver.file_reader.cloud_storage_file_reader import (
    CloudStorageFileReader,
)
import xarray as xr

from tensorlakehouse_openeo_driver.file_reader.raster_file_reader import (
    RasterFileReader,
)
from tensorlakehouse_openeo_driver.geospatial_utils import (
    clip_box,
    filter_by_time,
    reproject_bbox,
)
from urllib.parse import urlparse
import pandas as pd


class NetCDFFileReader(RasterFileReader):

    def __init__(
        self,
        items: List[Item],
        bands: List[str],
        bbox: Tuple[float, float, float, float],
        temporal_extent: Tuple[datetime, Optional[datetime]],
        properties: Optional[Dict[str, Any]],
    ) -> None:
        super().__init__(
            items=items,
            bands=bands,
            bbox=bbox,
            temporal_extent=temporal_extent,
            properties=properties,
        )

    def _concat_bucket_and_path(self, path) -> str:
        url = f"s3://{self.bucket}/{path}"
        return url

    def load_items(self) -> xr.DataArray:
        """load items that are associated with netcdf files

        Returns:
            xr.DataArray: raster data cube
        """
        # initialize array and crs variables
        da = None
        crs_code = None
        data_arrays = list()
        # load each item
        for item in self.items:
            assets: Dict[str, Asset] = item.assets
            asset_value = next(iter(assets.values()))
            # href field can be either URL (a link to a file on COS) or a path to a local file
            path_or_url = asset_value.href
            parse_url = urlparse(path_or_url)
            if parse_url.scheme == "":
                ds = xr.open_dataset(path_or_url, engine="netcdf4")
            else:
                s3fs = self.create_s3filesystem()
                s3_file_obj = s3fs.open(path_or_url, mode="rb")
                ds = xr.open_dataset(s3_file_obj, engine="scipy")
            # get dimension names
            x_dim = CloudStorageFileReader._get_dimension_name(
                item=item.to_dict(), axis=DEFAULT_X_DIMENSION
            )
            y_dim = CloudStorageFileReader._get_dimension_name(
                item=item.to_dict(), axis=DEFAULT_Y_DIMENSION
            )

            # get CRS
            crs_code = CloudStorageFileReader._get_epsg(item=item.to_dict())
            if ds.rio.crs is None:
                ds.rio.write_crs(f"epsg:{crs_code}", inplace=True)
            assert all(
                band in list(ds) for band in self.bands
            ), f"Error! not all bands={self.bands} are in ds={list(ds)}"
            # drop bands that are not required by the user
            ds = ds[self.bands]
            ds = self._filter_by_extra_dimensions(ds)
            # if bands is already one of the dimensions, use default 'variable'
            if DEFAULT_BANDS_DIMENSION in dict(ds.dims).keys():
                da = ds.to_array()
            else:
                # else export array using bands
                da = ds.to_array(dim=DEFAULT_BANDS_DIMENSION)
            # add temporal dimension if it does not exist on dataarray
            time_dim = CloudStorageFileReader._get_dimension_name(
                item=item.to_dict(), dim_type="temporal"
            )
            if time_dim is None:
                raise ValueError(f"Error! {item=}")
            elif time_dim not in da.dims:

                dt_str = item.properties.get("datetime")
                dt = pd.Timestamp(dt_str).to_datetime64()

                da = da.expand_dims({time_dim: [dt]})
            data_arrays.append(da)
        if len(data_arrays) > 1:
            # concatenate all xarray.DataArray objects
            data_array = xr.concat(data_arrays, dim=time_dim)
        else:
            data_array = data_arrays.pop()
        # filter by area of interest
        assert isinstance(crs_code, int), f"Error! Invalid type: {crs_code=}"
        reprojected_bbox = reproject_bbox(
            bbox=self.bbox, src_crs=4326, dst_crs=crs_code
        )
        assert x_dim is not None and y_dim is not None
        da = clip_box(
            data=data_array,
            bbox=reprojected_bbox,
            x_dim=x_dim,
            y_dim=y_dim,
            crs=crs_code,
        )
        # remove timestamps that have not been selected by end-user
        if time_dim is not None:
            da = filter_by_time(
                data=da, temporal_extent=self.temporal_extent, temporal_dim=time_dim
            )

        return da
