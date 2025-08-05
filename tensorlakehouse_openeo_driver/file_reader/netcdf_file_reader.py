from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
import fsspec
from fsspec.implementations.http import HTTPFileSystem
from pystac import Asset, Item
from tensorlakehouse_openeo_driver.constants import (
    DEFAULT_BANDS_DIMENSION,
    DEFAULT_TIME_DIMENSION,
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
    create_missing_coords,
    expand_time_dimension,
    filter_by_time,
    reproject_bbox,
)
from urllib.parse import urlparse


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
            # create s3 file system
            # if scheme is an empty string it means it is a local file
            if parse_url.scheme == "":
                # open local file
                ds = xr.open_dataset(path_or_url, engine="netcdf4")
            # if credentials have not been set it means that data is publicly available
            elif (
                self.endpoint is None
                and self.access_key_id is None
                and self.secret_access_key is None
            ):
                # open publicly available remote file
                fs: HTTPFileSystem = fsspec.filesystem("https")
                # chunks={} to fix this issue https://github.com/fsspec/s3fs/issues/337
                ds = xr.open_dataset(fs.open(path_or_url), chunks={}, engine="h5netcdf")
            else:
                # create s3 session using credentials
                s3fs = self.create_s3filesystem()
                s3_file_obj = s3fs.open(path_or_url, mode="rb")
                # open remote file
                ds = xr.open_dataset(s3_file_obj, engine="scipy")
            # add temporal dimension if it does not exist on dataarray
            time_dim = CloudStorageFileReader._get_dimension_name(
                item=item.to_dict(), dim_type="temporal"
            )
            dt_str: str | None = item.properties.get("datetime")

            # get dimension names
            x_dim = CloudStorageFileReader._get_dimension_name(
                item=item.to_dict(), axis=DEFAULT_X_DIMENSION
            )
            y_dim = CloudStorageFileReader._get_dimension_name(
                item=item.to_dict(), axis=DEFAULT_Y_DIMENSION
            )

            ds = expand_time_dimension(data=ds, time_dim=time_dim, dt=dt_str)
            # ds = rename_dimensions(data=ds, y_dim=y_dim, x_dim=x_dim, time_dim=time_dim)
            ds = create_missing_coords(data=ds, time_dim=time_dim)
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
        assert (
            x_dim is not None and y_dim is not None
        ), f"Error! {x_dim=} and {y_dim=} cannot be None"
        da = clip_box(
            data=data_array,
            bbox=reprojected_bbox,
            crs=crs_code,
            y_dim=y_dim,
            x_dim=x_dim,
        )
        # remove timestamps that have not been selected by end-user
        if time_dim is not None:
            da = filter_by_time(
                data=da, temporal_extent=self.temporal_extent, temporal_dim=time_dim
            )

        return da
