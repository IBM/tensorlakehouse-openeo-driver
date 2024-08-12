from pathlib import Path
import warnings
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from tensorlakehouse_openeo_driver.constants import (
    DEFAULT_BANDS_DIMENSION,
    DEFAULT_X_DIMENSION,
    DEFAULT_Y_DIMENSION,
    logger,
)
from tensorlakehouse_openeo_driver.file_reader.cloud_storage_file_reader import (
    CloudStorageFileReader,
)
import xarray as xr

from tensorlakehouse_openeo_driver.geospatial_utils import (
    clip_box,
    filter_by_time,
    reproject_bbox,
)


class FSTDFileReader(CloudStorageFileReader):

    def __init__(
        self,
        items: List[Dict[str, Any]],
        bands: List[str],
        bbox: Tuple[float, float, float, float],
        temporal_extent: Tuple[datetime, Optional[datetime]],
        dimension_map: Optional[Dict[str, str]],
    ) -> None:

        assert isinstance(items, list)
        assert len(items) > 0
        self.items = items
        # validate bbox
        assert isinstance(bbox, tuple), f"Error! {type(bbox)} is not a tuple"
        assert len(bbox) == 4, f"Error! Invalid size: {len(bbox)}"
        west, south, east, north = bbox
        assert -180 <= west <= east <= 180, f"Error! {west=} {east=}"
        assert -90 <= south <= north <= 90, f"Error! {south=} {north=}"
        self.bbox = bbox
        self.bands = bands
        if temporal_extent is not None and len(temporal_extent) > 0:
            # if temporal_extent is not empty tuple, then the first item cannot be None
            assert isinstance(temporal_extent[0], datetime)
            # the second item can be None for open intervals
            if temporal_extent[1] is not None:
                assert isinstance(temporal_extent[1], datetime)
                assert temporal_extent[0] <= temporal_extent[1]
        self.temporal_extent = temporal_extent
        self.dimension_map = dimension_map

    def load_items(self) -> xr.DataArray:
        """load items that are associated with FSTD files

        Returns:
            xr.DataArray: raster data cube
        """
        import fstd2nc

        # initialize array and crs variables
        da = None
        crs_code = None
        data_arrays = list()
        # load each item
        for item in self.items:
            assets: Dict[str, Any] = item["assets"]
            asset_value = next(iter(assets.values()))
            # initial implementation assumes that file is local
            file_path = asset_value["href"]
            assert Path(file_path).exists(), f"Error! File does not exist: {file_path}"

            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                logger.debug(
                    f"StandardFileReader::load_items - reading file: {file_path}"
                )
                buffer = fstd2nc.Buffer(file_path, forecast_axis=True)
                ds = buffer.to_xarray()
                # get dimension names
                x_dim = CloudStorageFileReader._get_dimension_name(
                    item=item, axis=DEFAULT_X_DIMENSION
                )
                y_dim = CloudStorageFileReader._get_dimension_name(
                    item=item, axis=DEFAULT_Y_DIMENSION
                )
                time_dim = CloudStorageFileReader._get_dimension_name(
                    item=item, dim_type="temporal"
                )
                # get CRS
                crs_code = CloudStorageFileReader._get_epsg(item=item)
                if ds.rio.crs is None:
                    ds.rio.write_crs(f"epsg:{crs_code}", inplace=True)
                assert all(
                    band in list(ds) for band in self.bands
                ), f"Error! not all bands={self.bands} are in ds={list(ds)}"
                # drop bands that were not required
                ds = ds[self.bands]
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
        da = clip_box(
            data=data_array,
            bbox=reprojected_bbox,
            x_dim=x_dim,
            y_dim=y_dim,
            crs=crs_code,
        )
        # remove timestamps that have not been selected by end-user
        da = filter_by_time(
            data=da, temporal_extent=self.temporal_extent, temporal_dim=time_dim
        )

        return da
