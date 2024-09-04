from pathlib import Path
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
import pandas as pd
import xarray as xr
import cfgrib
from tensorlakehouse_openeo_driver.geospatial_utils import (
    clip_box,
    filter_by_time,
    reproject_bbox,
)
from urllib.parse import urlparse


class Grib2FileReader(CloudStorageFileReader):

    def __init__(
        self,
        items: List[Dict[str, Any]],
        bands: List[str],
        bbox: Tuple[float, float, float, float],
        temporal_extent: Tuple[datetime, Optional[datetime]],
        properties: Optional[Dict[str, Any]],
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
        self.properties = properties

    def load_items(self) -> xr.DataArray:
        """load items that are associated with grib2 files

        Based on https://docs.xarray.dev/en/stable/examples/ERA5-GRIB-example.html

        Returns:
            xr.DataArray: raster data cube
        """
        logger.debug(f"Loading GRIB2 files: bands={self.bands} bbox={self.bbox}")
        # initialize array and crs variables
        da = None
        crs_code = None
        data_arrays = list()
        # load each item
        for item in self.items:
            assets: Dict[str, Any] = item["assets"]
            asset_value = next(iter(assets.values()))
            # initial implementation assumes that file is local
            # href field can be either URL (a link to a file on COS) or a path to a local file
            path_or_url = asset_value["href"]
            parse_url = urlparse(path_or_url)
            if parse_url.scheme == "":
                assert Path(
                    path_or_url
                ).exists(), f"Error! File does not exist: {path_or_url}"
                ds = cfgrib.open_dataset(path_or_url)
            else:
                s3fs = self.create_s3filesystem()
                s3_file_obj = s3fs.open(path_or_url, mode="rb")
                ds = xr.open_dataset(s3_file_obj, engine="cfgrib")

            x_dim_name = Grib2FileReader._get_dimension_name(item=item, axis="x")
            try:
                units = item["properties"]["cube:dimensions"][x_dim_name].get("unit")
            except KeyError as e:
                msg = f"Error! Missing key: {item=} {e=}"
                raise KeyError(msg)
            # cfgrib follows NetCDF Climate and Forecast (CF) Metadata Conventions and because of
            # that longitude is represented as degrees east,i.e., from 0 to 360
            if (
                units is not None
                and isinstance(units, str)
                and units.lower()
                in [
                    "degrees_east",
                    "degree_east",
                    "degree_e",
                    "degrees_e",
                    "degreee",
                    "degreesE",
                ]
            ):
                ds = ds.assign_coords(
                    {x_dim_name: (((ds[x_dim_name] + 180) % 360) - 180)}
                )
                y_dim_name = Grib2FileReader._get_dimension_name(item=item, axis="y")
                ds = ds.sortby([x_dim_name, y_dim_name])
            assert isinstance(ds, xr.Dataset), f"Error! Unexpected type={type(ds)}"
            # get dimension names
            x_dim = CloudStorageFileReader._get_dimension_name(
                item=item, axis=DEFAULT_X_DIMENSION
            )
            y_dim = CloudStorageFileReader._get_dimension_name(
                item=item, axis=DEFAULT_Y_DIMENSION
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

            ds = self._filter_by_extra_dimensions(dataset=ds)
            # if bands is already one of the dimensions, use default 'variable'
            if DEFAULT_BANDS_DIMENSION in dict(ds.dims).keys():
                da = ds.to_array()
            else:
                # else export array using bands
                da = ds.to_array(dim=DEFAULT_BANDS_DIMENSION)

            # add temporal dimension if it does not exist on dataarray
            time_dim = CloudStorageFileReader._get_dimension_name(
                item=item, dim_type="temporal"
            )
            if time_dim is None:
                raise ValueError(f"Error! {item=}")
            elif time_dim not in da.dims:
                dt_str = item["properties"].get("datetime")
                timestamps = pd.to_datetime([pd.Timestamp(dt_str)])

                da = da.expand_dims({time_dim: timestamps})
            data_arrays.append(da)
        # get temporal dimension name from an arbitrary item. Assumption that all items
        # have the same temporal dimension name

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
        assert x_dim is not None
        assert y_dim is not None
        da = clip_box(
            data=data_array,
            bbox=reprojected_bbox,
            x_dim=x_dim,
            y_dim=y_dim,
            crs=crs_code,
        )
        # remove timestamps that have not been selected by end-user
        if time_dim is not None and time_dim in da.dims:
            da = filter_by_time(
                data=da, temporal_extent=self.temporal_extent, temporal_dim=time_dim
            )

        return da
