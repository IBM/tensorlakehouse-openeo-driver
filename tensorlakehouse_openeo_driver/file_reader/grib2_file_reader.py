from pathlib import Path
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from tensorlakehouse_openeo_driver.constants import (
    DEFAULT_BANDS_DIMENSION,
    DEFAULT_X_DIMENSION,
    DEFAULT_Y_DIMENSION,
    TENSORLAKEHOUSE_OPENEO_DRIVER_DATA_DIR,
    logger,
)
from tensorlakehouse_openeo_driver.file_reader.cloud_storage_file_reader import (
    CloudStorageFileReader,
)
import uuid
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

    def _check_coords(self, ds: xr.Dataset) -> bool:
        extra_dims_filter = self.get_extra_dimensions_filter()
        coords_names = set(list(ds.coords.keys()))
        for dim_name, dim_value in extra_dims_filter.items():
            if dim_name not in coords_names or dim_value not in ds[dim_name].values:
                return False

        return True

    def _check_dimensions(
        self, ds: xr.Dataset, x_dim: str, y_dim: str, temporal_dim: Optional[str]
    ) -> bool:
        extra_dims_filter = self.get_extra_dimensions_filter()
        required_dims = set(list(extra_dims_filter.keys()))
        dimensions = set(list(ds.sizes.keys()))
        if temporal_dim is not None:
            return required_dims.union(set([x_dim, y_dim])) == dimensions - set(
                temporal_dim
            )
        else:
            return required_dims.union(set([x_dim, y_dim])) == dimensions

    def _check_bands(self, ds: xr.Dataset) -> bool:
        variables = set(list(ds.keys()))
        bands = set(self.bands)
        return bands.issubset(variables)

    @staticmethod
    def convert_longitude_coords(
        ds: xr.Dataset, units: Optional[str], x_dim: str, y_dim: str
    ) -> xr.Dataset:
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
            ds = ds.assign_coords({x_dim: (((ds[x_dim] + 180) % 360) - 180)})
            ds = ds.sortby([x_dim, y_dim])
        return ds

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
            # get dimension names
            x_dim = CloudStorageFileReader._get_dimension_name(
                item=item, axis=DEFAULT_X_DIMENSION
            )
            assert x_dim is not None
            y_dim = CloudStorageFileReader._get_dimension_name(
                item=item, axis=DEFAULT_Y_DIMENSION
            )
            assert y_dim is not None
            time_dim = CloudStorageFileReader._get_dimension_name(
                item=item, dim_type="temporal"
            )
            crs_code = CloudStorageFileReader._get_epsg(item=item)
            # initial implementation assumes that file is local
            # href field can be either URL (a link to a file on COS) or a path to a local file
            path_or_url = asset_value["href"]
            parse_url = urlparse(path_or_url)
            if parse_url.scheme == "":
                path = Path(path_or_url)
                assert path.exists(), f"Error! File does not exist: {path_or_url}"
                hex_code = uuid.uuid4().hex
                indexpath = (
                    TENSORLAKEHOUSE_OPENEO_DRIVER_DATA_DIR
                    / f"{path.name}.{hex_code}.idx"
                )
                datasets = cfgrib.open_datasets(
                    path_or_url, backend_kwargs={"indexpath": str(indexpath)}
                )
            else:
                s3fs = self.create_s3filesystem()
                s3_file_obj = s3fs.open(path_or_url, mode="rb")
                ds = xr.open_dataset(s3_file_obj, engine="cfgrib")
                datasets = [ds]
            try:
                units = item["properties"]["cube:dimensions"][x_dim].get("unit")
            except KeyError as e:
                msg = f"Error! Missing key: {item=} {e=}"
                raise KeyError(msg)
            # cfgrib follows NetCDF Climate and Forecast (CF) Metadata Conventions and because of
            # that longitude is represented as degrees east,i.e., from 0 to 360
            i = 0
            found = False
            while i < len(datasets) and not found:
                ds = datasets[i]
                i += 1

                # set of dimensions that this dataset contains

                if (
                    self._check_coords(ds=ds)
                    and self._check_bands(ds=ds)
                    and self._check_dimensions(
                        ds=ds, x_dim=x_dim, y_dim=y_dim, temporal_dim=time_dim
                    )
                ):
                    found = True
                    ds = Grib2FileReader.convert_longitude_coords(
                        ds=ds, units=units, x_dim=x_dim, y_dim=y_dim
                    )
                    assert isinstance(
                        ds, xr.Dataset
                    ), f"Error! Unexpected type={type(ds)}"

                    # get CRS
                    if ds.rio.crs is None:
                        ds.rio.write_crs(f"epsg:{crs_code}", inplace=True)
                    # assert all(
                    #     band in list(ds) for band in self.bands
                    # ), f"Error! not all bands={self.bands} are in ds={list(ds)}"
                    # drop bands that are not required
                    ds = ds[self.bands]
                    # drop dimensions that are not required
                    extra_dim_filter = self.get_extra_dimensions_filter()
                    ds = ds.sel(extra_dim_filter)
                    # if bands is already one of the dimensions, use default 'variable'
                    if DEFAULT_BANDS_DIMENSION in dict(ds.dims).keys():
                        da = ds.to_array()
                    else:
                        # else export array using bands
                        da = ds.to_array(dim=DEFAULT_BANDS_DIMENSION)

                    # add temporal dimension if it does not exist on dataarray

                    if time_dim is None:
                        raise ValueError(f"Error! {item=}")
                    elif time_dim not in da.dims:
                        dt_str = item["properties"].get("datetime")
                        timestamps = pd.to_datetime([pd.Timestamp(dt_str)])

                        da = da.expand_dims({time_dim: timestamps})
            assert (
                found
            ), f"Error! Unable to find data that contains all {self.bands} variables all {self.get_extra_dimensions_filter()}"
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
