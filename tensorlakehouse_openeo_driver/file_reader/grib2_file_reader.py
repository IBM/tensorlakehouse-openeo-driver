from io import BytesIO
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import earthkit
from pystac import Asset, Item
import urllib
from tensorlakehouse_openeo_driver.constants import (
    DEFAULT_BANDS_DIMENSION,
    DEFAULT_TIME_DIMENSION,
    DEFAULT_X_DIMENSION,
    DEFAULT_Y_DIMENSION,
    TENSORLAKEHOUSE_OPENEO_DRIVER_DATA_DIR,
    logger,
)
from tensorlakehouse_openeo_driver.file_reader.cloud_storage_file_reader import (
    CloudStorageFileReader,
)
import uuid
import xarray as xr
import cfgrib
from tensorlakehouse_openeo_driver.file_reader.raster_file_reader import (
    RasterFileReader,
)
from datetime import datetime
from tensorlakehouse_openeo_driver.geospatial_utils import (
    clip_box,
    expand_time_dimension,
    filter_by_time,
    rename_dimensions,
    rename_vars,
    reproject_bbox,
)
from urllib.parse import urlparse


class Grib2FileReader(RasterFileReader):

    def __init__(
        self,
        items: List[Item],
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

    def _open_remote_grib_file(self, item: Item) -> xr.Dataset:
        # idx_file: Optional[pd.DataFrame] = None
        datasets = list()
        asset = item.assets["data"]
        layers = asset.extra_fields["grib:layers"]
        for band_name in self.bands:
            grib_layer = layers[band_name]

            start_byte = grib_layer["start_byte"]
            end_byte = start_byte + grib_layer["byte_size"]
            # open remote file and pull only the data specified by start and end byte positions
            req = urllib.request.Request(asset.href)
            req.headers["Range"] = f"bytes={start_byte}-{end_byte}"

            with urllib.request.urlopen(req) as response:
                data = response.read()
                bytes_io = BytesIO(data)

                data_s = earthkit.data.from_source("stream", bytes_io)
                ds_aux = data_s.to_xarray()
                datasets.append(ds_aux)

        ds = xr.merge(datasets)
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
        time_dim = None
        data_arrays = list()
        x_dim = None
        y_dim = None
        # load each item
        for item in self.items:
            assets: Dict[str, Any] = item.assets
            arbitrary_asset: Asset = next(iter(assets.values()))
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
            path_or_url = arbitrary_asset.href
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
                ds = self._open_remote_grib_file(item=item)
                # rename variables to avoid conflict with default dimensions
                ds = rename_vars(data=ds)
                # get datetime as str
                dt_str: str | None = item.properties["datetime"]
                # create new time dimension if it does not exist
                ds = expand_time_dimension(data=ds, time_dim=time_dim, dt=dt_str)
                # rename dimensions if necessary
                ds = rename_dimensions(
                    data=ds, time_dim=time_dim, x_dim=x_dim, y_dim=y_dim
                )
            data_arrays.append(ds.to_array(dim=DEFAULT_BANDS_DIMENSION))
        # the assumption is that each item represents a single timestamp, i.e., each arrays has
        # a single temporal dimension 
        if len(data_arrays) > 1:
            assert isinstance(time_dim, str), f"Error! {time_dim=} is not a str"
            # concatenate all xarray.DataArray objects
            data_array = xr.concat(data_arrays, dim=DEFAULT_TIME_DIMENSION)
        else:
            data_array = data_arrays.pop()
        # filter by area of interest
        assert isinstance(crs_code, int), f"Error! Invalid type: {crs_code=}"
        reprojected_bbox = reproject_bbox(
            bbox=self.bbox, src_crs=4326, dst_crs=crs_code
        )
        assert x_dim is not None
        assert y_dim is not None
        assert isinstance(data_array, xr.DataArray)
        da = clip_box(
            data=data_array,
            bbox=reprojected_bbox,
            crs=crs_code,
            x_coord=x_dim,
            y_coord=y_dim,
        )
        # remove timestamps that have not been selected by end-user
        if time_dim is not None and time_dim in da.dims:
            da = filter_by_time(
                data=da, temporal_extent=self.temporal_extent, temporal_dim=time_dim
            )

        return da
