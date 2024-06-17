from numbers import Number
from pathlib import Path
import uuid
import numpy as np
import pandas as pd
from openeo_driver.save_result import ImageCollectionResult
import xarray as xr
from typing import Optional
from tensorlakehouse_openeo_driver.driver_data_cube import TensorLakehouseDataCube
from tensorlakehouse_openeo_driver.constants import (
    DEFAULT_BANDS_DIMENSION,
    FILE_DATETIME_FORMAT,
    GEOTIFF_PREFIX,
    GTIFF,
    NETCDF,
    DEFAULT_TIME_DIMENSION,
)
import logging
import logging.config
import zipfile
import os

logging.config.fileConfig(fname="logging.conf", disable_existing_loggers=False)
logger = logging.getLogger("geodnLogger")


class GeoDNImageCollectionResult(ImageCollectionResult):
    def __init__(
        self,
        cube: TensorLakehouseDataCube,
        format: Optional[str] = None,
        options: Optional[dict] = None,
    ):
        super().__init__(cube, format, options)
        if format is None:
            format = NETCDF
        self.format = format

        self.options = options

    def save_result(self, filename: str) -> str:
        """save result as a file specified by filename

        Args:
            path (str): full path to file

        Raises:
            NotImplementedError: _description_

        Returns:
            str: filename
        """
        engine = "netcdf4"
        logger.debug(f"GeoDNImageCollectionResult::save_result - {filename=}")
        if GTIFF == self.format.upper():
            return self._save_as_geotiff(filename=filename)
        elif NETCDF == self.format.upper():
            array = self.cube.data

            assert isinstance(
                array, xr.DataArray
            ), f"Error! Not a xr.DataArray: {type(self.cube.data)}"
            try:
                logger.debug(f"Storing xarray as netcdf file called {filename}")

                # explicitly convert from DataArray to Dataset because xarray would do it anyway
                dimensions = array.dims
                assert all(
                    isinstance(d, str) for d in dimensions
                ), f"Error! Unexpected dimension name: {dimensions=}"
                if DEFAULT_BANDS_DIMENSION in dimensions:
                    ds = array.to_dataset(dim=DEFAULT_BANDS_DIMENSION)
                else:
                    ds = array.to_dataset(name="variable")
                logger.debug(f"DataSet dimensions {dimensions}")
                ds.to_netcdf(path=filename, engine=engine)  # type: ignore[call-overload]
            except TypeError as e:
                logger.error(
                    f"TypeError: Invalid attr. Exception handling: trying to convert invalid attrs to str: {e}"
                )
                valid_types = (str, Number, np.ndarray, np.number, list, tuple)
                # convert attributes of the xarray.Dataset
                for attr_key, attr_value in ds.attrs.items():
                    if not isinstance(attr_value, valid_types) or isinstance(
                        attr_value, bool
                    ):
                        logger.debug(f"Invalid attr: {attr_key}")
                        ds.attrs[attr_key] = str(attr_value)
                # convert attributes of the variables
                for variable in list(ds):
                    for attr_key, attr_value in ds[variable].attrs.items():
                        if not isinstance(attr_value, valid_types) or isinstance(
                            attr_value, bool
                        ):
                            logger.debug(f"Invalid attr: {attr_key}")
                            ds[variable].attrs[attr_key] = str(attr_value)

                ds.to_netcdf(path=filename, engine=engine)  # type: ignore[call-overload]

        else:
            raise NotImplementedError(f"Support for {format} is not implemented")
        logger.debug(f"save_result process: {filename=}")
        return filename

    def _save_as_geotiff(self, filename: str) -> str:
        """save files as geotiff

        Args:
            filename (str): full path to the file

        Returns:
            str: full path to the file
        """
        driver = "COG"
        data = self.cube.data

        assert isinstance(
            data, xr.DataArray
        ), f"Error! Not a xr.DataArray: {type(self.cube.data)}"

        # Save each slice of the DataArray as a separate GeoTIFF file
        if data.openeo is not None and data.openeo.temporal_dims is not None:
            temporal_dims = data.openeo.temporal_dims
            time_dim = temporal_dims[0]
        else:
            time_dim = DEFAULT_TIME_DIMENSION
        if time_dim in data.dims:
            time_size = len(data[time_dim])
        else:
            time_size = 0

        # Note: The rio.to_raster() method only works on a 2-dimensional
        # or 3-dimensional xarray.DataArray or a 2-dimensional xarray.Dataset.
        if time_size == 1:
            # destroy time dimension
            data = data.isel({time_dim: 0})
            data.rio.to_raster(
                filename,
                driver=driver,  # Write driver
                reading_driver=driver,  # Read driver
            )
        else:
            path = Path(filename)
            parent_dir = path.parent
            # save as zip instead of tif
            filename = filename.replace(".gtiff", ".zip")
            self.format = "ZIP"
            geotiff_files = list()
            time_list = list(data[time_dim].values)
            for index in range(0, time_size):
                t: np.datetime64
                t = time_list[index]
                timestamp = pd.Timestamp(t)
                timestamp_str = timestamp.strftime(FILE_DATETIME_FORMAT)
                unique_id = uuid.uuid4().hex
                output_filename = (
                    parent_dir / f"{GEOTIFF_PREFIX}_{timestamp_str}_{unique_id}.tif"
                )
                slice_array = data.isel({time_dim: index})
                if not np.isnan(slice_array.data).all():
                    slice_array.rio.to_raster(output_filename)
                    geotiff_files.append(output_filename)
            # Create a zip file and add GeoTIFF files to it
            with zipfile.ZipFile(filename, "w", zipfile.ZIP_DEFLATED) as zipf:
                for geotiff_file in geotiff_files:
                    zipf.write(geotiff_file)
            # Remove the temporary GeoTIFF files
            for geotiff_file in geotiff_files:
                os.remove(geotiff_file)

        return filename
