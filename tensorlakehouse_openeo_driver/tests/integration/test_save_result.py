from tensorlakehouse_openeo_driver.constants import NETCDF
from tensorlakehouse_openeo_driver.save_result import GeoDNImageCollectionResult
from tensorlakehouse_openeo_driver.driver_data_cube import TensorLakehouseDataCube
from tensorlakehouse_openeo_driver.tests.unit.unit_test_util import (
    generate_xarray_datarray,
    validate_downloaded_file,
)
from rasterio.crs import CRS
import pandas as pd


def test_save_result():
    bands = ["b02", "b03"]
    da = generate_xarray_datarray(
        bands=bands,
        latmax=41,
        latmin=40,
        lonmax=-90,
        lonmin=-91,
        timestamps=[pd.Timestamp(2020, 1, 1), pd.Timestamp(2021, 1, 1)],
        freq=None,
    )
    cube = TensorLakehouseDataCube(data=da)
    expected_crs = CRS.from_epsg(4326)
    image_coll_result = GeoDNImageCollectionResult(cube=cube, format=NETCDF)
    filename = image_coll_result.save_result(filename="test_save_result.nc")
    validate_downloaded_file(
        path=filename,
        expected_dimension_size={},
        band_names=bands,
        expected_crs=expected_crs,
    )
