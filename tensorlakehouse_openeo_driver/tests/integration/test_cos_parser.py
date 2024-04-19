from openeo_pg_parser_networkx.pg_schema import BoundingBox
from tensorlakehouse_openeo_driver.cos_parser import COSConnector
from pystac_client import Client
from tensorlakehouse_openeo_driver.constants import STAC_URL
from tensorlakehouse_openeo_driver.process_implementations.load_collection import (
    LoadCollectionFromCOS,
)
import xarray as xr
import numpy as np
import pandas as pd


def test_load_items_using_stackstac():
    item_ids_list = [
        "HLS.S30.T37MBU.2022123T073621.v2.0.B04",
        "HLS.S30.T37MBV.2022123T073621.v2.0.B04",
        "HLS.S30.T37MCT.2022123T073621.v2.0.B04",
        "HLS.S30.T37MCU.2022123T073621.v2.0.B04",
        "HLS.S30.T37MCV.2022123T073621.v2.0.B04",
    ]
    collections = ["HLSS30"]
    fields = {
        "include": [
            "id",
            "bbox",
            "properties.datetime",
            "properties.cube:variables",
            "properties.cube:dimensions",
        ],
        "exclude": [],
    }
    expected_epsg = 4326
    resolution = (3660, 3660)
    # expected_epsg = 4326
    # resolution = 0.0174532925199433
    spatial_extent = BoundingBox(
        west=36.3047972,
        south=-2.8023718,
        east=38.1884585,
        north=0.20025600000000002,
        crs="EPSG:4326",
    )
    bands = ["B04"]

    client = Client.open(STAC_URL)
    for item_id in item_ids_list:
        # item = client.get_item(id=item_id)
        res = client.search(ids=[item_id], fields=fields, collections=collections)
        items = list(res.items())
        bbox_wsg84 = LoadCollectionFromCOS._convert_to_WSG84(spatial_extent=spatial_extent)

        # selected_items = list()
        bucket = None
        i = 0
        while bucket is None and i < len(items):
            item = items[i]
            i += 1
            cube_vars = item.properties["cube:variables"]
            available_bands = list(cube_vars.keys())

            for band in bands:
                if band in available_bands:
                    bucket = COSConnector._extract_bucket_name_from_url(
                        url=item.assets["data"].href
                    )
                    # selected_items.append(item)
            # item = next(iter(items), None)
        assert bucket is not None
        cos_conn = COSConnector(bucket=bucket)
        data = cos_conn.load_items_using_stackstac(
            items=items,
            bbox=bbox_wsg84,
            bands=bands,
            epsg=expected_epsg,
            resolution=resolution,
        )
        assert isinstance(data, xr.DataArray)
        assert data.time.size == 1
        assert data.bands.size == len(bands)
        epsg = data.rio.crs.to_epsg()
        assert (
            epsg == expected_epsg
        ), f"Error! expected EPSG {expected_epsg} is different than {epsg}"
        for t in data.time.values:
            assert isinstance(t, np.datetime64)
            assert t is not pd.NaT
        # filename = f"test_load_items_using_stackstac_{item_id}.nc"
        # path = TEST_DATA_ROOT / filename
        # engine = "netcdf4"
        # try:

        #     # explicitly convert from DataArray to Dataset because xarray would do it anyway
        #     ds = data.to_dataset(dim=BANDS)
        #     ds.to_netcdf(path=path, engine=engine)
        # except TypeError as e:

        #     valid_types = (str, Number, np.ndarray, np.number, list, tuple)
        #     # convert attributes of the xarray.Dataset
        #     for attr_key, attr_value in ds.attrs.items():
        #         if not isinstance(attr_value, valid_types) or isinstance(
        #             attr_value, bool
        #         ):
        #             ds.attrs[attr_key] = str(attr_value)
        #     # convert attributes of the variables
        #     for variable in list(ds):
        #         for attr_key, attr_value in ds[variable].attrs.items():
        #             if not isinstance(attr_value, valid_types) or isinstance(
        #                 attr_value, bool
        #             ):
        #                 ds[variable].attrs[attr_key] = str(attr_value)

        #     ds.to_netcdf(path=filename, engine=engine)  # Works as expected
