from numbers import Number
from pathlib import Path
from openeo_pg_parser_networkx.pg_schema import BoundingBox
from openeo_geodn_driver.cos_parser import COSConnector
from pystac_client import Client
from openeo_geodn_driver.constants import (
    DEFAULT_BANDS_DIMENSION,
    STAC_URL,
    TEST_DATA_ROOT,
)
from openeo_geodn_driver.process_implementations.load_collection import (
    LoadCollectionFromCOS,
)
import xarray as xr
import numpy as np
import pandas as pd
import pytest
from openeo_geodn_driver.stac import STAC

INPUT_TEST_LOAD_ITEMS_USING_STACKSTAC = [
    (
        "ibm-eis-ga-1-esa-sentinel-2-l2a",
        "S2A_MSIL1C_20170103T190802_N0204_R013_T10SEG_20170103T190949",
        "b04",
        4326,
        0.000064,
    ),
]


@pytest.mark.parametrize(
    "collection_id, item_id, band, expected_epsg, resolution",
    INPUT_TEST_LOAD_ITEMS_USING_STACKSTAC,
)
def test_load_items_using_stackstac(
    collection_id: str, item_id: str, band: str, expected_epsg: int, resolution: float
):
    stac = STAC(STAC_URL)
    if stac.is_collection_available(collection_id=collection_id):
        client = Client.open(STAC_URL)
        item = stac.get_item(collection_id=collection_id, item_id=item_id)
        bbox = item["bbox"]
        west, south, east, north = bbox
        # item = client.get_item(id=item_id)
        collection_ids = [collection_id]
        res = client.search(
            ids=[item_id],
            # fields=fields,
            collections=collection_ids,
            max_items=5,
        )
        matched_items = list(res.items_as_dicts())
        spatial_extent = BoundingBox(
            west=west,
            south=south,
            east=east,
            north=north,
            crs="EPSG:4326",
        )
        bbox_wsg84 = LoadCollectionFromCOS._convert_to_WSG84(
            spatial_extent=spatial_extent
        )

        cos_conn = COSConnector()
        bands = [band]
        data = cos_conn.load_items_using_stackstac(
            items=matched_items,
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
        filename = f"test_load_items_using_stackstac_{item_id}.nc"
        path = TEST_DATA_ROOT / filename
        engine = "netcdf4"
        try:
            # explicitly convert from DataArray to Dataset because xarray would do it anyway
            ds = data.to_dataset(dim=DEFAULT_BANDS_DIMENSION)
            ds.to_netcdf(path=path, engine=engine)  # type: ignore
        except TypeError:
            valid_types = (str, Number, np.ndarray, np.number, list, tuple)
            # convert attributes of the xarray.Dataset
            for attr_key, attr_value in ds.attrs.items():
                if not isinstance(attr_value, valid_types) or isinstance(
                    attr_value, bool
                ):
                    ds.attrs[attr_key] = str(attr_value)
            # convert attributes of the variables
            for variable in list(ds):
                for attr_key, attr_value in ds[variable].attrs.items():
                    if not isinstance(attr_value, valid_types) or isinstance(
                        attr_value, bool
                    ):
                        ds[variable].attrs[attr_key] = str(attr_value)

            ds.to_netcdf(path=filename, engine=engine)  # type: ignore


def test_upload_fileobj():
    OUTPUT_BUCKET_NAME = "openeo-geodn-driver-output"

    cos_conn = COSConnector()
    creds = COSConnector.get_credentials_by_bucket(bucket=OUTPUT_BUCKET_NAME)
    filename = "test_upload_fileobj.nc"
    path = "./openeo_geodn_driver/tests/unit/unit_test_data/test_post_result_23100f202f2b46999fdd1cbc3ae53903.nc"
    cos_conn.upload_fileobj(
        key=filename,
        bucket=OUTPUT_BUCKET_NAME,
        path=Path(path),
        endpoint=creds["endpoint"],
        access_key_id=creds["access_key_id"],
        secret=creds["secret_access_key"],
    )
