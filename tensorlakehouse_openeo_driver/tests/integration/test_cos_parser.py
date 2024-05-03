from pathlib import Path
from tensorlakehouse_openeo_driver.s3_connections.cos_parser import COSConnector

INPUT_TEST_LOAD_ITEMS_USING_STACKSTAC = [
    (
        "ibm-eis-ga-1-esa-sentinel-2-l2a",
        "S2A_MSIL1C_20170103T190802_N0204_R013_T10SEG_20170103T190949",
        "b04",
        4326,
        0.000064,
    ),
]


def test_upload_fileobj():
    output_bucket_name = "openeo-geodn-driver-output"

    cos_conn = COSConnector(bucket=output_bucket_name)
    filename = "test_upload_fileobj.nc"
    f = "./tensorlakehouse_openeo_driver/tests/unit/unit_test_data/test_post_result_23100f202f2b46999fdd1cbc3ae53903.nc"
    path = Path(f)
    assert path.exists()
    cos_conn.upload_fileobj(
        key=filename,
        path=path,
    )
