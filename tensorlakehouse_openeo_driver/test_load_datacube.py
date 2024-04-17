import glob
from pathlib import Path
from typing import Dict, List
import openeo
import datetime
import time
import uuid
from openeo_geodn_driver.constants import (
    OPENEO_URL,
    OPENEO_PASSWORD,
    OPENEO_USERNAME,
    TEST_DATA_ROOT,
    logger,
)
import pytest

TIMEOUT_TEST_DIR = TEST_DATA_ROOT / "timeout_test"


@pytest.fixture(scope="module")
def setup():
    if TIMEOUT_TEST_DIR.exists():
        p = TIMEOUT_TEST_DIR.glob("**/*")
        files = [x for x in p if x.is_file()]
        for f in files:
            logger.debug(f"Deleted file:{f}")
            f.unlink()
    else:
        TIMEOUT_TEST_DIR.mkdir()


@pytest.mark.skip("too long, remove this line when it is necessary to run this test")
def test_load_collection(setup):
    # timeout issue
    inference_hls_4 = dict(
        event_id="hls_4_maintest",
        bbox=[35.5, -0.99, 36.7, 0],
        start_date="2023-05-25",
        end_date="2023-05-30",
        data_type="hlsl30",
        request_type="openeo",
        rgb_constant_multiply=10000,
    )

    inference_hls_3 = dict(
        event_id="hls_3_secondtest",
        bbox=[
            34.84210885815902,
            0.19311650840824435,
            34.95984532412864,
            0.27439787596984033,
        ],
        start_date="2023-01-01",
        end_date="2023-01-01",
        data_type="hlsl30",
        request_type="openeo",
        rgb_constant_multiply=10000,
    )

    for _ in range(5):
        load_openeo(inference_hls_3)
        load_openeo(inference_hls_4)
        time.sleep(600)


def simple_pipeline(
    inference_dict: Dict, inputs_folder: Path, unique_id, logger
) -> List[Path]:
    logger.debug(f"openeo request {unique_id}")

    start_date_obj = datetime.datetime.strptime(
        inference_dict["start_date"], "%Y-%m-%d"
    )
    end_date_obj = datetime.datetime.combine(
        datetime.datetime.strptime(inference_dict["end_date"], "%Y-%m-%d"),
        datetime.time(23, 59, 59),
    )

    if inference_dict["data_type"] == "hls":
        collections = ["HLSS30", "HLSL30"]
    elif inference_dict["data_type"] == "hlsl30":
        collections = ["HLSL30"]
    elif inference_dict["data_type"] == "hlss30":
        collections = ["HLSS30"]

    # Pull from OpenEO
    geodn = openeo.connect(OPENEO_URL, default_timeout=240).authenticate_basic(
        OPENEO_USERNAME, OPENEO_PASSWORD
    )
    logger.debug("Connected to OpenEO")
    file_paths: List[Path] = list()
    for collection in collections:
        if collection == "HLSS30":
            # band order is Blue, Green, Red, NIR Narrow, SWIR-1, SWIR-2
            bands = ["B02", "B03", "B04", "B8A", "B11", "B12", "Fmask"]
        elif collection == "HLSL30":
            # band order is Blue, Green, Red, NIR Narrow, SWIR-1, SWIR-2
            bands = ["B02", "B03", "B04", "B05", "B06", "B07", "Fmask"]

        logger.debug("Load datacube")
        datacube = geodn.load_collection(
            collection_id=collection,
            spatial_extent={
                "west": inference_dict["bbox"][0],
                "south": inference_dict["bbox"][1],
                "east": inference_dict["bbox"][2],
                "north": inference_dict["bbox"][3],
            },
            temporal_extent=[
                start_date_obj.strftime("%Y-%m-%dT%H:%M:%SZ"),
                end_date_obj.strftime("%Y-%m-%dT%H:%M:%SZ"),
            ],
            bands=bands,
        )

        output_format = "GTiff"
        datacube = datacube.save_result(output_format)
        file_id = uuid.uuid4().hex
        file_path = inputs_folder / f"openeo_data_{file_id}.tif"
        logger.debug(f"store result as {file_path}")
        logger.debug(datacube.to_json())
        datacube.download(file_path)
        file_paths.append(file_path)
        logger.debug("Datacube loaded")
    return file_paths


def load_openeo(inference_dict):
    if "event_id" in inference_dict:
        unique_id = inference_dict["event_id"]
    else:
        unique_id = str(uuid.uuid4())

    # # S2 data pipeline # #

    start = time.time()
    file_paths = list()
    try:
        file_paths = simple_pipeline(
            inference_dict, TIMEOUT_TEST_DIR, unique_id, logger
        )
        logger.debug(f"Took {round(time.time() - start)} seconds")
        output_files = glob.glob(TIMEOUT_TEST_DIR + "*.tif")
        logger.debug(f"Files: {output_files}")
    except Exception as err:
        logger.error(f"{type(err)}: {err}")
        logger.debug(f"After {round(time.time() - start)} seconds")
    finally:
        assert len(file_paths) > 0
        assert all(path.exists() for path in file_paths)
