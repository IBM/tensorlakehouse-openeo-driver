import glob
import os
import openeo
import datetime
import time
import logging
import uuid
import shutil

logger = logging.getLogger(__name__)
logger.setLevel("DEBUG")


handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
# Create a formatter to specify the format of the log messages
formatter = logging.Formatter("%(asctime)s - %(message)s")
handler.setFormatter(formatter)

# Add the handler to the logger
logger.addHandler(handler)

# Add file handler
folder = "testing/timeout/"
os.makedirs(folder, exist_ok=True)
FileOutputHandler = logging.FileHandler(folder + "logs.log")
FileOutputHandler.setLevel(logging.DEBUG)
logger.addHandler(FileOutputHandler)


def simple_pipeline(inference_dict, inputs_folder, unique_id, logger):
    logger.debug(f"openeo request {unique_id}")
    username = os.environ["OPENEO_USERNAME"]
    password = os.environ["OPENEO_PASSWORD"]
    # OpenEO service URL
    openeo_url = os.environ[
        "OPENEO_URL"
    ]  # "http://openeo-geodn-driver-nasageospatial-dev.cash.sl.cloud9.ibm.com/openeo/1.1.0/"

    start_date_obj = datetime.datetime.strptime(inference_dict["start_date"], "%Y-%m-%d")
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
    geodn = openeo.connect(openeo_url, default_timeout=240).authenticate_basic(username, password)
    logger.debug("Connected to OpenEO")
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
        file_path = f"{inputs_folder}openeo_data.tif"
        datacube.download(file_path)
        logger.debug("Datacube loaded")


def load_openeo(inference_dict):
    if "event_id" in inference_dict:
        unique_id = inference_dict["event_id"]
    else:
        unique_id = str(uuid.uuid4())

    inputs_folder = folder + unique_id + "/inputs/"
    shutil.rmtree(inputs_folder)
    os.makedirs(inputs_folder, exist_ok=True)

    # # S2 data pipeline # #

    start = time.time()
    try:
        simple_pipeline(inference_dict, inputs_folder, unique_id, logger)
        logger.debug(f"Took {round(time.time() - start)} seconds")
        output_files = glob.glob(inputs_folder + "*.tif")
        logger.debug(f"Files: {output_files}")
    except Exception as err:
        logger.error(f"{type(err)}: {err}")
        logger.debug(f"After {round(time.time() - start)} seconds")
        pass


if __name__ == "__main__":
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

    os.environ["OPENEO_USERNAME"] = "john"
    os.environ["OPENEO_PASSWORD"] = "john123"
    os.environ["OPENEO_URL"] = "https://openeo-geodn-nasageospatial-dev.cash.sl.cloud9.ibm.com"

    while True:
        load_openeo(inference_hls_3)
        load_openeo(inference_hls_4)

        time.sleep(600)
