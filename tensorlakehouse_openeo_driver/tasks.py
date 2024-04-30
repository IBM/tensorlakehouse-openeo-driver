from pathlib import Path
from openeo_pg_parser_networkx import OpenEOProcessGraph
from typing import Any, Dict
from celery import Celery
from celery import states
from tensorlakehouse_openeo_driver.constants import (
    GTIFF,
    NETCDF,
    OPENEO_GEODN_DRIVER_DATA_DIR,
    logger,
)
from shapely.geometry.polygon import Polygon
from shapely.ops import unary_union
import geopandas
from tensorlakehouse_openeo_driver.cos_parser import COSConnector
import pandas as pd

from tensorlakehouse_openeo_driver.processing import GeoDNProcessing
from tensorlakehouse_openeo_driver.save_result import GeoDNImageCollectionResult

app = Celery("tasks")

app.config_from_object("tensorlakehouse_openeo_driver.celeryconfig")
OUTPUT_BUCKET_NAME = "openeo-geodn-driver-output"


@app.task(bind=True)
def create_batch_jobs(
    self,
    job_id: str,
    status: str,
    process: Dict,
    created: str,
    job_options: Dict,
    title: str,
    description: Dict,
) -> Dict[str, Any]:
    job_id = self.request.id
    logger.debug(
        f"tasks::create_batch_jobs - job_id={job_id} status={status} process={process} title={title}"
    )
    # set metadata
    metadata: Dict[str, Any] = {
        "created": created,
        "title": title,  # required by get_result_assets
        "status": status,
        "job_options": job_options,
        "description": description,
    }
    # get metadata from process (input)
    process_metadata = _extract_metadata(process=process)
    # update metadata dict
    metadata.update(process_metadata)
    logger.debug(f"updating task metadata: {metadata}")
    # update task metadata by setting state to created and also its metadata
    self.update_state(
        state=states.STARTED,
        meta=metadata,
    )
    # parse process graph
    processing = GeoDNProcessing()
    parsed_graph = OpenEOProcessGraph(pg_data=process)
    pg_callable = parsed_graph.to_callable(process_registry=processing.process_registry)
    # execute the process graph, i.e., traverse all nodes and execute each one of them
    datacube = pg_callable()
    # store result into COS
    cos = COSConnector(bucket=OUTPUT_BUCKET_NAME)
    media_type = metadata["media_type"]
    assert media_type is not None, f"Error! invalid media type = {media_type}"
    assert isinstance(media_type, str), f"Error! media_type is not a str: {media_type}"
    if media_type.upper() == NETCDF:
        extension = "nc"
    elif media_type.upper() == GTIFF:
        extension = "tif"
    else:
        raise ValueError(f"Missing output format: {media_type}")
    # set filename
    now = pd.Timestamp.now().strftime("%Y%m%dT%H%M%S")
    filename = f"{now}-{job_id}.{extension}"
    path: Path = OPENEO_GEODN_DRIVER_DATA_DIR / filename
    assert isinstance(datacube, GeoDNImageCollectionResult)
    # save file locally
    datacube.save_result(filename=path)
    metadata["filename"] = filename  # required by get_result_assets
    # upload file to COS
    cos.upload_fileobj(key=filename, path=path)
    # create pre-signed url to allow users to download it
    href = cos.create_presigned_link(key=filename)

    metadata["href"] = href  # required by get_result_assets
    logger.debug("Finished")
    return metadata


def _extract_metadata(process: Dict[str, Any]) -> Dict:
    """extract metadata (geometry, time interval) from the process graph

    Args:
        process (Dict[str, Any]): json that contains the process graph

    Returns:
        Dict: metadata
    """
    try:
        process_graph = process["process_graph"]
    except KeyError as e:
        logger.error(f"Error! missing process_graph: {process}")
        raise e
    assert isinstance(process_graph, dict), f"Error! process_graph is not a dict:{process_graph}"
    polygons = list()
    start_datetime = end_datetime = None
    epsg = 4326
    media_type = None
    for node in process_graph.values():
        if node["process_id"] == "load_collection":
            spatial_extent = node["arguments"]["spatial_extent"]
            west = spatial_extent["west"]
            south = spatial_extent["south"]
            east = spatial_extent["east"]
            north = spatial_extent["north"]
            epsg = spatial_extent.get("crs", 4326)
            p = Polygon([[west, south], [west, north], [east, north], [east, south]])
            polygons.append(p)
            temp_start = pd.Timestamp(node["arguments"]["temporal_extent"][0])
            if start_datetime is None or temp_start < start_datetime:
                start_datetime = temp_start
            if node["arguments"]["temporal_extent"][1] is not None:
                temp_end = pd.Timestamp(node["arguments"]["temporal_extent"][1])
                if end_datetime is None or temp_end > end_datetime:
                    end_datetime = temp_end
        elif node["process_id"] == "save_result":
            media_type = node["arguments"]["format"]

    if len(polygons) > 0:
        union_poly = unary_union(polygons)
        geom = geopandas.GeoSeries([union_poly]).__geo_interface__
        bbox = list(union_poly.bounds)
    else:
        bbox = None
        geom = None
    if start_datetime is not None and end_datetime is not None:
        assert start_datetime <= end_datetime
    metadata = {
        "geometry": geom,
        "bbox": bbox,
        "start_datetime": (
            start_datetime.isoformat(sep="T", timespec="seconds")
            if start_datetime is not None
            else None
        ),
        "end_datetime": (
            end_datetime.isoformat(sep="T", timespec="seconds")
            if end_datetime is not None
            else None
        ),
        "epsg": epsg,
        "media_type": media_type,
    }
    return metadata
