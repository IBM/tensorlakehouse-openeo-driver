from typing import List, Optional
import pandas as pd
import xarray as xr
import openeo
from openeo_pg_parser_networkx.pg_schema import (
    TemporalInterval,
    BoundingBox,
)


def load_collection_agg_time(
    collection_id: str,
    spatial_extent: BoundingBox,
    temporal_extent: TemporalInterval,
    bands: Optional[List[str]],
) -> xr.DataArray:
    stac_datetime_format = "%Y-%m-%dT%H:%M:%S.000Z"

    start = pd.Timestamp(temporal_extent.start.to_numpy()).to_pydatetime()
    end = pd.Timestamp(temporal_extent.end.to_numpy()).to_pydatetime()
    temporal_extent = [
        start.strftime(stac_datetime_format),
        end.strftime(stac_datetime_format),
    ]

    filename = "sample_file_sentinel2.tiff"
    connection = openeo.connect("openeo.cloud").authenticate_oidc()
    datacube = connection.load_collection(
        collection_id=collection_id,
        spatial_extent={
            "south": spatial_extent.south,
            "west": spatial_extent.west,
            "north": spatial_extent.north,
            "east": spatial_extent.east,
        },
        temporal_extent=temporal_extent,
        bands=bands,
    )
    datacube = datacube.min_time()
    result = datacube.save_result("GTiff")
    result.download(filename)
    ds = xr.open_dataset(filename)
    return ds.to_array()
