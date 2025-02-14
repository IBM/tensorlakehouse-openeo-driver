from typing import Any, Dict
import uuid
import pandas as pd
from tensorlakehouse_openeo_driver.constants import (
    DEFAULT_BANDS_DIMENSION,
    DEFAULT_TIME_DIMENSION,
)
from pystac import Asset, Item

from tensorlakehouse_openeo_driver.geospatial_utils import (
    convert_bbox_to_polygon,
    to_geojson,
)


def get_dimension_names(cube_dimensions: Dict[str, Any]) -> Dict[str, str]:
    """this method parses the cube:dimensions field from STAC and extracts the type and name of
    each dimension in order to support load_collection process to rename the dimensions according
    to the way they were specified in STAC

    Args:
        cube_dimensions (Dict[str, Any]): this is field cube:dimensions as specified by datacube
            STAC extension

    Returns:
        Dict[str, str]: _description_
    """
    dimension_names = dict()
    for name, value in cube_dimensions.items():
        # type is a mandatory field
        dimension_type = value["type"]

        # if this is a horizontal spatial dimension, then it has axis (either x, y, or z)
        if dimension_type == "spatial":
            axis = value["axis"]
            dimension_names[axis] = name
        elif dimension_type == "temporal":
            dimension_names[DEFAULT_TIME_DIMENSION] = name
        elif dimension_type == "bands":
            dimension_names[DEFAULT_BANDS_DIMENSION] = name
        else:
            dimension_names[dimension_type] = name
    return dimension_names


def make_pystac_item(item_as_dict: Dict) -> Item:
    """create pystac.Item given a dictionary

    Args:
        item_as_dict (Dict): item as dict

    Returns:
        Item: pystac.Item
    """
    assets = dict()
    for k, v in item_as_dict["assets"].items():
        href = v["href"]
        role = v.get("roles")
        asset = Asset(href=href, roles=role)
        assets[k] = asset
    random_id = uuid.uuid4().hex
    bbox = item_as_dict["bbox"]
    item_id = item_as_dict.get("id", random_id)
    item_properties = item_as_dict["properties"]
    dt = start_dt = end_dt = None
    if item_properties.get("datetime") is not None:
        dt = pd.Timestamp(item_properties["datetime"]).to_pydatetime()
    else:
        start_dt = pd.Timestamp(item_properties["start_datetime"]).to_pydatetime()
        end_dt = pd.Timestamp(item_properties["end_datetime"]).to_pydatetime()
    poly = convert_bbox_to_polygon(bbox=bbox)
    geom = to_geojson(geom=poly, output_format="dict")
    item = Item(
        id=item_id,
        bbox=item_as_dict["bbox"],
        start_datetime=start_dt,
        properties=item_properties,
        end_datetime=end_dt,
        datetime=dt,
        assets=assets,
        geometry=geom,
        stac_extensions=[
            "https://stac-extensions.github.io/datacube/v2.2.0/schema.json"
        ],
    )

    return item
