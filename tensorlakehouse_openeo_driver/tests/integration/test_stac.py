from pathlib import Path
from typing import Any, Dict, Optional
import pytest
from pystac_client import Client
from tensorlakehouse_openeo_driver.constants import STAC_DATETIME_FORMAT, STAC_URL
from tensorlakehouse_openeo_driver.stac import STAC
import pandas as pd
from stac_validator import stac_validator

from tensorlakehouse_openeo_driver.tests.unit.unit_test_util import (
    validate_stac_object_against_schema,
)


@pytest.fixture(scope="module")
def client():
    client = Client.open(STAC_URL)
    return client


def is_collection_available(collection_id: str) -> bool:
    stac = STAC(url=STAC_URL)
    collections = stac.list_collections()
    for c in collections["collections"]:
        if collection_id == c["id"]:
            return True
    return False


@pytest.mark.parametrize(
    "client, collection_id, expected_num_items, item_id, stac_filter, fields_flag",
    [
        (
            "client",
            "HLSL30_version2",
            6,
            "HLS.L30.T19MBQ.2021072T145017.v2.0",
            None,
            True,
        ),
        (
            "client",
            "HLSS30",
            56,
            "HLS.S30.T10TFP.2022002T190759.v2.0.Fmask",
            None,
            True,
        ),
    ],
    indirect=["client"],
)
def test_search(
    client: Client,
    collection_id: str,
    expected_num_items: int,
    item_id: str,
    stac_filter: Optional[Dict[str, Any]],
    fields_flag: bool,
):
    stac = STAC(STAC_URL)
    if not stac.is_collection_available(collection_id=collection_id):
        pytest.skip(f"Warning! {collection_id=} is not available")
    else:
        item = stac.get_item(collection_id=collection_id, item_id=item_id)
        bbox = item["bbox"]
        bbox[0] -= 0.1
        bbox[1] -= 0.1
        bbox[2] += 0.1
        bbox[3] += 0.1
        bbox_tuple = tuple(bbox)
        timestamp = item["properties"]["datetime"]
        start = pd.Timestamp(timestamp) - pd.Timedelta(1, unit="day")
        end = pd.Timestamp(timestamp) + pd.Timedelta(1, unit="day")
        dt = f"{start.strftime(STAC_DATETIME_FORMAT)}/{end.strftime(STAC_DATETIME_FORMAT)}"
        if fields_flag:
            fields: Optional[Dict] = {
                "includes": [
                    "id",
                    "bbox",
                    "properties.cube:variables",
                    "properties.cube:dimensions",
                    "properties.datetime",
                ],
                "excludes": [],
            }
        else:
            fields = None
        search_res = client.search(
            collections=[collection_id],
            bbox=bbox_tuple,
            datetime=dt,
            fields=fields,
            limit=100,
        )
        matched_items = list(search_res.items_as_dicts())
        assert len(matched_items) == expected_num_items > 0
        items_ids = list()
        for item in matched_items:
            items_ids.append(item["id"])
            assert isinstance(item, dict)
            if fields is not None:
                for key in fields["includes"]:
                    if key is not None and isinstance(key, str) and "." in key:
                        key = key.split(".")[0]
                    assert (
                        item.get(key) is not None
                    ), f"Error! {key} field is not available"

        assert item_id in items_ids, f"Error! Missing item={item_id}"


@pytest.mark.parametrize(
    "collection_id, item_id",
    [
        ("HLSL30_version2", "HLS.L30.T19MBQ.2021072T145017.v2.0"),
        ("HLSL30_version2", "HLS.L30.T20MPC.2021328T141831.v2.0"),
        ("HLSL30_version2", "HLS.L30.T21KXV.2021315T134623.v2.0"),
        ("HLSL30_version2", "HLS.L30.T22JFS.2021312T131730.v2.0"),
        ("HLSL30_version2", "HLS.L30.T22KBB.2021109T133407.v2.0"),
        ("HLSL30_version2", "HLS.L30.T20NPJ.2021236T142329.v2.0"),
        ("HLSL30_version2", "HLS.L30.T21LWG.2021162T135025.v2.0"),
        ("HLSL30_version2", "HLS.L30.T22KEG.2021134T132639.v2.0"),
        ("HLSL30_version2", "HLS.L30.T19MFT.2021266T143733.v2.0"),
        ("HLSL30_version2", "HLS.L30.T22NFF.2021333T132914.v2.0"),
        ("HLSL30", "HLS.L30.T10SDH.2020197T185144.v2.0.B01"),
        ("HLSL30", "HLS.L30.T10SDH.2021215T185156.v2.0.B07"),
        ("HLSS30", "HLS.S30.T14RNS.2022284T171241.v2.0.B8A"),
        ("HLSS30", "HLS.S30.T14RNS.2022286T170239.v2.0.B01"),
        ("HLSS30", "HLS.S30.T14RNS.2022286T170239.v2.0.B03"),
        ("HLSS30", "HLS.S30.T14RNS.2022286T170239.v2.0.B05"),
        ("HLSS30", "HLS.S30.T14RNS.2022286T170239.v2.0.B07"),
        ("HLSS30", "HLS.S30.T14RNS.2022286T170239.v2.0.B09"),
        ("HLSS30", "HLS.S30.T14RNS.2022286T170239.v2.0.B08"),
        ("HLSS30", "HLS.S30.T14RNS.2022286T170239.v2.0.B10"),
        ("HLSS30", "HLS.S30.T14RNS.2022286T170239.v2.0.B12"),
        ("HLSS30", "HLS.S30.T14RNS.2022286T170239.v2.0.Fmask"),
        ("HLSS30", "HLS.S30.T14RNS.2022289T171309.v2.0.B02"),
        ("HLSS30", "HLS.S30.T14RNS.2022289T171309.v2.0.B04"),
        ("HLSS30", "HLS.S30.T14RNS.2022286T170239.v2.0.B11"),
        ("HLSS30", "HLS.S30.T14RNS.2022286T170239.v2.0.B8A"),
        ("HLSS30", "HLS.S30.T14RNS.2022289T171309.v2.0.B01"),
        ("HLSS30", "HLS.S30.T14RNS.2022289T171309.v2.0.B03"),
        ("HLSS30", "HLS.S30.T14RNS.2022289T171309.v2.0.B05"),
    ],
)
def test_validate_items_using_stac_validator(collection_id: str, item_id: str):
    # make a request to get /collections/{cid}/items/{iid}
    stac = STAC(STAC_URL)
    if not stac.is_collection_available(collection_id=collection_id):
        pytest.skip(f"Warning! {collection_id=} is not available")
    else:
        item = stac.get_item(collection_id=collection_id, item_id=item_id)

        item_schema_path = Path(
            "tensorlakehouse_openeo_driver/tests/schemas/item-spec/json-schema/item.json"
        )
        assert item_schema_path.exists()

        stac_valid = stac_validator.StacValidate(extensions=True)
        stac_valid.validate_dict(item)
        for m in stac_valid.message:
            assert m["valid_stac"], f"Error! message={m}"


@pytest.mark.skip("Error! Missing stac_extensions")
# @pytest.mark.parametrize(
#     "collection_id, item_id",
#     [
#         ("HLSL30_version2", "HLS.L30.T19MBQ.2021072T145017.v2.0"),
#         ("global-weather-era5", "global-weather-era5"),
#     ],
# )
def test_validate_items_using_jsonschema(collection_id: str, item_id: str):
    # make a request to get /collections/{cid}/items/{iid}
    stac = STAC(STAC_URL)
    if not stac.is_collection_available(collection_id=collection_id):
        pytest.skip(f"Warning! {collection_id=} is not available")
    else:
        item = stac.get_item(collection_id=collection_id, item_id=item_id)
        try:
            stac_extensions = item["stac_extensions"]
        except KeyError as e:
            print(item)
            print(e)
            raise e

        # method 1: validate STAC item using jsonschema
        item_schema_path = Path(
            "tensorlakehouse_openeo_driver/tests/schemas/item-spec/json-schema/item.json"
        )
        assert item_schema_path.exists()
        validate_stac_object_against_schema(
            stac_object=item, schema_path=item_schema_path
        )

        if any("projection" in ext for ext in stac_extensions):
            project_schema_path = Path(
                "tensorlakehouse_openeo_driver/tests/schemas/projection/schema.json"
            )
            assert project_schema_path.exists()
            validate_stac_object_against_schema(
                stac_object=item, schema_path=project_schema_path
            )

        if any("datacube" in ext for ext in stac_extensions):
            datacube_schema_path = Path(
                "tensorlakehouse_openeo_driver/tests/schemas/datacube/schema.json"
            )
            assert datacube_schema_path.exists()
            validate_stac_object_against_schema(
                stac_object=item, schema_path=datacube_schema_path
            )


@pytest.mark.parametrize(
    "collection_id",
    [
        "HLSS30",
        "esa-sentinel-2A-msil1c",
        "ibm-eis-ga-1-esa-sentinel-2-l2a",
        "HLSL30_version2",
        # both era5 collections have invalid license
        "global-weather-era5",
        "atmospheric-era5",
    ],
)
def test_validate_collections(collection_id: str):
    stac = STAC(STAC_URL)
    stac_valid = stac_validator.StacValidate(extensions=True)
    if stac.is_collection_available(collection_id=collection_id):
        collection = stac.get_collection(collection_id=collection_id)
        stac_valid.validate_dict(collection)
        for m in stac_valid.message:
            assert m["valid_stac"], f"Error! message={m}"
