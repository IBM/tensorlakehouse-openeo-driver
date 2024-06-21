from tensorlakehouse_openeo_driver.process_implementations.load_collection import (
    LoadCollectionFromCOS,
)

from tensorlakehouse_openeo_driver.tests.unit.unit_test_util import (
    HLSS30_ITEMS,
)
import pytest
from openeo_pg_parser_networkx.pg_schema import ParameterReference


@pytest.mark.parametrize(
    "items, properties, expected_result_list",
    [
        (
            HLSS30_ITEMS,
            {
                "cloud_coverage": {
                    "process_graph": {
                        "lte1": {
                            "process_id": "lte",
                            "arguments": {
                                "x": ParameterReference(from_parameter="value"),
                                "y": 97,
                            },
                            "result": True,
                        }
                    }
                }
            },
            [False, True, False],
        ),
        (
            HLSS30_ITEMS,
            {
                "cloud_coverage": {
                    "process_graph": {
                        "lte1": {
                            "process_id": "lte",
                            "arguments": {
                                "x": ParameterReference(from_parameter="value"),
                                "y": 98,
                            },
                            "result": True,
                        }
                    }
                },
                "tile": {
                    "process_graph": {
                        "eq1": {
                            "process_id": "eq",
                            "arguments": {
                                "x": ParameterReference(from_parameter="value"),
                                "y": "T18TYP",
                            },
                            "result": True,
                        }
                    }
                },
            },
            [True, False, False],
        ),
    ],
)
def test_group_items_by_media_type(items, properties, expected_result_list):
    load_coll = LoadCollectionFromCOS()
    for item, expected_res in zip(items, expected_result_list):
        result = load_coll._filter_by_properties(item=item, properties=properties)
        assert result == expected_res
