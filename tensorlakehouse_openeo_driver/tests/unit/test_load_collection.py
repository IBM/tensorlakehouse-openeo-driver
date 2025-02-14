from tensorlakehouse_openeo_driver.process_implementations.load_collection import (
    LoadCollectionFromCOS,
)

import pytest
from openeo_pg_parser_networkx.pg_schema import ParameterReference
import deepdiff


@pytest.mark.parametrize(
    "properties, expected_filter",
    [
        (
            {},
            None,
        ),
        (
            {
                "cloud_coverage": {
                    "process_graph": {
                        "lte1": {
                            "process_id": "lte",
                            "arguments": {
                                "x": ParameterReference(from_parameter="value"),
                                "y": "97",
                            },
                            "result": True,
                        }
                    }
                }
            },
            {
                "op": "<=",
                "args": [
                    {"property": "properties.cloud_coverage"},
                    "97",
                ],
            },
        ),
        (
            {
                "cloud_coverage": {
                    "process_graph": {
                        "lte1": {
                            "process_id": "lte",
                            "arguments": {
                                "x": ParameterReference(from_parameter="value"),
                                "y": "97",
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
            {
                "op": "and",
                "args": [
                    {
                        "op": "<=",
                        "args": [
                            {"property": "properties.cloud_coverage"},
                            "97",
                        ],
                    },
                    {
                        "op": "=",
                        "args": [
                            {"property": "properties.tile"},
                            "T18TYP",
                        ],
                    },
                ],
            },
        ),
        (
            {
                "cube:dimensions.level.values": {
                    "process_graph": {
                        "eq1": {
                            "process_id": "eq",
                            "arguments": {
                                "x": ParameterReference(from_parameter="value"),
                                "y": "97",
                            },
                            "result": True,
                        }
                    }
                }
            },
            {
                "op": "a_contains",
                "args": [
                    {"property": "properties.cube:dimensions.level.values"},
                    "97",
                ],
            },
        ),
    ],
)
def test_convert_properties_to_filter(properties, expected_filter):
    filter_cql = LoadCollectionFromCOS._convert_properties_to_filter(
        properties=properties
    )
    d = deepdiff.DeepDiff(filter_cql, expected_filter)
    assert len(d) == 0, f"Error! not equal: {d}"
