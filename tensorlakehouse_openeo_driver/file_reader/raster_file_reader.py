from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from pystac import Item
from tensorlakehouse_openeo_driver.file_reader.cloud_storage_file_reader import (
    CloudStorageFileReader,
)
from openeo_pg_parser_networkx.pg_schema import ParameterReference
import xarray as xr


class RasterFileReader(CloudStorageFileReader):
    DATA = "data"

    def __init__(
        self,
        items: List[Item],
        bands: List[str],
        bbox: Tuple[float, float, float, float],
        temporal_extent: Tuple[datetime, Optional[datetime]],
        properties: Optional[Dict[str, Any]],
    ) -> None:
        super().__init__(items, bands, bbox, temporal_extent, properties)

    def load_items(self) -> xr.DataArray:
        raise NotImplementedError

    def _filter_by_extra_dimensions(self, dataset: xr.Dataset) -> xr.Dataset:
        """extract only dimensions (cube:dimension) from properties

        Returns:
            xr.Dataset: filtered dataset
        """
        if self.properties is not None and isinstance(self.properties, dict):
            # iterate over properties
            for property_name, property_values in self.properties.items():
                # ignore if property is not a dimension
                if property_name.startswith("cube:dimensions"):
                    # split property name into fields
                    fields = property_name.split(".")
                    assert len(fields) >= 2, f"Error! Unexpected fields: {fields=}"
                    # get dimension name
                    dimension_name = fields[1]
                    process_graph = property_values["process_graph"]
                    assert isinstance(
                        process_graph, dict
                    ), f"Error! Unexpected type: {process_graph=}"
                    for process_graph_values in process_graph.values():
                        # get process id which is the filter operation to be applied
                        process_id = process_graph_values["process_id"]
                        # get value
                        arguments = process_graph_values["arguments"]
                        if isinstance(arguments["x"], ParameterReference):
                            value = arguments["y"]
                        else:
                            value = arguments["x"]
                        # apply filter
                        if process_id in ["eq", "="]:
                            dataset = dataset.sel({dimension_name: [value]})

        return dataset
