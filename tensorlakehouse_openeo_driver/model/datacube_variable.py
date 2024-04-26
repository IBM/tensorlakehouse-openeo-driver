from typing import Dict, List, Optional, Union

from tensorlakehouse_openeo_driver.model.dimension import Dimension


class DataCubeVariable:
    def __init__(
        self,
        dimensions: List[Dimension],
        type: str,
        description: Optional[str] = None,
        extent: Optional[List[Union[str, float]]] = None,
        values: Optional[List[Union[str, float]]] = None,
        unit: Optional[str] = None,
    ) -> None:
        """
        Args:
            dimensions (List[Dimension]): The dimensions of the variable. This should refer to keys in the cube:dimensions object or be an empty list if the variable has no dimensions.
            type (List[str]): Type of the variable, either data or auxiliary.
            description (Optional[str], optional):
            extent (Optional[List[Union[str, float]]], optional): If the variable consists of ordinal values, the extent (lower and upper bounds) of the values as two-element array. Use null for open intervals.
            values (Optional[List[Union[str, float]]], optional): An (ordered) list of all values, especially useful for nominal values.
            unit (Optional[str], optional):The unit of measurement for the data, preferably compliant to UDUNITS-2 units (singular).
        """
        assert type in ["data", "auxiliary"]
        self.type = type
        assert len(dimensions) > 0
        assert all(
            isinstance(d, Dimension) for d in dimensions
        ), f"Error! not all items of dimensions list are instance of Dimension: {dimensions}"
        self.dimensions = dimensions
        self.description = description
        self.extent = extent
        self.values = values
        self.unit = unit

    def to_dict(self) -> Dict:
        data = dict()
        data[self.description] = {
            "dimensions": [d.description for d in self.dimensions],
            "type": self.type,
        }
        if self.unit is not None:
            data[self.description]["unit"] = self.unit
        if self.values is not None:
            data[self.description]["values"] = self.values
        if self.extent is not None:
            data[self.description]["extent"] = self.extent
        return data
