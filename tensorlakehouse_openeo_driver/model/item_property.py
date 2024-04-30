from typing import Any, Dict, List, Optional, Union
from tensorlakehouse_openeo_driver.model.datacube_variable import DataCubeVariable

from tensorlakehouse_openeo_driver.model.dimension import (
    Dimension,
    HorizontalSpatialDimension,
    TemporalDimension,
)


class ItemProperties:
    def __init__(self, variables: List[DataCubeVariable], dimensions: List[Dimension]) -> None:
        self._variables = variables
        self._dimensions = dimensions

    @staticmethod
    def make_item_properties(prop: Dict):
        dimensions: List[Dimension] = list()
        cube_dimensions = prop["cube:dimensions"]
        for description, cube_dim in cube_dimensions.items():
            if cube_dim["type"] == "spatial":
                dimensions.append(
                    HorizontalSpatialDimension(
                        axis=cube_dim.get("axis"),
                        extent=cube_dim.get("extent"),
                        description=description,
                        reference_system=cube_dim.get("reference_system"),
                        step=cube_dim.get("step"),
                    )
                )
            elif cube_dim["type"] == "temporal":
                temp_dim = TemporalDimension(
                    extent=cube_dim.get("extent"),
                    description=description,
                    step=cube_dim.get("step"),
                    values=cube_dim.get("values"),
                )
                dimensions.append(temp_dim)
        variables = list()
        cube_variables = prop["cube:variables"]
        for var_description, cube_var in cube_variables.items():
            dim_descriptions = cube_var["dimensions"]
            var_dimensions = list()
            for var_dim_descr in dim_descriptions:
                for dim in dimensions:
                    if var_dim_descr == dim.description:
                        var_dimensions.append(dim)
                        break

            unit = cube_var["unit"]
            type = cube_var["type"]
            variables.append(
                DataCubeVariable(
                    dimensions=var_dimensions,
                    description=var_description,
                    unit=unit,
                    type=type,
                )
            )
        return ItemProperties(variables=variables, dimensions=dimensions)

    @property
    def variables(self):
        return self._variables

    @property
    def dimensions(self):
        return self._dimensions

    def get_dimensions(
        self, filter_type: str = "spatial"
    ) -> List[Union[HorizontalSpatialDimension, TemporalDimension]]:
        selected_dims = list()
        for d in self.dimensions:
            if d.type == filter_type:
                selected_dims.append(d)
        return selected_dims

    def get_dimension(
        self, description: Optional[str] = None, axis: Optional[str] = None
    ) -> Optional[Union[TemporalDimension, HorizontalSpatialDimension]]:
        """get a dimension given the specified description or axis

        Args:
            description (Optional[str]): name of the dimension
            axis (Optional[str]): x and y - only HorizontalSpaceDimensions

        Returns:
            Optional[Union[TemporalDimension, HorizontalSpatialDimension]]: _description_
        """
        if description is None:
            assert axis is not None
        if axis is None:
            assert description is not None

        dim = None
        found = False
        i = 0
        while i < len(self.dimensions) and not found:
            dim = self.dimensions[i]
            if (dim.description == description) or (
                isinstance(dim, HorizontalSpatialDimension) and dim.axis == axis
            ):
                found = True
            i += 1
        return dim

    def get_variable(self, variable: str) -> Optional[DataCubeVariable]:
        """get cube variable given the specified variable name

        Args:
            variable (str): name of the variable

        Returns:
            DataCubeVariable: data cube variable object
        """
        for v in self.variables:
            if v.description == variable:
                assert isinstance(v, DataCubeVariable), f"Error! Unexpected type: {v=}"
                return v
        return None

    def to_dict(self) -> Dict[str, Any]:
        data: Dict[str, Any] = dict()
        for var in self.variables:
            data = data | var.to_dict()
        return data

    def get_epsg(self) -> Optional[int]:
        epsg = None
        for dim in self.dimensions:
            if (
                dim.type == "spatial"
                and hasattr(dim, "reference_system")
                and dim.reference_system is not None
            ):
                epsg = int(dim.reference_system)
                break
        return epsg

    def get_step(self) -> Optional[float]:
        step = None
        for dim in self.dimensions:
            if dim.type == "spatial" and hasattr(dim, "step") and dim.step is not None:
                step = dim.step
                break
        return step
