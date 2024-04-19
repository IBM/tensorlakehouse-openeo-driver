import xarray as xr
from typing import Any, Dict, Union

from openeo_pg_parser_networkx.graph import EvalEnv
from openeo_driver.datacube import DriverDataCube
from openeo_processes_dask.process_implementations import reduce_dimension
from openeo.metadata import CollectionMetadata
from xarray import DataArray
from geopandas import GeoDataFrame


class GeoDNDataCube(DriverDataCube):
    def __init__(
        self,
        metadata: CollectionMetadata = None,
        data: Union[DataArray, GeoDataFrame] = None,
    ):
        super().__init__(metadata)
        if data is not None:
            assert isinstance(data, DataArray) or isinstance(data, GeoDataFrame)
        self.data = data

    def reduce_dimension(
        self, reducer, dimension: str, context: Any, env: EvalEnv
    ) -> "GeoDNDataCube":
        assert isinstance(self.data, xr.DataArray), f"Error! {type(self.data)} is not a DataArray"
        raster_cube = self.data.to_array()
        data = reduce_dimension(
            data=raster_cube, reducer=reducer, context=context, dimension=dimension
        )
        return GeoDNDataCube(metadata=self.metadata, data=data)

    def _metadata_to_dict(self) -> Dict[str, Any]:
        dimensions = {d.name: {"type": d.type} for d in self.metadata._dimensions}
        return {"cube:dimensions": dimensions}
