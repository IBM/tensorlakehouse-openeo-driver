from typing import Optional


class LayerMetadata:
    def __init__(
        self,
        layer_id: str,
        description_short: str,
        name: str,
        dataset_id: int,
        nodata: Optional[str] = None,
        data_type: Optional[str] = None,
        spatial_resolution: Optional[str] = None,
        unit: Optional[str] = None,
        level: Optional[int] = None,
    ) -> None:
        assert layer_id is not None
        if isinstance(layer_id, int):
            layer_id = str(layer_id)
        self._layer_id = layer_id

        self.description_short = description_short
        assert name is not None
        self.name = name
        assert dataset_id is not None
        self.dataset_id = dataset_id
        self.nodata = nodata
        self.data_type = data_type
        self.spatial_resolution = spatial_resolution
        self.unit = unit
        self.level = level

    @property
    def band(self) -> str:
        return self.name

    @property
    def layer_id(self) -> str:
        return self._layer_id
