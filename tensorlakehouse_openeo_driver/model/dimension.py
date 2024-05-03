import numbers
from typing import Dict, List, Optional, Union
import pandas as pd


import logging
import logging.config

from tensorlakehouse_openeo_driver.constants import (
    DEFAULT_Z_DIMENSION,
    LOGGING_CONF_PATH,
    DEFAULT_X_DIMENSION,
    DEFAULT_Y_DIMENSION,
)

logging.config.fileConfig(fname=LOGGING_CONF_PATH, disable_existing_loggers=False)
logger = logging.getLogger("geodnLogger")


class Dimension:
    def __init__(self, description: str, type: str = "spatial") -> None:
        assert isinstance(description, str), f"Error! not a str {description}"
        self._description = description
        assert isinstance(type, str), f"Error! not a str {type}"
        self._type = type

    @property
    def description(self) -> str:
        return self._description

    @property
    def type(self) -> str:
        return self._type

    def to_dict(self) -> Dict:
        d = dict()
        d[self._description] = {
            "type": self._type,
        }
        return d

    def __str__(self):
        return f"Dimension {self.type} {self.description}"

    def merges(self, other):
        raise NotImplementedError()


class HorizontalSpatialDimension(Dimension):
    """A spatial raster dimension in one of the horizontal (x or y) directions."""

    def __init__(
        self,
        axis: str,
        extent: List[float],
        description: str,
        reference_system: Optional[int] = None,
        step: Optional[float] = None,
        type: str = "spatial",
    ) -> None:
        super().__init__(description, type)
        assert isinstance(axis, str), f"Error! not a str: {axis}"
        assert axis in [
            DEFAULT_X_DIMENSION,
            DEFAULT_Y_DIMENSION,
        ], f"Error! Invalid axis={axis}"
        self._axis = axis
        assert isinstance(extent, list), f"Error! not a list: {extent}"
        assert len(extent) == 2, f"Error! Unexpected size: {extent}"
        assert all(isinstance(x, numbers.Number) for x in extent), f"Error! Invalid type: {extent}"
        self.start = extent[0]
        self.end = extent[1]
        # optional
        self._reference_system = reference_system
        self._step = step

    @property
    def axis(self) -> str:
        return self._axis

    @property
    def start(self) -> float:
        return self._start

    @start.setter
    def start(self, v: float):
        self._start = float(v)

    @property
    def end(self):
        return self._end

    @end.setter
    def end(self, v: float):
        self._end = float(v)

    @property
    def step(self) -> Optional[float]:
        return self._step

    @property
    def extent(self) -> List[float]:
        return [self.start, self.end]

    @property
    def reference_system(self) -> Optional[int]:
        return self._reference_system

    @reference_system.setter
    def reference_system(self, v):
        assert isinstance(v, str), f"Error! Invalid type {v}"
        self._reference_system = v

    def to_dict(self):
        super_d = super().to_dict()
        d = dict()
        d[self.description] = {"axis": self._axis, "extent": self.extent}

        if self.reference_system is not None:
            d[self.description]["reference_system"] = self.reference_system
        # merge super dict with self dict
        d[self.description] = super_d[self.description] | d[self.description]
        return d

    def merges(self, other: "HorizontalSpatialDimension"):
        assert isinstance(other, HorizontalSpatialDimension)

        if self.start < other.start:
            self.start = other.start
        if self.end > other.end:
            self.end = other.end


class VerticalSpatialDimension(Dimension):
    """A spatial raster dimension in one of the horizontal (x or y) directions."""

    def __init__(
        self,
        axis: str,
        description: str,
        extent: Optional[List[float]],
        reference_system: Optional[int] = None,
        step: Optional[float] = None,
        type: str = "spatial",
    ) -> None:
        super().__init__(description, type)
        assert isinstance(axis, str), f"Error! not a str: {axis}"
        assert axis in DEFAULT_Z_DIMENSION, f"Error! Invalid axis={axis}"
        self._axis = axis
        assert isinstance(extent, list), f"Error! not a list: {extent}"
        assert len(extent) == 2, f"Error! Unexpected size: {extent}"
        assert all(isinstance(x, numbers.Number) for x in extent), f"Error! Invalid type: {extent}"
        self.start = extent[0]
        self.end = extent[1]
        # optional
        self._reference_system = reference_system
        self._step = step

    @property
    def axis(self) -> str:
        return self._axis

    @property
    def start(self) -> float:
        return self._start

    @start.setter
    def start(self, v: float):
        self._start = float(v)

    @property
    def end(self):
        return self._end

    @end.setter
    def end(self, v: float):
        self._end = float(v)

    @property
    def step(self) -> Optional[float]:
        return self._step

    @property
    def extent(self) -> List[float]:
        return [self.start, self.end]

    @property
    def reference_system(self) -> Optional[int]:
        return self._reference_system

    @reference_system.setter
    def reference_system(self, v):
        assert isinstance(v, str), f"Error! Invalid type {v}"
        self._reference_system = v

    def to_dict(self):
        super_d = super().to_dict()
        d = dict()
        d[self.description] = {"axis": self._axis, "extent": self.extent}

        if self.reference_system is not None:
            d[self.description]["reference_system"] = self.reference_system
        # merge super dict with self dict
        d[self.description] = super_d[self.description] | d[self.description]
        return d

    def merges(self, other: "VerticalSpatialDimension"):
        assert isinstance(other, VerticalSpatialDimension)

        if self.start < other.start:
            self.start = other.start
        if self.end > other.end:
            self.end = other.end


class TemporalDimension(Dimension):
    def __init__(
        self,
        extent: List[Union[str, None]],
        description: str,
        values: Optional[List[str]] = None,
        step: Optional[str] = None,
        type: str = "temporal",
    ) -> None:
        """

        https://github.com/stac-extensions/datacube#temporal-dimension-object

        Args:
            extent (List[Union[str, None]]): . Extent (lower and upper bounds) of the dimension as two-element array. The dates and/or times must be strings compliant to ISO 8601. null is allowed for open date ranges.
            description (str): _description_
            values (List[str]): _description_
            step (Optional[str]): _description_
            type (str, optional): _description_. Defaults to "spatial".
        """
        assert type == "temporal"
        super().__init__(description, type)
        assert isinstance(extent, list), f"Error! not a list: {extent}"
        assert len(extent) == 2, f"Error! Unexpected size: {extent}"
        start = extent[0]
        assert isinstance(start, str), f"Error! start not a str: {start}"
        try:
            self._start = pd.Timestamp(start)
        except ValueError as e:
            logger.debug(e)
            raise ValueError(e)

        end = extent[1]
        if isinstance(end, str):
            self._end = pd.Timestamp(end)
        else:
            assert end is None
            self._end = end

        self._step = step
        self._values = values

    @property
    def start(self) -> pd.Timestamp:
        return self._start

    @start.setter
    def start(self, v: pd.Timestamp):
        self._start = v

    @property
    def end(self):
        return self._end

    @end.setter
    def end(self, v: Union[pd.Timestamp, None]):
        self._end = v

    @property
    def extent(self) -> List[Optional[str]]:
        if self._end is None:
            end = self._end
        else:
            end = self._end.isoformat()
        intervals = [self._start.isoformat(), end]

        return intervals

    def to_dict(self):
        super_d = super().to_dict()
        d = dict()
        d[self.description] = {
            "extent": self.extent,
        }

        if self._step is not None:
            d[self.description]["step"] = self._step
        if self._values is not None:
            d[self.description]["values"] = self._values
        d[self.description] = super_d[self.description] | d[self.description]
        return d

    def merges(self, other: "TemporalDimension"):
        assert isinstance(other, TemporalDimension)

        if self.start > other.start:
            self.start = other.start

        if self.end is None or (other.end is not None and self.end < other.end):
            self.end = other.end


class BandDimension(Dimension):
    def __init__(self, description: str, values: List[str], type: str = "bands") -> None:
        """
        https://github.com/stac-extensions/datacube/blob/9e74fa706c9bdd971e01739cf18dcc53bdd3dd4f/examples/collection.json#L59
        Args:
            description (str): _description_
            type (str, optional): _description_. Defaults to "bands".
        """
        super().__init__(description, type)
        assert isinstance(values, list)
        assert len(values) > 0
        assert all(isinstance(v, str) for v in values)
        self.values = values

    @property
    def values(self) -> List[str]:
        return self._values

    @values.setter
    def values(self, v: List[str]):
        assert isinstance(v, list)
        self._values = v

    def to_dict(self) -> Dict:
        super_d = super().to_dict()
        data = dict()
        data[self.description] = {"values": self.values}
        data[self.description] = super_d[self.description] | data[self.description]
        return data

    def merges(self, other: "BandDimension"):
        assert isinstance(other, BandDimension)
        self.values = list(set(other.values).union(set(self.values)))
