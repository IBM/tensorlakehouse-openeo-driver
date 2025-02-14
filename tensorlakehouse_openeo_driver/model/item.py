from typing import Dict, Optional, Tuple
import pandas as pd
from shapely.geometry.polygon import Polygon

from tensorlakehouse_openeo_driver.constants import (
    DEFAULT_X_DIMENSION,
    DEFAULT_Y_DIMENSION,
    SENTINEL_2_L1C,
    SENTINEL_2_L2A,
)
from tensorlakehouse_openeo_driver.geospatial_utils import (
    from_bbox_to_polygon,
    from_geojson_to_polygon,
)
from tensorlakehouse_openeo_driver.stac.stac_utils import get_dimension_names


def make_item(item_dict: Dict) -> "Item":
    item_id = item_dict["id"]
    collection_id = item_dict["collection"]
    assert isinstance(collection_id, str)
    bbox_list = item_dict["bbox"]
    assert len(bbox_list) == 4
    bbox: Tuple[float, float, float, float] = tuple(bbox_list)  # type: ignore
    item_prop = item_dict["properties"]
    dt_str = item_prop.get("datetime")
    tile_id = item_prop.get("tileId", None)
    geometry = item_dict["geometry"]

    cube_dims = item_prop.get("cube:dimensions")
    if cube_dims is not None:
        try:
            dimension_names = get_dimension_names(cube_dimensions=cube_dims)
        except KeyError as e:
            msg = f"Key error: Invalid item = {item_id} - missing field: {e}"
            raise KeyError(msg)
        x_dim_name = dimension_names[DEFAULT_X_DIMENSION]
        x_resolution = cube_dims[x_dim_name]["step"]

        y_dim_name = dimension_names[DEFAULT_Y_DIMENSION]
        y_resolution = cube_dims[y_dim_name]["step"]

        epsg = cube_dims[x_dim_name]["reference_system"]
    else:
        x_resolution = y_resolution = epsg = None

    if geometry is not None:
        geometry = from_geojson_to_polygon(geometry)
    if dt_str is not None:
        dt = pd.Timestamp(dt_str)
        start = end = None

    else:
        start_dt_str = item_prop.get("start_datetime")
        end_dt_str = item_prop.get("end_datetime")
        dt = None
        start = pd.Timestamp(start_dt_str, tz="UTC")
        end = pd.Timestamp(end_dt_str, tz="UTC")
    hrefs = set()
    if collection_id == "SENTINEL-2":
        for v in item_dict["assets"]["PRODUCT"]["alternate"].values():
            hrefs.add(v["href"])
    else:
        for v in item_dict["assets"].values():
            hrefs.add(v["href"])

    if "HLS" in collection_id.upper():
        return HLSItem(
            bbox=bbox,
            datetime=dt,
            start=start,
            end=end,
            collection_id=collection_id,
            item_id=item_id,
            hrefs=hrefs,
            geometry=geometry,
            tile_id=tile_id,
            x_resolution=x_resolution,
            y_resolution=y_resolution,
            epsg=epsg,
        )
    elif collection_id == SENTINEL_2_L2A:
        return Sentinel2Item(
            bbox=bbox,
            datetime=dt,
            start=start,
            end=end,
            collection_id=collection_id,
            item_id=item_id,
            hrefs=hrefs,
            geometry=geometry,
            tile_id=tile_id,
            x_resolution=x_resolution,
            y_resolution=y_resolution,
            epsg=epsg,
        )
    elif collection_id == SENTINEL_2_L1C or (
        collection_id == "SENTINEL-2" and item_prop["productType"] == "S2MSI1C"
    ):
        return Sentinel2L1CItem(
            bbox=bbox,
            datetime=dt,
            start=start,
            end=end,
            collection_id=collection_id,
            item_id=item_id,
            hrefs=hrefs,
            geometry=geometry,
            tile_id=tile_id,
            x_resolution=x_resolution,
            y_resolution=y_resolution,
            epsg=epsg,
        )
    else:
        return Sentinel1Item(
            bbox=bbox,
            datetime=dt,
            start=start,
            end=end,
            collection_id=collection_id,
            item_id=item_id,
            hrefs=hrefs,
            geometry=geometry,
            tile_id=tile_id,
            x_resolution=x_resolution,
            y_resolution=y_resolution,
            epsg=epsg,
        )


class Item:
    DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

    def __init__(
        self,
        item_id: str,
        bbox: Tuple[float, float, float, float],
        datetime: Optional[pd.Timestamp],
        start: Optional[pd.Timestamp],
        end: Optional[pd.Timestamp],
        collection_id: str,
        hrefs: set,
        geometry: Optional[Polygon],
        x_resolution: Optional[float] = None,
        y_resolution: Optional[float] = None,
        epsg: Optional[int] = None,
    ):
        assert isinstance(item_id, str)
        self.item_id = item_id
        assert isinstance(bbox, tuple), f"Error! not a tuple: {bbox}"
        assert len(bbox) == 4
        assert all(isinstance(c, float) for c in bbox)
        west, south, east, north = bbox
        assert -180 <= west <= east <= 180, f"Error! Invalid longitude: {west=} {east=}"
        assert (
            -90 <= south <= north <= 90
        ), f"Error! Invalid latitude: {south=} {north=}"
        self.bbox = bbox
        self.datetime = datetime
        self._start = start
        self._end = end
        if datetime is None:
            assert isinstance(start, pd.Timestamp)
            assert isinstance(end, pd.Timestamp)
            assert start <= end
            self._start = start
            self._end = end
        assert isinstance(collection_id, str)
        self.collection_id = collection_id
        assert isinstance(hrefs, set)
        assert len(hrefs) > 0
        self.hrefs = hrefs
        self._geometry = geometry
        self.x_resolution = x_resolution
        self.y_resolution = y_resolution
        self.epsg: Optional[int] = epsg

    def __str__(self) -> str:
        s = f"Item id={self.item_id} collection={self.collection_id} bbox={self.bbox} datetime={self.datetime}"
        return s

    def to_params(self) -> Dict:
        if self.datetime is None:
            start = self.start_datetime
            end = self.end_datetime
        else:
            delta = pd.Timedelta(value=1, unit="second")
            start = self.datetime - delta
            end = self.datetime + delta
        assert isinstance(start, pd.Timestamp)
        assert isinstance(end, pd.Timestamp)
        temporal_extent = [start.isoformat(sep="T"), end.isoformat(sep="T")]
        params = {
            "bbox": self.bbox,
            "temporal_extent": temporal_extent,
            "id": self.item_id,
        }
        return params

    @property
    def start_datetime(self) -> Optional[pd.Timestamp]:
        return self._start

    @property
    def end_datetime(self) -> Optional[pd.Timestamp]:
        return self._end

    @property
    def west(self) -> float:
        west = self.bbox[0]
        return west

    @property
    def south(self) -> float:
        south = self.bbox[1]
        return south

    @property
    def east(self) -> float:
        east = self.bbox[2]
        return east

    @property
    def north(self) -> float:
        north = self.bbox[3]
        return north

    @property
    def geometry(self) -> Polygon:
        if self._geometry is not None:
            return self._geometry
        else:
            return from_bbox_to_polygon(
                bbox=(self.west, self.south, self.east, self.north)
            )


class TiledItem(Item):

    def __init__(
        self,
        item_id: str,
        bbox: Tuple[float, float, float, float],
        datetime: Optional[pd.Timestamp],
        start: Optional[pd.Timestamp],
        end: Optional[pd.Timestamp],
        collection_id: str,
        hrefs: set,
        geometry: Optional[Polygon],
        tile_id: Optional[str] = None,
        x_resolution: Optional[float] = None,
        y_resolution: Optional[float] = None,
        epsg: Optional[int] = None,
    ):
        super().__init__(
            item_id=item_id,
            bbox=bbox,
            datetime=datetime,
            start=start,
            end=end,
            collection_id=collection_id,
            hrefs=hrefs,
            geometry=geometry,
            x_resolution=x_resolution,
            y_resolution=y_resolution,
            epsg=epsg,
        )

        self._tile_id = tile_id

    @property
    def tile_id(self) -> Optional[str]:
        return self._tile_id

    def to_params(self) -> Dict:
        # set spatial bounds
        # spatial_bounds = f"{self.south}_{self.north}_{self.west}_{self.east}"
        # set temporal bounds
        if self.datetime is None:
            start = self.start_datetime
            end = self.end_datetime
        else:
            delta = pd.Timedelta(value=1, unit="second")
            start = self.datetime - delta
            end = self.datetime + delta
        assert isinstance(start, pd.Timestamp)
        assert isinstance(end, pd.Timestamp)
        start = start.strftime("%m_%Y")
        end = end.strftime("%m_%Y")
        temporal_bounds = f"{start}_{end}"
        if "s30" in self.collection_id.lower():
            geodn_collection_id = "HLS_S30"
        else:
            geodn_collection_id = "HLS_L30"

        params = {
            # "spatial_bounds": spatial_bounds,
            "temporal_bounds": temporal_bounds,
            "hls_tiles": self.tile_id,
            "check_geodn_stac": "0",
            "geodn_collection_id": geodn_collection_id,
            "log_level": "DEBUG",
        }
        return params


class HLSItem(TiledItem):

    def __init__(
        self,
        item_id: str,
        bbox: Tuple[float, float, float, float],
        datetime: Optional[pd.Timestamp],
        start: Optional[pd.Timestamp],
        end: Optional[pd.Timestamp],
        collection_id: str,
        hrefs: set,
        geometry: Optional[Polygon],
        tile_id: Optional[str] = None,
        x_resolution: Optional[float] = None,
        y_resolution: Optional[float] = None,
        epsg: Optional[int] = None,
    ):
        super().__init__(
            item_id=item_id,
            bbox=bbox,
            datetime=datetime,
            start=start,
            end=end,
            collection_id=collection_id,
            hrefs=hrefs,
            geometry=geometry,
            x_resolution=x_resolution,
            y_resolution=y_resolution,
            epsg=epsg,
        )

        self._tile_id = tile_id

    @property
    def tile_id(self) -> Optional[str]:
        if self._tile_id is None:
            fields = self.item_id.split(".")
            assert (
                len(fields) >= 3
            ), f"Error! This item id does not contain the expected number of dots: {self.item_id}"
            self._tile_id = fields[2]
        assert isinstance(
            self._tile_id, str
        ), f"Error! tile_id is not a str: {self._tile_id} - unable to extract tile id from item id: {self.item_id}"
        if self._tile_id.startswith("T"):
            self._tile_id = self._tile_id[1:]
        return self._tile_id


class Sentinel2Item(TiledItem):

    def __init__(
        self,
        item_id: str,
        bbox: Tuple[float, float, float, float],
        datetime: Optional[pd.Timestamp],
        start: Optional[pd.Timestamp],
        end: Optional[pd.Timestamp],
        collection_id: str,
        hrefs: set,
        geometry: Optional[Polygon],
        tile_id: Optional[str] = None,
        x_resolution: Optional[float] = None,
        y_resolution: Optional[float] = None,
        epsg: Optional[int] = None,
    ):
        super().__init__(
            item_id=item_id,
            bbox=bbox,
            datetime=datetime,
            start=start,
            end=end,
            collection_id=collection_id,
            hrefs=hrefs,
            geometry=geometry,
            x_resolution=x_resolution,
            y_resolution=y_resolution,
            epsg=epsg,
        )

        self._tile_id = tile_id

    @property
    def tile_id(self) -> Optional[str]:
        """
        this is an example of item id S2B_MSIL2A_20240813T165849_N0511_R069_T14RQU_20240813T222450
        """
        if self._tile_id is None:
            fields = self.item_id.split("_")
            assert (
                len(fields) >= 6
            ), f"Error! This item id does not contain dots as expected: {self.item_id}"
            self._tile_id = fields[5]
        if self._tile_id.startswith("T"):
            self._tile_id = self._tile_id[1:]
        return self._tile_id


class Sentinel1Item(TiledItem):

    def __init__(
        self,
        item_id: str,
        bbox: Tuple[float, float, float, float],
        datetime: Optional[pd.Timestamp],
        start: Optional[pd.Timestamp],
        end: Optional[pd.Timestamp],
        collection_id: str,
        hrefs: set,
        geometry: Optional[Polygon],
        tile_id: Optional[str] = None,
        x_resolution: Optional[float] = None,
        y_resolution: Optional[float] = None,
        epsg: Optional[int] = None,
    ):
        super().__init__(
            item_id=item_id,
            bbox=bbox,
            datetime=datetime,
            start=start,
            end=end,
            collection_id=collection_id,
            hrefs=hrefs,
            geometry=geometry,
            x_resolution=x_resolution,
            y_resolution=y_resolution,
            epsg=epsg,
        )

        self._tile_id = tile_id

    @property
    def tile_id(self) -> Optional[str]:
        if self._tile_id is None:
            return "none"
        else:
            return self._tile_id


class Sentinel2L1CItem(Sentinel2Item):

    def __init__(
        self,
        item_id,
        bbox,
        datetime,
        start,
        end,
        collection_id,
        hrefs,
        geometry,
        tile_id=None,
        x_resolution=None,
        y_resolution=None,
        epsg=None,
    ):
        super().__init__(
            item_id,
            bbox,
            datetime,
            start,
            end,
            collection_id,
            hrefs,
            geometry,
            tile_id,
            x_resolution,
            y_resolution,
            epsg,
        )

    def to_params(self):
        # TODO download_path
        assert len(self.hrefs) == 1, f"Error! Unexpected size of {self.hrefs=}"
        href_list = list(self.hrefs)
        d = {"s3_download_path": href_list[0]}
        return d
