from typing import Dict, List, Optional, Tuple

import pyproj
from tensorlakehouse_openeo_driver.constants import (
    DEFAULT_BANDS_DIMENSION,
    DEFAULT_TIME_DIMENSION,
    DEFAULT_X_DIMENSION,
    DEFAULT_Y_DIMENSION,
    GEODN_DATASERVICE_ENDPOINT,
    GEODN_DATASERVICE_PASSWORD,
    GEODN_DATASERVICE_USER,
    GEODN_DISCOVERY_CRS,
    GEODN_DISCOVERY_PASSWORD,
    GEODN_DISCOVERY_USERNAME,
    logger,
)
from tensorlakehouse_openeo_driver.geodn_discovery import GeoDNDiscovery
from tensorlakehouse_openeo_driver.layer import LayerMetadata
from tensorlakehouse_openeo_driver.process_implementations.load_collection import (
    AbstractLoadCollection,
)
import dataservice.dask
import dataservice.query
from openeo_pg_parser_networkx.pg_schema import (
    BoundingBox,
    TemporalInterval,
)
import xarray as xr
import pandas as pd


class LoadCollectionFromHBase(AbstractLoadCollection):
    def __init__(self) -> None:
        super().__init__()
        assert GEODN_DISCOVERY_USERNAME is not None
        assert GEODN_DISCOVERY_PASSWORD is not None
        self.discovery = GeoDNDiscovery(
            client_id=GEODN_DISCOVERY_USERNAME, password=GEODN_DISCOVERY_PASSWORD
        )

    def load_collection(
        self,
        id: str,
        spatial_extent: BoundingBox,
        temporal_extent: TemporalInterval,
        bands: List[str],
        dimensions: Dict[str, str],
        properties=None,
    ) -> xr.DataArray:
        """retrieves data from GeoDN.Discovery based on specified spatial and temporal extent

        Args:
            id (str): collection ID
            spatial_extent (BoundingBox): _description_
            temporal_extent (TemporalInterval): _description_
            bands (Optional[List[str]]): _description_
            properties (_type_, optional): _description_. Defaults to None.

        Raises:
            NotImplementedError: _description_

        Returns:
            xr.DataArray: a data cube which has x, y, bands dimensions and optionally t dimension
        """
        logger.debug(f"Loading collection via dataservice: collection_id={id} bands={bands}")
        # TODO temporary fix because the openeo_pg_parser_networkx converts str to float
        #  for instance, a "9" str is converted to 9.0 float
        if isinstance(id, float):
            id = str(int(id))
        elif isinstance(id, int):
            id = str(id)
        # get collection metadata
        layer_metadata_list = self.get_geodn_layer_metadata(collection_id=id, bands=bands)

        # extract coordinates from BoundingBox object
        lonmin, latmin, lonmax, latmax = LoadCollectionFromHBase._get_bounding_box(
            spatial_extent=spatial_extent
        )
        # convert temporal extent to datetime
        starttime = pd.Timestamp(temporal_extent.start.to_numpy()).to_pydatetime()
        endtime = pd.Timestamp(temporal_extent.end.to_numpy()).to_pydatetime()

        logger.debug(f"Getting global timestamps from {GEODN_DATASERVICE_ENDPOINT}")
        arrays = list()
        # for each band, download and create xr.DataArray
        for layer_metadata in layer_metadata_list:
            # find available global timestamps
            logger.debug(f"Getting global timestamps from {GEODN_DATASERVICE_ENDPOINT}")
            # TODO can we search available timestamps of the specified area?
            timestamps = dataservice.query.get_global_timestamps(
                layer_id=layer_metadata.layer_id,
                starttime=starttime,
                endtime=endtime,
                dataserviceendpoint=GEODN_DATASERVICE_ENDPOINT,
                username=GEODN_DATASERVICE_USER,
                password=GEODN_DATASERVICE_PASSWORD,
                count=None,
            )
            assert (
                len(timestamps) > 0
            ), f"Error! no available timestamp between {starttime} and {endtime} for layer {layer_metadata.layer_id}"
            # retrieve data from GeoDN.Discovery and load it as xr.DataArray
            data_array = dataservice.dask.to_xarray(
                layer_id=layer_metadata.layer_id,
                latmax=latmax,
                latmin=latmin,
                lonmax=lonmax,
                lonmin=lonmin,
                timestamps=timestamps,
                level=layer_metadata.level,
                dataserviceendpoint=GEODN_DATASERVICE_ENDPOINT,
                username=GEODN_DATASERVICE_USER,
                password=GEODN_DATASERVICE_PASSWORD,
            )
            arrays.append(data_array)
        # create a xr.Dataset merging xr.DataArray of all bands
        da: xr.DataArray
        da = xr.concat(arrays, pd.Index(bands, name=DEFAULT_BANDS_DIMENSION))
        # dataservice_sdk returns a xarray.DataArray that has dimensions 'lat', 'lon' and 'time'
        dimension_name = {
            "lat": dimensions[DEFAULT_Y_DIMENSION],
            "lon": dimensions[DEFAULT_X_DIMENSION],
            "time": dimensions[DEFAULT_TIME_DIMENSION],
        }

        da = da.rename(dimension_name)
        da.rio.write_crs(GEODN_DISCOVERY_CRS, inplace=True)

        return da

    @staticmethod
    def _get_bounding_box(
        spatial_extent: BoundingBox,
    ) -> Tuple[float, float, float, float]:
        """get bounds

        Args:
            spatial_extent (BoundingBox): _description_

        Returns:
            Tuple[float, float, float, float]: west, south, east, north
        """

        latmax = spatial_extent.north
        latmin = spatial_extent.south
        lonmax = spatial_extent.east
        lonmin = spatial_extent.west
        pyproj_crs = pyproj.CRS.from_string(spatial_extent.crs)
        epsg4326 = pyproj.CRS.from_epsg(4326)

        if pyproj_crs != epsg4326:
            lonmin, latmin, lonmax, latmax = AbstractLoadCollection._to_epsg4326(
                latmax=latmax,
                latmin=latmin,
                lonmax=lonmax,
                lonmin=lonmin,
                crs_from=pyproj_crs,
            )
        assert 90 >= latmax >= latmin >= -90, f"Error! latmax < latmin: {latmax} < {latmin}"

        assert 180 >= lonmax >= lonmin >= -180, f"Error! lonmax < lonmin: {lonmax} < {lonmin}"
        return lonmin, latmin, lonmax, latmax

    def get_geodn_layer_metadata(
        self, collection_id: str, bands: Optional[List[str]]
    ) -> List[LayerMetadata]:
        """get GeoDN Discovery layer metadata using collection_id and bands
        mapping between dataset and collection
        Args:
            collection_id (str): _description_
            bands (List[str]): _description_

        Returns:
            List[LayerMetadata]: list of objects that correspond to the specified bands
        """
        dataset_metadata_list = self.discovery.list_datasets()
        filtered_layer_metadata = list()
        # initialize variables to find dataset
        i = 0
        found_dset = False
        while i < len(dataset_metadata_list) and not found_dset:
            dset_metadata = dataset_metadata_list[i]
            i += 1
            # if dataset is found
            if dset_metadata.collection_id == collection_id:
                # list layers
                all_layer_metadata_list = self.discovery.list_datalayers_from_dataset(
                    dataset_id=dset_metadata.dataset_id, level=dset_metadata.level
                )
                # if bands parameters is None or empty, append all layers
                if bands is None or (isinstance(bands, list) and len(bands) == 0):
                    filtered_layer_metadata.extend(all_layer_metadata_list)
                else:
                    # initialize variables and find corresponding layers
                    for band in bands:
                        j = 0
                        found_layer = False
                        while j < len(all_layer_metadata_list) and not found_layer:
                            layer_metadata = all_layer_metadata_list[j]
                            j += 1
                            if band == layer_metadata.band:
                                filtered_layer_metadata.append(layer_metadata)
                                found_layer = True
                found_dset = True
        return filtered_layer_metadata
