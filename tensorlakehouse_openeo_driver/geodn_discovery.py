"""
Created Date: Thursday, January 14th 2021, 3:34:36 pm
Author: Leonardo Pondian Tizzei

Copyright (c) 2021 (C) Copyright IBM Corporation 2021
"""

from typing import Any, Dict, List, Optional, Tuple, Union
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging
import logging.config
import os
from datetime import datetime
from tensorlakehouse_openeo_driver.dataset import DatasetMetadata

# import ibmpairs.authentication as authentication
from tensorlakehouse_openeo_driver.constants import GEODN_DISCOVERY_METADATA_URL

from tensorlakehouse_openeo_driver.layer import LayerMetadata

assert os.path.isfile("logging.conf")
logging.config.fileConfig(fname="logging.conf", disable_existing_loggers=False)
logger = logging.getLogger("geodnLogger")


class GeoDNDiscovery:
    """this class manages the connection with PAIRS API. It has been customized to access the
    dataset https://ibmpairs.mybluemix.net/data-explorer?search=488&page_number=1
    since it provides the layer names and tables. Such metadata is not available on PAIRS API

    Returns
    -------
    [type]
        [description]
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        password: Optional[str] = None,
        username: Optional[str] = None,
        # auth_url: str = "https://auth-b2b-twc.ibm.com/auth/GetBearerForClient",
    ) -> None:
        """instantiate PAIRSConn class

        Parameters
        ----------
        access_token: str
        """
        # assert client_id is not None, "Error! client_id cannot be None"
        # self._client_id = client_id
        # assert (
        #     api_key is not None or password is not None
        # ), "Error! Both password and api_key are None"
        self._password = password
        self._username = username
        self._api_key = api_key
        # self._auth_url = auth_url
        self._api_url = GEODN_DISCOVERY_METADATA_URL
        self._access_token = None
        self._password = password
        # credentials = authentication.Basic(username=client_id, password=password)
        # self.pairs_client = client.Client(authentication=credentials)

        self._token_expiration = datetime.now()

    @property
    def api_key(self):
        return self._api_key

    @property
    def password(self):
        return self._password

    @property
    def access_token(self):
        return self._access_token

    @property
    def auth(self) -> Optional[Tuple[str, str]]:
        if self._username is not None and self._password is not None:
            auth = (self._username, self._password)
        else:
            auth = None
        return auth

    @property
    def headers(self):
        headers = {
            "Content-Type": "application/json",
        }
        if self.access_token is not None:
            headers["Authorization"] = "Bearer {}".format(self.access_token)
        return headers

    def get(
        self, endpoint: str, params: Optional[Dict[str, str]] = None
    ) -> Union[List[Dict[Any, Any]], Dict[str, Any]]:
        """this method makes all GET requests to the Discovery

        Args:
            endpoint (str): path (aka route) of the endpoint

        Returns:
            Union[List, Dict]: _description_
        """
        # self._refresh_token()
        url = f"{self._api_url}{endpoint}"
        retry_strategy = Retry(
            total=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        with requests.Session() as session:
            logger.debug(f"GET {url} params={params}")
            session.mount("https://", adapter)
            session.mount("http://", adapter)
            try:
                resp = session.get(
                    url=url,
                    headers=self.headers,
                    params=params,
                    timeout=60,
                    auth=self.auth,
                )
                resp.raise_for_status()
                response = resp.json()
                assert isinstance(response, list) or isinstance(
                    response, dict
                ), f"Error! Unexpected response: {response}"
                return response
            except (requests.exceptions.RetryError, AssertionError) as e:
                logger.error(e)
                raise e

    def list_datalayers_from_dataset(
        self, dataset_id: str, level: Optional[int] = None
    ) -> List[LayerMetadata]:
        list_layers_url = f"/v2/datasets/{dataset_id}/datalayers"
        layers: Union[List[Dict[Any, Any]], Dict[str, Any]] = self.get(
            endpoint=list_layers_url
        )
        assert isinstance(layers, list), f"Error! {layers=}"
        metadata = list()
        for layer in layers:
            assert isinstance(layer, dict)
            layer_id = layer["id"]
            get_layer_url = f"/v2/datalayers/{layer_id}"
            layer_full: Union[List[Dict[Any, Any]], Dict[str, Any]] = self.get(
                endpoint=get_layer_url
            )
            assert isinstance(layer_full, dict), f"Error! {layer_full=}"
            metadata.append(
                LayerMetadata(
                    layer_id=str(layer_full["id"]),
                    description_short=layer_full["description_short"],
                    dataset_id=layer_full["dataset_id"],
                    name=layer_full["name"],
                    unit=layer_full.get("unit"),
                    data_type=layer_full["type"],
                    level=layer_full["level"],
                )
            )
        return metadata

    @staticmethod
    def _make_dataset(d: Dict[str, Any]) -> DatasetMetadata:
        return DatasetMetadata(
            dataset_id=d["id"],
            latitude_min=d.get("latitude_min"),
            latitude_max=d.get("latitude_max"),
            longitude_max=d.get("longitude_max"),
            longitude_min=d.get("longitude_min"),
            temporal_min=d.get("temporal_min"),
            temporal_max=d.get("temporal_max"),
            name=d["name"],
            level=d["level"],
            temporal_resolution_description=d.get("temporal_resolution_description"),
            spatial_resolution_of_raw_data=d.get("spatial_resolution_of_raw_data"),
            temporal_resolution=d.get("temporal_resolution"),
            description_short=d.get("description_short"),
            license=d.get("license_information"),
        )

    def list_datasets(self) -> List[DatasetMetadata]:
        """make a request to GeoDN.Data to retrieve a list of all datasets

        Returns:
            List: _description_
        """
        list_datasets_url = "/v2/datasets"
        datasets: Union[List[Dict[Any, Any]], Dict[str, Any]] = self.get(
            endpoint=list_datasets_url
        )
        assert isinstance(datasets, list), f"Error! {datasets=}"
        dataset_metadata = list()
        for d in datasets:
            dataset_metadata.append(GeoDNDiscovery._make_dataset(d))
        return dataset_metadata

    def get_dataset(self, dataset_id) -> DatasetMetadata:
        """make a request to GeoDN.Data to retrieve a list of all datasets

        Returns:
            Dict: metadata
        """
        url = f"/v2/datasets/{dataset_id}"
        dataset: Union[List[Dict[Any, Any]], Dict[str, Any]] = self.get(endpoint=url)
        assert isinstance(dataset, dict), f"Error! {dataset=}"
        return GeoDNDiscovery._make_dataset(dataset)
