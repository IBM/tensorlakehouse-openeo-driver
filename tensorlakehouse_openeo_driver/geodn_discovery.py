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
from datetime import datetime, timedelta
from tensorlakehouse_openeo_driver.dataset import DatasetMetadata
import ibmpairs.authentication as authentication
import ibmpairs.client as client
import ibmpairs.catalog as catalog
from tensorlakehouse_openeo_driver.layer import LayerMetadata
from ibmpairs.catalog import DataSet, DataLayers, DataSets

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
        client_id: str = "ibm-pairs",
        password: Optional[str] = None,
        auth_url: str = "https://auth-b2b-twc.ibm.com/auth/GetBearerForClient",
        api_url: str = "https://pairs.res.ibm.com",
    ) -> None:
        """instantiate PAIRSConn class

        Parameters
        ----------
        access_token: str
        """
        assert client_id is not None, "Error! client_id cannot be None"
        self._client_id = client_id
        assert (
            api_key is not None or password is not None
        ), "Error! Both password and api_key are None"
        self._api_key = api_key
        self._auth_url = auth_url
        self._api_url = api_url
        self._access_token = None
        self._password = password
        credentials = authentication.Basic(username=client_id, password=password)
        self.pairs_client = client.Client(authentication=credentials)

        self._token_expiration = datetime.now()

    @property
    def auth(self) -> Optional[Tuple[str, str]]:
        if self.client_id is not None and self.password is not None:
            return (self.client_id, self.password)
        else:
            return None

    @property
    def api_key(self):
        return self._api_key

    @property
    def password(self):
        return self._password

    @property
    def client_id(self):
        return self._client_id

    @property
    def access_token(self):
        return self._access_token

    @property
    def headers(self):
        headers = {
            "Content-Type": "application/json",
        }
        if self.access_token is not None:
            headers["Authorization"] = "Bearer {}".format(self.access_token)
        return headers

    def _refresh_token(self):
        logger.debug("Refresh token")
        if self.api_key is not None:
            auth_response = requests.post(
                self._auth_url,
                headers={"content-type": "application/json"},
                json={"apiKey": self.api_key, "clientId": self.client_id},
            )
        else:
            auth_response = requests.post(
                self._auth_url,
                headers={"Content-type": "application/json"},
                auth=self.auth,
            )

        assert auth_response.status_code in [
            200,
            201,
        ], f"Error! status={auth_response.status_code} url={auth_response.url} text={auth_response.text}"
        auth_obj = auth_response.json()
        # logger.debug(auth_obj)
        self._access_token = auth_obj["access_token"]
        self._token_expiration = datetime.now() + timedelta(seconds=3600)

    def get(
        self, endpoint: str, params: Optional[Dict[str, str]] = None
    ) -> Union[List[Dict[Any, Any]], Dict[Any, Any]]:
        """this method makes all GET requests to the Discovery

        Args:
            endpoint (str): path (aka route) of the endpoint

        Returns:
            Union[List, Dict]: _description_
        """
        self._refresh_token()
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
        layers: DataLayers = catalog.get_data_layers(
            data_set_id=dataset_id, client=self.pairs_client
        )
        # endpoint = f"/v2/datasets/{dataset_id}/datalayers"
        # layers = self.get(endpoint=endpoint)
        metadata = list()
        for layer in layers.data_layers:
            layer_full = catalog.get_data_layer(id=layer.id, client=self.pairs_client)
            metadata.append(
                LayerMetadata(
                    layer_id=str(layer_full.id),
                    description_short=layer_full.description_short,
                    dataset_id=layer_full.dataset_id,
                    name=layer_full.name,
                    unit=layer_full.unit,
                    data_type=layer_full.type,
                    level=layer_full.level,
                )
            )
        return metadata

    @staticmethod
    def _make_dataset(d: DataSet) -> DatasetMetadata:
        return DatasetMetadata(
            dataset_id=d.id,
            latitude_min=d.latitude_min,
            latitude_max=d.latitude_max,
            longitude_max=d.longitude_max,
            longitude_min=d.longitude_min,
            temporal_min=d.temporal_min,
            temporal_max=d.temporal_max,
            name=d.name,
            level=d.level,
            temporal_resolution_description=d.temporal_resolution_description,
            spatial_resolution_of_raw_data=d.spatial_resolution_of_raw_data,
            temporal_resolution=d.temporal_resolution,
            description_short=d.description_short,
            license=d.license_information,
            # crs=d.get("crs"),
        )

    def list_datasets(self) -> List[DatasetMetadata]:
        """make a request to GeoDN.Data to retrieve a list of all datasets

        Returns:
            List: _description_
        """
        datasets: DataSets = catalog.get_data_sets(client=self.pairs_client, verify=True)

        dataset_metadata = list()
        for d in datasets.get_data_sets():
            dataset_metadata.append(GeoDNDiscovery._make_dataset(d))
        return dataset_metadata

    def get_dataset(self, dataset_id) -> DatasetMetadata:
        """make a request to GeoDN.Data to retrieve a list of all datasets

        Returns:
            Dict: metadata
        """
        dataset = catalog.get_data_set(id=dataset_id, client=self.pairs_client)
        return GeoDNDiscovery._make_dataset(dataset)

    # def list_datalayers(self) -> List[Dict[Any, Any]]:
    #     """make a request to GeoDN.Data to retrieve a list of all datasets

    #     Returns:
    #         List: _description_
    #     """
    #     endpoint = "/v2/datalayers/full"
    #     datalayers = self.get(endpoint=endpoint)
    #     assert isinstance(datalayers, list), f"Error! Unexpected response: {datalayers}"
    #     return datalayers
