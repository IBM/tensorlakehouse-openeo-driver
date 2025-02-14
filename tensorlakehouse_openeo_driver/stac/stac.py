from typing import Any, Dict, List, Optional, Tuple, Union

import planetary_computer
from tensorlakehouse_openeo_driver.model.item import Item, make_item
from tensorlakehouse_openeo_driver.stac import rest
from datetime import datetime, timedelta
from tensorlakehouse_openeo_driver.constants import (
    APPID_ISSUER,
    APPID_PASSWORD,
    APPID_USERNAME,
    OPENEO_AUTH_CLIENT_ID,
    OPENEO_AUTH_CLIENT_SECRET,
    SENTINEL_2_L2A,
    STAC_DATETIME_FORMAT,
    STAC_URL,
    logger,
)
import requests
from urllib3 import Retry

from pystac_client import Client
from pystac_client.stac_api_io import StacApiIO


def sign_request(request: requests.Request) -> requests.Request:
    client_id = OPENEO_AUTH_CLIENT_ID
    client_secret = OPENEO_AUTH_CLIENT_SECRET
    token_url = APPID_ISSUER
    payload = {
        "grant_type": "password",
        "client_id": client_id,
        "username": APPID_USERNAME,
        "password": APPID_PASSWORD,
        "client_secret": client_secret,
    }

    response = requests.post(token_url, data=payload)
    access_token = response.json().get("access_token")
    request.headers["Authorization"] = f"Bearer {access_token}"
    return request


def make_stac_client(url) -> Client:
    logger.debug(f"make_stac_client {url=}")
    if "osprey.hartree.stfc.ac.uk" in url:
        catalog = Client.open(url=url, request_modifier=sign_request)
    else:

        retry = Retry(
            total=10,
            backoff_factor=0.5,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=None,
        )
        stac_api_io = StacApiIO(max_retries=retry)
        catalog = Client.open(url, stac_io=stac_api_io, timeout=60)
    return catalog


class STAC:
    def __init__(self, url: str) -> None:
        assert isinstance(url, str)
        if url.endswith("/"):
            url = url[:-1]
        self._url = url
        self._client = None

    @property
    def headers(self):
        headers = {
            "Content-Type": "application/json",
        }
        return headers

    @property
    def client(self) -> Client:
        if self._client is not None:
            return self._client
        else:
            if "planetarycomputer.microsoft.com" in self._url:
                client = Client.open(
                    self._url,
                    modifier=planetary_computer.sign_inplace,
                )
            else:
                client = make_stac_client(url=self._url)
            return client

    def get_item_as_dict(self, collection_id: str, item_id: str) -> Dict[str, Any]:
        """make a request to get STAC item specified by collection and item ids

        Args:
            collection_id (str): collection ID
            item_id (str): item id

        Returns:
            Dict: item as dict
        """
        url = f"{self._url}/collections/{collection_id}/items/{item_id}"
        # endpoint = urllib.parse.quote(path)
        resp = rest.get(url=url, headers=self.headers)
        resp.raise_for_status()
        item = resp.json()
        assert isinstance(item, dict)
        return item

    def get_item(self, collection_id: str, item_id: str) -> Item:
        item_as_dict = self.get_item_as_dict(
            collection_id=collection_id, item_id=item_id
        )
        return make_item(item_dict=item_as_dict)

    def list_items(self, collection_id: str, limit: int = 10) -> Dict[str, Any]:
        """make a request to get STAC item specified by collection and item ids

        Args:
            collection_id (str): collection ID
            item_id (str): item id

        Returns:
            Dict: item as dict
        """
        url = f"{self._url}/collections/{collection_id}/items/"
        resp = rest.get(url=url, params={"limit": limit}, headers=self.headers)
        item = resp.json()
        assert isinstance(item, dict)
        return item

    def list_collections(self):
        resp = rest.get(url=f"{self._url}/collections", headers=self.headers)
        return resp.json()

    def is_collection_available(self, collection_id: str) -> bool:
        collections = self.list_collections()
        for c in collections["collections"]:
            if c["id"] == collection_id:
                return True
        return False

    def get_collection(self, collection_id: str) -> Dict:
        resp = rest.get(
            url=f"{self._url}/collections/{collection_id}", headers=self.headers
        )
        resp.raise_for_status()
        coll = resp.json()
        assert isinstance(coll, dict)
        return coll

    def update_collection(self, new_collection):
        rest.put(url=f"{self._url}/collections", payload=new_collection)

    @staticmethod
    def _from_datetime_to_str(
        temporal_extent: Tuple[datetime, datetime | None]
    ) -> List[Union[str, None]]:
        # check temporal_extent
        assert isinstance(
            temporal_extent, tuple
        ), f"Error! not a tuple {temporal_extent}"
        assert (
            len(temporal_extent) == 2
        ), f"Error! invalid number of items: {temporal_extent}"
        start = temporal_extent[0]
        start_str = start.strftime(STAC_DATETIME_FORMAT)
        end = temporal_extent[1]
        if end is not None:
            end_str = end.strftime(STAC_DATETIME_FORMAT)
        else:
            end_str = None
        dt = [start_str, end_str]
        return dt

    def search_items_as_dicts(
        self,
        collections: List[str],
        bbox: Tuple[float, float, float, float],
        temporal_extent: Tuple[datetime, datetime | None],
        fields: Optional[Dict] = None,
        limit: int = 1000,
        temporal_buffer: int = 0,
        filter_cql: Optional[Dict] = None,
        max_items: int | None = None,
    ) -> List[Dict[str, Any]]:
        assert len(collections) >= 1
        assert all(isinstance(cid, str) for cid in collections)
        assert isinstance(bbox, tuple)
        assert len(bbox) == 4
        assert all(isinstance(c, (float, int)) for c in bbox)

        temporal_extent = STAC._expand_temporal_extent(
            temporal_buffer=temporal_buffer, temporal_extent=temporal_extent
        )
        time_range = self._from_datetime_to_str(temporal_extent=temporal_extent)
        bbox_list = list(bbox)

        logger.info(f"Searching STAC: {collections} {bbox_list} {time_range} {fields}")

        if filter_cql is not None:
            filter_lang = "cql2-json"
        else:
            filter_lang = None
        result = self.client.search(
            collections=collections,
            bbox=bbox_list,
            limit=limit,
            datetime=time_range,  # type: ignore
            fields=fields,
            filter=filter_cql,
            filter_lang=filter_lang,
            max_items=max_items,
        )
        items_as_dicts = list(result.items_as_dicts())

        logger.info(f"Number of matched items: {len(items_as_dicts)} url={self._url}")

        return items_as_dicts

    def search_items(
        self,
        collections: List[str],
        bbox: Tuple[float, float, float, float],
        temporal_extent: Tuple[datetime, datetime | None],
        fields: Optional[Dict] = None,
        limit: int = 1000,
        temporal_buffer: int = 0,
        filter_cql: Optional[Dict] = None,
    ) -> List[Item]:
        items_as_dicts = self.search_items_as_dicts(
            collections=collections,
            bbox=bbox,
            temporal_extent=temporal_extent,
            fields=fields,
            limit=limit,
            temporal_buffer=temporal_buffer,
            filter_cql=filter_cql,
        )
        items = [make_item(i) for i in items_as_dicts]
        return items

    @staticmethod
    def _expand_temporal_extent(
        temporal_extent: Tuple[datetime, datetime | None], temporal_buffer: int
    ) -> Tuple[datetime, datetime | None]:
        """expand temporal extent by subtrating temporal_buffer from start and by adding
        temporal_buffer to end

        Args:
            temporal_extent (Tuple[datetime, datetime  |  None]): original temporal extent
            temporal_buffer (int): time delta in seconds

        Returns:
            Tuple[datetime, datetime | None]: modified temporal extent
        """
        delta = timedelta(seconds=temporal_buffer)
        start = temporal_extent[0] - delta
        end = temporal_extent[1]
        if end is not None:
            end = end + delta

        return (start, end)

    def delete_item(self, item_id: str, collection_id: str):
        url = f"{self._url}/collections/{collection_id}/items/{item_id}"
        rest.delete(url=url, headers=self.headers)
        logger.debug(f"Removed item: {item_id}")


def main():
    pass


if __name__ == "__main__":
    stac = STAC(url=STAC_URL)
    collection_id = SENTINEL_2_L2A
    item_id = "S2A_MSIL2A_20210930T165111_N0500_R026_T15SUT_20230115T184450"
    item = stac.get_item(collection_id=collection_id, item_id=item_id)
    print(item)
    # items = stac.list_items(collection_id=collection_id, limit=100)
    # for i in items["features"]:
    #     bbox = i["bbox"]
    #     item_id = i["id"]
    #     west, south, east, north = bbox
    #     if not (-180 <= west <= east <= 180 and -90 <= south <= north <= 90):
    #         print(item_id, bbox)
    #         stac.delete_item(item_id=item_id, collection_id=collection_id)
