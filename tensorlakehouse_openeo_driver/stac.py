from typing import Any, Dict, List, Optional

import urllib.parse
from tensorlakehouse_openeo_driver.constants import (
    APPID_ISSUER,
    APPID_PASSWORD,
    APPID_USERNAME,
    OPENEO_AUTH_CLIENT_ID,
    OPENEO_AUTH_CLIENT_SECRET,
    STAC_URL,
    logger,
)
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pystac_client import Client


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


def make_stac_client(url):
    if "osprey.hartree.stfc.ac.uk" in url:
        catalog = Client.open(url=STAC_URL, request_modifier=sign_request)
    else:
        catalog = Client.open(STAC_URL)
    return catalog


class STAC:
    def __init__(self, url: str) -> None:
        assert isinstance(url, str)
        if url.endswith("/"):
            url = url[:-1]
        self._url = url

    @property
    def headers(self):
        headers = {
            "Content-Type": "application/json",
        }
        return headers

    def _get(
        self, endpoint: str, params: Optional[Dict[str, Any]] = None
    ) -> requests.Response:
        """this method makes all GET requests to the Discovery

        Args:
            endpoint (str): path (aka route) of the endpoint

        Returns:
            Union[List, Dict]: _description_
        """

        url = f"{self._url}{endpoint}"
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
                    # verify=False,
                )
                assert resp.status_code in [
                    200,
                    201,
                ], f"Error! Invalid request - status_code={resp.status_code}\ntext={resp.text}\nurl={resp.url}\nheaders={self.headers}"

                return resp
            except requests.exceptions.RetryError as e:
                logger.error(e)
                raise e
            except AssertionError as e:
                logger.error(e)
                raise e

    def _post(
        self, endpoint: str, payload: Optional[Dict[str, Any]] = None
    ) -> requests.Response:
        """this method makes all GET requests to the Discovery

        Args:
            endpoint (str): path (aka route) of the endpoint

        Returns:
            Union[List, Dict]: _description_
        """

        url = f"{self._url}{endpoint}"
        retry_strategy = Retry(
            total=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["POST"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        with requests.Session() as session:
            logger.debug(f"POST {url} payload={payload}")
            session.mount("https://", adapter)
            session.mount("http://", adapter)
            try:
                resp = session.post(
                    url=url,
                    headers=self.headers,
                    json=payload,
                    timeout=60,
                    verify=False,
                )
                assert resp.status_code in [
                    200,
                    201,
                ], f"Error! Invalid request - status_code={resp.status_code}\ntext={resp.text}\nurl={resp.url}\nheaders={self.headers}"

                return resp
            except requests.exceptions.RetryError as e:
                logger.error(e)
                raise e
            except AssertionError as e:
                logger.error(e)
                raise e

    def _put(
        self, endpoint: str, payload: Optional[Dict[str, Any]] = None
    ) -> requests.Response:
        """this method makes all GET requests to the Discovery

        Args:
            endpoint (str): path (aka route) of the endpoint

        Returns:
            Union[List, Dict]: _description_
        """

        url = f"{self._url}{endpoint}"
        retry_strategy = Retry(
            total=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["PUT"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        with requests.Session() as session:
            logger.debug(f"PUT {url} payload={payload} headers={self.headers}")
            session.mount("https://", adapter)
            session.mount("http://", adapter)
            try:
                resp = session.put(
                    url=url,
                    headers=self.headers,
                    json=payload,
                    timeout=60,
                )
                resp.raise_for_status()

                return resp
            except requests.exceptions.RetryError as e:
                logger.error(e)
                raise e
            except AssertionError as e:
                logger.error(e)
                raise e

    def get_item(self, collection_id: str, item_id: str) -> Dict[str, Any]:
        """make a request to get STAC item specified by collection and item ids

        Args:
            collection_id (str): collection ID
            item_id (str): item id

        Returns:
            Dict: item as dict
        """
        path = f"/collections/{collection_id}/items/{item_id}"
        endpoint = urllib.parse.quote(path)
        resp = self._get(endpoint=endpoint)
        item = resp.json()
        assert isinstance(item, dict)
        return item

    def list_items(self, collection_id: str, limit: int = 10) -> Dict[str, Any]:
        """make a request to get STAC item specified by collection and item ids

        Args:
            collection_id (str): collection ID
            item_id (str): item id

        Returns:
            Dict: item as dict
        """
        path = f"/collections/{collection_id}/items/"
        endpoint = urllib.parse.quote(path)
        resp = self._get(endpoint=endpoint, params={"limit": limit})
        item = resp.json()
        assert isinstance(item, dict)
        return item

    def search(
        self, collections: List[str], bbox: List[str], datetime: str, limit: int = 10
    ) -> List[Dict]:
        payload = {
            "collections": collections,
            "bbox": bbox,
            "datetime": datetime,
            "limit": limit,
        }
        resp = self._post(endpoint="/search", payload=payload)
        items = resp.json()
        assert isinstance(items, list)
        return items

    def list_collections(self):
        resp = self._get(endpoint="/collections")
        return resp.json()

    def is_collection_available(self, collection_id: str) -> bool:
        collections = self.list_collections()
        for c in collections["collections"]:
            if c["id"] == collection_id:
                return True
        return False

    def get_collection(self, collection_id: str) -> Dict:
        resp = self._get(endpoint=f"/collections/{collection_id}")
        resp.raise_for_status()
        coll = resp.json()
        assert isinstance(coll, dict)
        return coll

    def update_collection(self, new_collection):
        self._put(endpoint="/collections", payload=new_collection)


def main():
    stac = STAC(url=STAC_URL)
    collection = stac.get_collection(collection_id="HLSS30")
    print(collection)
    if "license" in collection.keys():
        print("valid collection")
    else:
        collection["license"] = "Unknown"
        stac.update_collection(new_collection=collection)


if __name__ == "__main__":
    main()
