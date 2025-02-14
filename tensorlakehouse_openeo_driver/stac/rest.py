from typing import Any, Dict, Optional
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from tensorlakehouse_openeo_driver.constants import logger


def get(
    url: str, headers: Dict, params: Optional[Dict[str, Any]] = None
) -> requests.Response:
    """this method makes all GET requests to the Discovery

    Args:
        endpoint (str): path (aka route) of the endpoint

    Returns:
        Union[List, Dict]: _description_
    """

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
                headers=headers,
                params=params,
                timeout=60,
                # verify=False,
            )
            assert resp.status_code in [
                200,
                201,
            ], f"Error! Invalid request - status_code={resp.status_code}\ntext={resp.text}\nurl={resp.url}\nheaders={headers}"

            return resp
        except requests.exceptions.RetryError as e:
            logger.error(e)
            raise e
        except AssertionError as e:
            logger.error(e)
            raise e


def post(
    url: str, headers: Dict, payload: Optional[Dict[str, Any]] = None
) -> requests.Response:
    """this method makes all GET requests to the Discovery

    Args:
        endpoint (str): path (aka route) of the endpoint

    Returns:
        Union[List, Dict]: _description_
    """

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
                headers=headers,
                json=payload,
                timeout=60,
                verify=False,
            )
            assert resp.status_code in [
                200,
                201,
            ], f"Error! Invalid request - status_code={resp.status_code}\ntext={resp.text}\nurl={resp.url}\nheaders={headers}"

            return resp
        except requests.exceptions.RetryError as e:
            logger.error(e)
            raise e
        except AssertionError as e:
            logger.error(e)
            raise e


def put(
    url: str, headers: Dict, payload: Optional[Dict[str, Any]] = None
) -> requests.Response:
    """this method makes all GET requests to the Discovery

    Args:
        endpoint (str): path (aka route) of the endpoint

    Returns:
        Union[List, Dict]: _description_
    """

    retry_strategy = Retry(
        total=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["PUT"],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    with requests.Session() as session:
        logger.debug(f"PUT {url} payload={payload} headers={headers}")
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        try:
            resp = session.put(
                url=url,
                headers=headers,
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


def delete(
    url: str, headers: Dict, payload: Optional[Dict[str, Any]] = None
) -> requests.Response:
    """this method makes all GET requests to the Discovery

    Args:
        endpoint (str): path (aka route) of the endpoint

    Returns:
        Union[List, Dict]: _description_
    """

    retry_strategy = Retry(
        total=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["PUT"],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    with requests.Session() as session:
        logger.debug(f"DELETE {url} payload={payload} headers={headers}")
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        try:
            resp = session.delete(
                url=url,
                headers=headers,
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
