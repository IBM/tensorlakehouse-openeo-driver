import openeo
import requests

from tensorlakehouse_openeo_driver.constants import (
    APPID_ISSUER,
    APPID_PASSWORD,
    APPID_USERNAME,
    OPENEO_AUTH_CLIENT_ID,
    OPENEO_AUTH_CLIENT_SECRET,
    OPENEO_URL,
)

assert APPID_PASSWORD is not None
assert APPID_USERNAME is not None
assert OPENEO_AUTH_CLIENT_ID is not None
assert OPENEO_AUTH_CLIENT_SECRET is not None


def test_authenticate_oidc_resource_owner_password_credentials():
    connection = openeo.connect(OPENEO_URL)

    connection.authenticate_oidc_resource_owner_password_credentials(
        username=APPID_USERNAME,
        password=APPID_PASSWORD,
        client_id=OPENEO_AUTH_CLIENT_ID,
        client_secret=OPENEO_AUTH_CLIENT_SECRET,
        provider_id="app_id",
        store_refresh_token=False,
    )
    collections = connection.list_collections()
    assert collections is not None
    for c in collections:
        assert c is not None
        assert isinstance(c, dict)
        assert c["id"] is not None


def test_authenticate_oidc_resource_owner_password_credentials_refresh_token_true():
    connection = openeo.connect(OPENEO_URL)

    connection.authenticate_oidc_resource_owner_password_credentials(
        username=APPID_USERNAME,
        password=APPID_PASSWORD,
        client_id=OPENEO_AUTH_CLIENT_ID,
        client_secret=OPENEO_AUTH_CLIENT_SECRET,
        provider_id="app_id",
        store_refresh_token=True,
    )
    collections = connection.list_collections()
    assert collections is not None
    for c in collections:
        assert c is not None
        assert isinstance(c, dict)
        assert c["id"] is not None


def test_authenticate_oidc_client_credentials():
    connection = openeo.connect(OPENEO_URL)

    connection.authenticate_oidc_client_credentials(
        client_id=OPENEO_AUTH_CLIENT_ID,
        client_secret=OPENEO_AUTH_CLIENT_SECRET,
        provider_id="app_id",
    )
    collections = connection.list_collections()
    assert collections is not None
    for c in collections:
        assert c is not None
        assert isinstance(c, dict)
        assert c["id"] is not None


def test_capabilities():
    capabilities_url = f"{OPENEO_URL}openeo/1.2/"
    resp = requests.get(capabilities_url, headers={"Content-type": "application/json"})
    resp.raise_for_status()
    capabilities = resp.json()
    assert capabilities is not None
    assert isinstance(capabilities, dict)
    for k in ["title", "stac_extensions", "api_version", "description"]:
        assert capabilities[k] is not None


def test_credentials_oidc():
    url = f"{OPENEO_URL}openeo/1.2/credentials/oidc"
    resp = requests.get(url=url, headers={"Content-type": "application/json"})
    resp.raise_for_status()
    response = resp.json()
    assert response is not None
    providers = response["providers"]
    assert isinstance(providers, list)
    assert len(providers) > 0
    found = False
    for p in providers:
        if p.get("id") == "app_id":
            assert p["issuer"] == APPID_ISSUER
            found = True
    assert found


def test_health():
    url = f"{OPENEO_URL}openeo/1.2/health"
    resp = requests.get(url=url, headers={"Content-type": "application/json"})
    assert resp.status_code == 200
    health = resp.json()
    assert isinstance(health, dict)
    assert "health" in health.keys()


def test_post_result():
    process_graph = (
        {
            "process": {
                "process_graph": {
                    "loadcollection1": {
                        "process_id": "load_collection",
                        "arguments": {
                            "bands": ["B02"],
                            "id": "HLSS30",
                            "spatial_extent": {
                                "west": -121.5,
                                "south": 44.0,
                                "east": -121.25,
                                "north": 44.25,
                            },
                            "temporal_extent": [
                                "2022-01-02T00:00:00Z",
                                "2022-01-02T23:59:59Z",
                            ],
                        },
                    },
                    "saveresult1": {
                        "process_id": "save_result",
                        "arguments": {
                            "data": {"from_node": "loadcollection1"},
                            "format": "netCDF",
                            "options": {},
                        },
                        "result": True,
                    },
                }
            }
        },
    )
    url = f"{OPENEO_URL}openeo/1.2/result"
    resp = requests.post(url=url, headers={"Content-type": "application/json"}, json=process_graph)
    assert resp.status_code == 401, f"Error! {resp.status_code=}"
