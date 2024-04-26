import requests

from tensorlakehouse_openeo_driver.constants import (
    OPENEO_AUTH_CLIENT_ID,
    OPENEO_AUTH_CLIENT_SECRET,
    APPID_PASSWORD,
    APPID_USERNAME,
)


def make_request(token: str, token_type: str):
    # url = "http://0.0.0.0:8080/"
    url = "https://openeo-geodn-driver-pgstac-nasageospatial-dev.cash.sl.cloud9.ibm.com/openeo/1.2/health"
    headers = {"Authorization": f"{token_type} {token}", "Accept": "application/json"}
    print(f"GET {url} {headers=}")
    resp = requests.get(url=url, headers=headers)
    resp.raise_for_status()
    return resp.json()


def get_token(username, password, token_url, client_id, client_secret):
    headers = {"Accept": "application/json"}
    payload = {
        "grant_type": "password",
        "username": username,
        "password": password,
    }
    auth = (client_id, client_secret)
    resp = requests.post(token_url, headers=headers, auth=auth, json=payload)
    resp.raise_for_status()
    return resp.json()


def main():

    username = APPID_USERNAME
    password = APPID_PASSWORD

    token_url = (
        "https://us-south.appid.cloud.ibm.com/oauth/v4/11eff848-ef33-4900-ac26-bdcc7639b5b0/token"
    )
    client_id = OPENEO_AUTH_CLIENT_ID
    client_secret = OPENEO_AUTH_CLIENT_SECRET
    token_resp = get_token(
        username=username,
        password=password,
        token_url=token_url,
        client_id=client_id,
        client_secret=client_secret,
    )
    print(token_resp)
    token_type = token_resp["token_type"]
    token = token_resp["access_token"]
    # token = token_resp["refresh_token"]
    # token = token_resp["id_token"]
    app_resp = make_request(token=token, token_type=token_type)
    print(app_resp)


if __name__ == "__main__":
    main()
