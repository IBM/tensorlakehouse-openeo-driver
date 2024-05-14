from tensorlakehouse_openeo_driver.geodn_backend import GeoDNBackendImplementation
from tensorlakehouse_openeo_driver.process_implementations.load_collection import (
    LoadCollectionFromCOS,
)
from tensorlakehouse_openeo_driver.tests.unit.unit_test_util import get_collection_items
import uuid
import json
import logging
from contextlib import contextmanager, ExitStack
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
from unittest import mock
import boto3
import flask
from moto import mock_s3
from pystac_client.client import Client
import pytest
import werkzeug.exceptions
from openeo.capabilities import ComparableVersion
from openeo_driver.ProcessGraphDeserializer import custom_process_from_process_graph
from openeo_driver.backend import (
    BatchJobMetadata,
    UserDefinedProcessMetadata,
    BatchJobs,
    OpenEoBackendImplementation,
    Processing,
    not_implemented,
)
from unittest.mock import patch
from openeo_driver.constants import STAC_EXTENSION
from openeo_driver.dummy import dummy_backend
from tensorlakehouse_openeo_driver.catalog import TensorLakehouseCollectionCatalog
from tensorlakehouse_openeo_driver.tests.unit.unit_tests_data import (
    FEATURE_COLLECTION_JSON,
)
from openeo_driver.dummy.dummy_backend import DummyBackendImplementation
from openeo_driver.testing import (
    ApiTester,
    TEST_USER,
    ApiResponse,
    TEST_USER_AUTH_HEADER,
    generate_unique_test_process_id,
    build_basic_http_auth_header,
    DictSubSet,
    RegexMatcher,
)
from openeo_driver.users.auth import HttpAuthHandler
from openeo_driver.util.logging import LOGGING_CONTEXT_FLASK
from openeo_driver.views import (
    EndpointRegistry,
    _normalize_collection_metadata,
    build_app,
    STREAM_CHUNK_SIZE_DEFAULT,
)
from tensorlakehouse_openeo_driver.tests.unit.unit_test_util import (
    make_pystac_client_collection,
    generate_xarray_datarray,
    validate_downloaded_file,
    validate_OpenEO_collection,
    validate_STAC_Collection,
    MockPystacClient,
)
from tensorlakehouse_openeo_driver.tests.conftest import (
    TEST_APP_CONFIG,
    enhanced_logging,
)
from tensorlakehouse_openeo_driver.constants import (
    GEODN_DISCOVERY_CRS,
    TEST_DATA_ROOT,
    DEFAULT_X_DIMENSION,
    DEFAULT_Y_DIMENSION,
    DEFAULT_TIME_DIMENSION,
)


DAILY_PRISM = {
    "collection": "Daily US weather (PRISM)",
    "bands": ["Daily maximum temperature", "Daily minimum temperature"],
}

CROPSCAPE = {
    "collection": "Historical crop planting map (USA)",
    "bands": ["111"],
}


@pytest.fixture(params=["1.0.0", "1.1.0"])
def api_version(request):
    return request.param


@pytest.fixture
def api(api_version, client) -> ApiTester:
    return ApiTester(api_version=api_version, client=client, data_root=TEST_DATA_ROOT)


@pytest.fixture
def api040(client) -> ApiTester:
    return ApiTester(api_version="0.4.0", client=client, data_root=TEST_DATA_ROOT)


@pytest.fixture
def api100(client) -> ApiTester:
    return ApiTester(api_version="1.0.0", client=client, data_root=TEST_DATA_ROOT)


@pytest.fixture
def api110(client) -> ApiTester:
    return ApiTester(api_version="1.1.0", client=client, data_root=TEST_DATA_ROOT)


def api_from_backend_implementation(
    backend_implementation: OpenEoBackendImplementation,
    api_version="1.0.0",
    data_root=TEST_DATA_ROOT,
) -> ApiTester:
    app: flask.Flask = build_app(backend_implementation)
    app.config.from_mapping(TEST_APP_CONFIG)
    client = app.test_client()
    api = ApiTester(api_version=api_version, client=client, data_root=data_root)
    return api


def mock_uuid4(value: str = "abc123"):
    class UUIDMock:
        def __str__(self):
            return value

        @property
        def hex(self):
            return value

    return mock.patch("uuid.uuid4", new=UUIDMock)


TEST_AWS_REGION_NAME = "eu-central-1"


@pytest.fixture(scope="function")
def aws_credentials(monkeypatch):
    """Mocked AWS Credentials and related environment variables for moto/boto3."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", TEST_AWS_REGION_NAME)
    monkeypatch.setenv("AWS_REGION", TEST_AWS_REGION_NAME)
    monkeypatch.setenv("SWIFT_BUCKET", "openeo-test-bucket")


@pytest.fixture(scope="function")
def mock_s3_resource(aws_credentials):
    with mock_s3():
        yield boto3.resource("s3", region_name=TEST_AWS_REGION_NAME)


def create_s3_bucket(s3_resource, bucket_name):
    bucket = s3_resource.Bucket(bucket_name)
    bucket.create(
        CreateBucketConfiguration={"LocationConstraint": TEST_AWS_REGION_NAME}
    )
    return bucket


class TestGeneral:
    """
    General tests (capabilities, collections, processes)
    """

    def test_well_known_openeo(self, client):
        resp = client.get("/.well-known/openeo")
        assert resp.status_code == 200
        versions = resp.json["versions"]
        # in .well-known/openeo there should only be one item per api_version (no aliases)
        by_api_version = {d["api_version"]: d for d in versions}
        assert len(versions) == len(by_api_version)
        assert by_api_version == {
            "1.0.0": {
                "api_version": "1.0.0",
                "production": True,
                "url": "http://oeo.net/openeo/1.0/",
            },
            "1.1.0": {
                "api_version": "1.1.0",
                "production": True,
                "url": "http://oeo.net/openeo/1.1/",
            },
            "1.2.0": {
                "api_version": "1.2.0",
                "production": True,
                "url": "http://oeo.net/openeo/1.2/",
            },
        }
        assert resp.headers["Cache-Control"] == "max-age=900, public"

    def test_versioned_well_known_openeo(self, api):
        api.get("/.well-known/openeo").assert_error(404, "NotFound")

    @pytest.mark.parametrize(
        ["headers", "expected"],
        [
            ({}, "http://oeo.net/"),
            ({"X-Forwarded-Proto": "https"}, "https://oeo.net/"),
        ],
    )
    def test_https_proxy_handling(self, client, headers, expected):
        resp = client.get("/.well-known/openeo", headers=headers)
        for url in [v["url"] for v in resp.json["versions"]]:
            assert url.startswith(expected)

    @pytest.mark.parametrize(
        ["url", "expected_version"],
        [
            ("/openeo/1.0/", "1.0.0"),
            ("/openeo/1.0.0/", "1.0.0"),
            ("/openeo/1.1/", "1.1.0"),
            ("/openeo/1.1.0/", "1.1.0"),
            ("/openeo/1.2/", "1.2.0"),
            ("/openeo/1/", "1.2.0"),
            ("/openeo/", "1.2.0"),
        ],
    )
    def test_versioned_urls(self, client, url, expected_version):
        resp = client.get(url)
        assert resp.status_code == 200
        capabilities = resp.json
        assert capabilities["title"] == "Untitled openEO Backend"
        assert capabilities["api_version"] == expected_version

    @pytest.mark.skip("v0.4.0 is no longer supported")
    def test_capabilities_040(self, api040):
        capabilities = api040.get("/").assert_status_code(200).json
        assert capabilities["api_version"] == "0.4.0"
        assert capabilities["version"] == "0.4.0"
        assert capabilities["stac_version"] == "0.9.0"
        assert capabilities["title"] == "openEO Unit Test Dummy Backend"
        assert capabilities["id"] == "openeounittestdummybackend-0.4.0"
        assert capabilities["production"] is True

    def test_capabilities_100(self, api100):
        capabilities = api100.get("/").assert_status_code(200).json
        assert capabilities["api_version"] == "1.0.0"
        assert capabilities["stac_version"] == "0.9.0"
        assert capabilities["title"] == "Untitled openEO Backend"
        assert (
            capabilities["description"]
            == "This is a generic openEO Backend, powered by [openeo-python-driver](https://github.com/Open-EO/openeo-python-driver)."
        )
        assert capabilities["backend_version"] == "0.96.0a1"
        assert capabilities["id"] == "untitledopeneobackend-0.96.0a1"
        assert capabilities["production"] is True

        def get_link(rel):
            links = [link for link in capabilities["links"] if link["rel"] == rel]
            assert len(links) == 1
            return links[0]

        assert (
            get_link("version-history")["href"] == "http://oeo.net/.well-known/openeo"
        )
        assert get_link("data")["href"] == "http://oeo.net/openeo/1.0.0/collections"
        assert (
            get_link("conformance")["href"] == "http://oeo.net/openeo/1.0.0/conformance"
        )

    @pytest.mark.skip("v0.4.2 is no longer supported")
    def test_capabilities_version_alias(self, client):
        resp = ApiResponse(client.get("/openeo/0.4/")).assert_status_code(200).json
        assert resp["api_version"] == "0.4.2"
        assert resp["production"] is True

    def test_capabilities_invalid_api_version(self, client):
        resp = ApiResponse(client.get("/openeo/0.0.0/"))
        resp.assert_error(501, "UnsupportedApiVersion")
        assert "Unsupported version component in URL: '0.0.0'" in resp.json["message"]

    def test_capabilities_endpoints(self, api100):
        capabilities = api100.get("/").assert_status_code(200).json
        endpoints = {e["path"]: sorted(e["methods"]) for e in capabilities["endpoints"]}
        assert endpoints["/collections"] == ["GET"]
        assert endpoints["/collections/{collection_id}"] == ["GET"]
        assert endpoints["/result"] == ["POST"]
        assert endpoints["/jobs"] == ["GET", "POST"]
        assert endpoints["/processes"] == ["GET"]
        assert endpoints["/udf_runtimes"] == ["GET"]
        assert endpoints["/file_formats"] == ["GET"]
        assert endpoints["/service_types"] == ["GET"]
        assert endpoints["/services"] == ["GET", "POST"]
        assert endpoints["/services/{service_id}"] == ["DELETE", "GET"]
        # assert endpoints["/subscription"] == ["GET"]
        assert endpoints["/jobs/{job_id}"] == ["DELETE", "GET"]
        assert endpoints["/jobs/{job_id}/results"] == ["DELETE", "GET", "POST"]
        assert endpoints["/credentials/basic"] == ["GET"]
        assert endpoints["/credentials/oidc"] == ["GET"]
        assert endpoints["/me"] == ["GET"]
        assert endpoints["/validation"] == ["POST"]

    def test_capabilities_caching(self, api):
        headers = api.get("/").assert_status_code(200).headers
        assert headers["Cache-Control"] == "max-age=900, public"
        assert "Expires" in headers

    def test_capabilities_endpoints_hiding(self):
        class MyProcessing(Processing):
            @not_implemented
            def validate(self, *args, **kwargs): ...  # noqa: E704

        backend_implementation = DummyBackendImplementation(processing=MyProcessing())
        api = api_from_backend_implementation(backend_implementation)
        capabilities = api.get("/").assert_status_code(200).json
        endpoints = {e["path"]: sorted(e["methods"]) for e in capabilities["endpoints"]}
        assert "/validation" not in endpoints

    @pytest.mark.skip("v0.4.0 is no longer supported")
    def test_capabilities_endpoints_issue_28_v040(self, api040):
        """https://github.com/Open-EO/openeo-python-driver/issues/28"""
        capabilities = api040.get("/").assert_status_code(200).json
        endpoints = {e["path"]: e["methods"] for e in capabilities["endpoints"]}
        assert endpoints["/output_formats"] == ["GET"]
        assert "/file_formats" not in endpoints

    @pytest.mark.skip("Warning! /credentials/basic should be removed")
    def test_capabilities_endpoints_issue_28_v100(self, api100):
        """https://github.com/Open-EO/openeo-python-driver/issues/28"""
        capabilities = api100.get("/").assert_status_code(200).json
        endpoints = {e["path"]: e["methods"] for e in capabilities["endpoints"]}
        assert endpoints["/file_formats"] == ["GET"]
        assert "/output_formats" not in endpoints

        backend_implementation = GeoDNBackendImplementation()
        api100 = api_from_backend_implementation(backend_implementation)
        capabilities = api100.get("/").assert_status_code(200).json
        endpoints = {e["path"] for e in capabilities["endpoints"]}
        assert "/credentials/basic" in endpoints
        api100.get("/credentials/basic").assert_error(401, "AuthenticationRequired")

        backend_implementation.enable_basic_auth = False
        api100 = api_from_backend_implementation(backend_implementation)
        capabilities = api100.get("/").assert_status_code(200).json
        endpoints = {e["path"] for e in capabilities["endpoints"]}
        assert "/credentials/basic" not in endpoints
        api100.get("/credentials/basic").assert_error(404, "NotFound")

    @pytest.mark.skip(
        "Warning! /credentials/basic should be removed - backend_implementation.enable_oidc_auth is not working"
    )
    def test_capabilities_no_oidc_auth(self):
        backend_implementation = GeoDNBackendImplementation()
        api100 = api_from_backend_implementation(backend_implementation)
        capabilities = api100.get("/").assert_status_code(200).json
        endpoints = {e["path"] for e in capabilities["endpoints"]}
        assert "/credentials/oidc" in endpoints
        api100.get("/credentials/oidc").assert_status_code(200)

        backend_implementation.enable_oidc_auth = False
        api100 = api_from_backend_implementation(backend_implementation)
        capabilities = api100.get("/").assert_status_code(200).json
        endpoints = {e["path"] for e in capabilities["endpoints"]}
        assert "/credentials/oidc" not in endpoints
        api100.get("/credentials/oidc").assert_error(404, "NotFound")

    @pytest.mark.skip("not implemented")
    def test_capabilities_processing_software(self, api100):
        capabilities = api100.get("/").assert_status_code(200).json
        assert capabilities["processing:software"] == {
            "openeo": RegexMatcher(r"0.\d+\.\d+"),
            "openeo_driver": RegexMatcher(r"0.\d+\.\d+"),
        }
        assert any(
            "stac-extensions.github.io/processing" in e
            for e in capabilities.get("stac_extensions", [])
        )

    def test_conformance(self, api100):
        res = api100.get("/conformance").assert_status_code(200).json
        assert "conformsTo" in res

    @pytest.mark.parametrize(
        "path",
        [
            "/",
            "/collections",
            "/processes",
            "/jobs",
        ],
    )
    def test_cors_headers_options(self, api, path):
        # Mimic browser's OPTION request
        api.client.environ_base["HTTP_ORIGIN"] = "https://editor.openeo.org"
        api.client.environ_base["HTTP_ACCESS_CONTROL_REQUEST_METHOD"] = "GET"
        api.client.environ_base["HTTP_ACCESS_CONTROL_REQUEST_HEADERS"] = (
            "Authorization, Content-Type"
        )
        resp = api.client.options(api.url(path))

        assert resp.status_code == 204
        assert len(resp.data) == 0
        # General CORS headers
        assert resp.access_control_allow_origin == "*"
        assert resp.access_control_allow_credentials is False
        assert {"Location", "OpenEO-Identifier", "OpenEO-Costs", "Link"}.issubset(
            resp.access_control_expose_headers
        )

        # OPTIONS specific CORS headers
        assert {"Authorization", "Content-Type"}.issubset(
            resp.access_control_allow_headers
        )
        assert {"GET", "POST", "PATCH"}.issubset(resp.access_control_allow_methods)
        assert resp.content_type == "application/json"

    @pytest.mark.skip("not implemented")
    @pytest.mark.parametrize("path", ["/", "/collections", "/processes"])
    def test_cors_headers_get(self, api, path):
        response = api.get(path)
        resp = response.assert_status_code(200).response
        # General CORS headers
        assert resp.access_control_allow_origin == "*"
        assert resp.access_control_allow_credentials is False
        assert {"Location", "OpenEO-Identifier", "OpenEO-Costs", "Link"}.issubset(
            resp.access_control_expose_headers
        )

    def test_cors_headers_post(self, api):
        pg = {
            "process_graph": {
                "add": {
                    "process_id": "add",
                    "arguments": {"x": 1, "y": 2},
                    "result": True,
                }
            }
        }
        response = api.post(
            "/jobs",
            headers=TEST_USER_AUTH_HEADER,
            json={"process": pg} if api.api_version_compare.at_least("1.0.0") else pg,
        )
        resp = response.assert_status_code(201).response
        # General CORS headers
        assert resp.access_control_allow_origin == "*"
        assert resp.access_control_allow_credentials is False
        assert {"Location", "OpenEO-Identifier", "OpenEO-Costs", "Link"}.issubset(
            resp.access_control_expose_headers
        )

    def test_cors_headers_put(self, api100):
        response = api100.put(
            "/process_graphs/myndvi",
            headers=TEST_USER_AUTH_HEADER,
            json={
                "id": "oneplustwo",
                "process_graph": {
                    "a": {
                        "process_id": "add",
                        "arguments": {"x": 1, "y": 2},
                        "result": True,
                    }
                },
            },
        )
        resp = response.assert_status_code(200).response
        # General CORS headers
        assert resp.access_control_allow_origin == "*"
        assert resp.access_control_allow_credentials is False
        assert {"Location", "OpenEO-Identifier", "OpenEO-Costs", "Link"}.issubset(
            resp.access_control_expose_headers
        )

    @pytest.mark.skip("not implemented yet")
    def test_error_handling_generic(self, api, caplog):
        caplog.set_level(logging.WARNING)
        with mock_uuid4(value="t3st"):
            resp = api.get("/_debug/error")
        assert (resp.status_code, resp.json) == (
            500,
            {
                "code": "Internal",
                "message": "Server error: Exception('Computer says no.')",
                "id": "r-t3st",
            },
        )
        assert caplog.record_tuples == [
            (
                "openeo_driver.views.error",
                logging.ERROR,
                "Exception('Computer says no.')",
            ),
        ]

    @pytest.mark.skip("not implemented yet")
    def test_error_handling_not_implemented_error(self, api, caplog):
        caplog.set_level(logging.WARNING)
        with mock_uuid4(value="t3st"):
            resp = api.get("/_debug/error/basic/NotImplementedError")
        assert (resp.status_code, resp.json) == (
            500,
            {
                "code": "Internal",
                "message": "Server error: NotImplementedError()",
                "id": "r-t3st",
            },
        )
        assert caplog.record_tuples == [
            (
                "openeo_driver.views.error",
                logging.ERROR,
                "NotImplementedError()",
            )
        ]

    @pytest.mark.skip("not implemented yet")
    def test_error_handling_error_summary(self, api, caplog):
        caplog.set_level(logging.WARNING)
        with mock_uuid4(value="t3st"):
            resp = api.get("/_debug/error/basic/ErrorSummary")
        assert (resp.status_code, resp.json) == (
            500,
            {
                "code": "Internal",
                "message": "Server error: No negatives please.",
                "id": "r-t3st",
            },
        )
        assert caplog.record_tuples == [
            (
                "openeo_driver.views.error",
                logging.ERROR,
                "ValueError(-123)",
            )
        ]

    @pytest.mark.skip("not implemented yet")
    @pytest.mark.parametrize(
        ["url", "error_status", "error_code", "error_message"],
        [
            ("/_debug/error/api", 500, "Internal", "Computer says no."),
            (
                "/_debug/error/api/404/CollectionNotFound",
                404,
                "CollectionNotFound",
                "Computer says no.",
            ),
            (
                "/_debug/error/http",
                500,
                "Internal",
                "500 Internal Server Error: Computer says no.",
            ),
            (
                "/_debug/error/http/404",
                404,
                "NotFound",
                "404 Not Found: Computer says no.",
            ),
            (
                "/_debug/error/http/501",
                501,
                "Internal",
                "501 Not Implemented: Computer says no.",
            ),
            (
                "/invalid/url",
                404,
                "NotFound",
                f"404 Not Found: {werkzeug.exceptions.NotFound.description}",
            ),
        ],
    )
    def test_error_handling_api_error(
        self, api, caplog, url, error_status, error_code, error_message
    ):
        caplog.set_level(logging.WARNING)
        with mock_uuid4(value="t3st"):
            resp = api.get(url)
        assert resp.status_code == error_status
        assert resp.json == {
            "code": error_code,
            "message": error_message,
            "id": "r-t3st",
        }
        assert caplog.record_tuples == [
            (
                "openeo_driver.views.error",
                logging.ERROR,
                f"OpenEOApiException(status_code={error_status}, code={error_code!r}, message={error_message!r}, id='r-t3st')",
            ),
        ]

    def test_health_basic(self, api):
        resp = api.get("/health").assert_status_code(200).json
        assert resp == {"health": "OK"}

    def test_health_dict(self, api, backend_implementation):
        with mock.patch.object(backend_implementation, "health_check") as health_check:
            health_check.return_value = {"status": "OK", "color": "green"}
            resp = api.get("/health").assert_status_code(200).json
            assert resp == {"status": "OK", "color": "green"}

    def test_health_flask_response(self, api, flask_app, backend_implementation):
        with mock.patch.object(backend_implementation, "health_check") as health_check:
            health_check.return_value = flask_app.make_response(
                ('{"code": "meh"}', 500, {"Content-type": "application/json"})
            )
            api.get("/health").assert_error(500, "meh")

    def test_health_dynamic(self, api, backend_implementation):
        def health(options: Optional[dict] = None):
            return {
                "status": "OK",
                "color": (options or {}).get("color", "green"),
            }

        with mock.patch.object(backend_implementation, "health_check", new=health):
            resp = api.get("/health").assert_status_code(200).json
            assert resp == {"status": "OK", "color": "green"}
            resp = api.get("/health?color=blue").assert_status_code(200).json
            assert resp == {"status": "OK", "color": "blue"}
            resp = (
                api.get("/health?shape=square&color=red").assert_status_code(200).json
            )
            assert resp == {"status": "OK", "color": "red"}

    @pytest.mark.skip("v0.4.0 is no longer supported")
    def test_credentials_oidc_040(self, api040):
        resp = api040.get("/credentials/oidc").assert_status_code(303)
        assert (
            resp.headers["Location"]
            == "https://oidc.test/.well-known/openid-configuration"
        )

    def test_credentials_oidc_100(self, api100):
        resp = api100.get("/credentials/oidc").assert_status_code(200).json
        """id="app_id",
                issuer=APPID_ISSUER,
                scopes=["openid", "users"],
                title="geolab-backend",
        """
        assert resp.get("providers")[0].get("id") == "app_id"

    @pytest.mark.skip("not implemented")
    def test_output_formats(self, api040):
        resp = api040.get("/output_formats").assert_status_code(200).json
        assert resp == {
            "GTiff": {
                "title": "GeoTiff",
                "gis_data_types": ["raster"],
                "parameters": {},
            },
        }

    def test_file_formats(self, api100):
        response = api100.get("/file_formats")
        resp = response.assert_status_code(200).json
        assert resp == {
            "input": DictSubSet(
                {
                    "GeoJSON": {"gis_data_types": ["vector"], "parameters": {}},
                }
            ),
            "output": DictSubSet(
                {
                    "GTiff": {
                        "title": "GeoTiff",
                        "gis_data_types": ["raster"],
                        "parameters": {},
                    },
                }
            ),
        }
        assert response.headers["Cache-Control"] == "max-age=900, public"

    @pytest.mark.parametrize("endpoint", ["/processes"])  # , "/processes/backend"
    def test_processes(self, api, endpoint):
        resp = api.get(endpoint).assert_status_code(200).json
        processes = resp["processes"]
        process_ids = set(p["id"] for p in processes)
        assert {
            "apply",
            "aggregate_spatial",
            "multiply",
            "add",
            "subtract",
            "divide",
            "max",
            "min",
            "mean",
            "median",
            "dimension_labels",
            "drop_dimension",
            "create_raster_cube",
            "reduce_dimension",
            "reduce_spatial",
            "load_collection",
            "save_result",
        }.issubset(process_ids)
        expected_keys = {"id", "description", "parameters", "returns"}
        for process in processes:
            assert all(k in process for k in expected_keys)

    @pytest.mark.skip("not implemented")
    def test_process_details(self, api100):
        spec = api100.get("/processes/backend/add").assert_status_code(200).json
        assert spec["id"] == "add"
        assert spec["summary"].lower() == "addition of two numbers"
        assert "x + y" in spec["description"]
        assert set(p["name"] for p in spec["parameters"]) == {"x", "y"}
        assert "computed sum" in spec["returns"]["description"]
        assert "process_graph" in spec

    @pytest.mark.skip("not implemented")
    def test_processes_non_standard_atmospheric_correction(self, api):
        if api.api_version_compare.below("1.0.0"):
            pytest.skip()
        resp = api.get("/processes").assert_status_code(200).json
        (spec,) = [p for p in resp["processes"] if p["id"] == "atmospheric_correction"]
        assert spec["summary"] == "Apply atmospheric correction"
        assert spec["categories"] == ["cubes", "optical"]
        assert spec["experimental"] is True
        assert spec["links"][0]["rel"] == "about"
        assert "DigitalElevationModelInvalid" in spec["exceptions"]

    @pytest.mark.skip("not implemented")
    def test_processes_040_vs_100(self, api040, api100):
        pids040 = {
            p["id"]
            for p in api040.get("/processes").assert_status_code(200).json["processes"]
        }
        pids100 = {
            p["id"]
            for p in api100.get("/processes").assert_status_code(200).json["processes"]
        }
        expected_only_040 = {"aggregate_polygon"}
        expected_only_100 = {
            "reduce_dimension",
            "aggregate_spatial",
            "mask_polygon",
            "add",
        }
        for pid in expected_only_040:
            assert pid in pids040
            assert pid not in pids100
        for pid in expected_only_100:
            assert pid not in pids040
            assert pid in pids100

    @pytest.mark.skip("not implemented")
    def test_custom_process_listing(self, api100):
        process_id = generate_unique_test_process_id()

        # Register a custom process with process graph
        process_spec = api100.load_json("pg/1.0/add_and_multiply.json")
        process_spec["id"] = process_id
        custom_process_from_process_graph(process_spec=process_spec)

        processes = api100.get("/processes").assert_status_code(200).json["processes"]
        processes_by_id = {p["id"]: p for p in processes}
        assert process_id in processes_by_id
        assert processes_by_id[process_id] == process_spec

    @pytest.mark.skip("not implemented")
    def test_processes_from_namespace(self, api100):
        process_id = generate_unique_test_process_id()

        # Register a custom process with process graph
        process_spec = api100.load_json("pg/1.0/add_and_multiply.json")
        process_spec["id"] = process_id
        custom_process_from_process_graph(process_spec=process_spec, namespace="foobar")
        process_spec_short = process_spec.copy()
        process_spec_short.pop("process_graph")

        processes = (
            api100.get("/processes/foobar").assert_status_code(200).json["processes"]
        )
        processes_by_id = {p["id"]: p for p in processes}
        assert process_id in processes_by_id
        assert processes_by_id[process_id] == process_spec_short

        processes = (
            api100.get("/processes/foobar?full=yes")
            .assert_status_code(200)
            .json["processes"]
        )
        processes_by_id = {p["id"]: p for p in processes}
        assert process_id in processes_by_id
        assert processes_by_id[process_id] == process_spec

        processes = api100.get("/processes").assert_status_code(200).json["processes"]
        assert "increment" not in set(p["id"] for p in processes)

    @pytest.mark.parametrize(
        ["user_id", "expect_success"],
        [
            ("Mark", False),
            ("John", True),
        ],
    )
    def test_user_access_validation(self, api, user_id, expect_success):
        headers = {
            "Authorization": "Bearer basic//"
            + HttpAuthHandler.build_basic_access_token(user_id=user_id)
        }
        response = api.get("/jobs", headers=headers)
        if expect_success:
            response.assert_status_code(200)
        else:
            response.assert_error(
                403, "PermissionsInsufficient", message="No access for Mark."
            )


@pytest.fixture
def oidc_provider(requests_mock):
    oidc_issuer = "https://eoidc.test"
    user_db = {
        "j0hn": {"sub": "john"},
        "4l1c3": {"sub": "Alice"},
        "b0b": {"sub": "b0b1b08571101437a2"},
        "c6r01": {"sub": "Carol"},
    }
    oidc_conf = f"{oidc_issuer}/.well-known/openid-configuration"
    oidc_userinfo_url = f"{oidc_issuer}/userinfo"
    requests_mock.get(oidc_conf, json={"userinfo_endpoint": oidc_userinfo_url})

    def userinfo(request, context):
        """Fake OIDC /userinfo endpoint handler"""
        _, _, token = request.headers["Authorization"].partition("Bearer ")
        if token in user_db:
            return user_db[token]
        else:
            context.status_code = 401
            return {"code": "InvalidToken"}

    requests_mock.get(oidc_userinfo_url, json=userinfo)


class TestUser:
    def test_no_auth(self, api):
        api.get("/me").assert_error(401, "AuthenticationRequired")

    def test_basic_auth(self, api):
        response = (
            api.get("/me", headers=TEST_USER_AUTH_HEADER).assert_status_code(200).json
        )
        assert response == {"name": TEST_USER, "user_id": TEST_USER}

    @pytest.mark.skip("quarantine - request-mock is not working")
    def test_oidc_basic(self, api, oidc_provider):
        response = (
            api.get("/me", headers={"Authorization": "Bearer oidc/app_id/j0hn"})
            .assert_status_code(200)
            .json
        )
        assert response == DictSubSet({"user_id": "john"})

    @pytest.mark.skip("quarantine - request-mock is not working")
    def test_oidc_invalid_access_token(self, api, oidc_provider):
        api.get(
            "/me", headers={"Authorization": "Bearer oidc/app_id/invalid"}
        ).assert_error(403, "TokenInvalid")

    def test_oidc_invalid_provider(self, api, oidc_provider):
        api.get(
            "/me", headers={"Authorization": "Bearer oidc/invalid/j0hn"}
        ).assert_error(403, "TokenInvalid")

    @pytest.mark.skip("not implemented")
    def test_default_plan(self, api, oidc_provider):
        response = (
            api.get("/me", headers={"Authorization": "Bearer oidc/app_id/4l1c3"})
            .assert_status_code(200)
            .json
        )
        assert response == DictSubSet(
            {"user_id": "Alice", "default_plan": "alice-plan"}
        )

    @pytest.mark.skip("not implemented")
    def test_roles(self, api, oidc_provider):
        response = (
            api.get("/me", headers={"Authorization": "Bearer oidc/app_id/c6r01"})
            .assert_status_code(200)
            .json
        )
        assert response == DictSubSet(
            {"user_id": "Carol", "roles": ["admin", "devops"]}
        )


class TestLogging:
    @pytest.mark.skip("not implemented")
    def test_user_id_logging(self, api, oidc_provider):
        with enhanced_logging(json=True, context=LOGGING_CONTEXT_FLASK) as logs:
            # Make request that requires auth (so that we have a user) and fails (so that we have a log to look at)
            resp = api.post(
                "/result",
                json={"broken": "yezz"},
                headers={"Authorization": "Bearer oidc/app_id/b0b"},
            )
            resp.assert_error(400, "ProcessGraphMissing")

        logs = [log for log in logs.getvalue().strip().split("\n")]
        logs = [json.loads(log) for log in logs]
        error = next(log for log in logs if "ProcessGraphMissing" in log["message"])

        assert error["user_id"] == "b0b1b08571101437a2"
        assert error["req_id"] == "123-456"


class TestCollections:
    def test_normalize_collection_metadata_no_id(self, caplog):
        with pytest.raises(KeyError):
            _normalize_collection_metadata(
                {"foo": "bar"}, api_version=ComparableVersion("1.0.0")
            )
        errors = [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR]
        assert any("should have 'id' field" in m for m in errors)

    @pytest.mark.skip("no support for v0.4.0")
    def test_normalize_collection_metadata_minimal_040(self, caplog):
        assert _normalize_collection_metadata(
            {"id": "foobar"}, api_version=ComparableVersion("0.4.2")
        ) == {
            "id": "foobar",
            "stac_version": "0.6.2",
            "stac_extensions": [
                "datacube",
                "https://stac-extensions.github.io/eo/v1.1.0/schema.json",
            ],
            "description": "foobar",
            "extent": {"spatial": [0, 0, 0, 0], "temporal": [None, None]},
            "license": "proprietary",
            "links": [],
        }
        warnings = set(
            r.getMessage() for r in caplog.records if r.levelno == logging.WARN
        )
        assert warnings == {
            "Collection 'foobar' metadata does not have field 'extent'."
        }

    @pytest.mark.skip("no support for v0.4.0")
    def test_normalize_collection_metadata_minimal_full_040(self, caplog):
        assert _normalize_collection_metadata(
            {"id": "foobar"}, api_version=ComparableVersion("0.4.2"), full=True
        ) == {
            "id": "foobar",
            "stac_version": "0.6.2",
            "stac_extensions": [
                "datacube",
                "https://stac-extensions.github.io/eo/v1.1.0/schema.json",
            ],
            "description": "foobar",
            "extent": {"spatial": [0, 0, 0, 0], "temporal": [None, None]},
            "license": "proprietary",
            "properties": {},
            "other_properties": {},
            "links": [],
        }
        warnings = set(
            r.getMessage() for r in caplog.records if r.levelno == logging.WARN
        )
        assert warnings == {
            "Collection 'foobar' metadata does not have field 'extent'.",
            "Collection 'foobar' metadata does not have field 'other_properties'.",
            "Collection 'foobar' metadata does not have field 'properties'.",
        }

    def test_normalize_collection_metadata_minimal_100(self, caplog):
        assert _normalize_collection_metadata(
            {"id": "foobar"}, api_version=ComparableVersion("1.0.0")
        ) == {
            "id": "foobar",
            "stac_version": "0.9.0",
            "stac_extensions": [
                "https://stac-extensions.github.io/datacube/v2.2.0/schema.json",
                "https://stac-extensions.github.io/eo/v1.1.0/schema.json",
            ],
            "description": "foobar",
            "extent": {
                "spatial": {"bbox": [[0, 0, 0, 0]]},
                "temporal": {"interval": [[None, None]]},
            },
            "license": "proprietary",
            "links": [],
        }
        warnings = set(
            r.getMessage() for r in caplog.records if r.levelno == logging.WARN
        )
        assert warnings == {
            "Collection 'foobar' metadata does not have field 'extent'."
        }

    def test_normalize_collection_metadata_minimal_full_100(self, caplog):
        assert _normalize_collection_metadata(
            {"id": "foobar"}, api_version=ComparableVersion("1.0.0"), full=True
        ) == {
            "id": "foobar",
            "stac_version": "0.9.0",
            "stac_extensions": [
                STAC_EXTENSION.DATACUBE,
                STAC_EXTENSION.EO,
            ],
            "description": "foobar",
            "extent": {
                "spatial": {"bbox": [[0, 0, 0, 0]]},
                "temporal": {"interval": [[None, None]]},
            },
            "license": "proprietary",
            "cube:dimensions": {},
            "summaries": {},
            "links": [],
        }
        warnings = set(
            r.getMessage() for r in caplog.records if r.levelno == logging.WARN
        )
        assert warnings == {
            "Collection 'foobar' metadata does not have field 'cube:dimensions'.",
            "Collection 'foobar' metadata does not have field 'extent'.",
            "Collection 'foobar' metadata does not have field 'summaries'.",
        }

    def test_normalize_collection_metadata_cube_dimensions_extent_full_100(
        self, caplog
    ):
        metadata = {
            "id": "foobar",
            "extent": {
                "spatial": {"bbox": [[-180, -56, 180, 83]]},
                "temporal": {"interval": [["2015-07-06", None]]},
            },
            "cube:dimensions": {
                "x": {"type": "spatial", "axis": "x"},
                "y": {"type": "spatial", "axis": "y"},
                "t": {"type": "temporal"},
            },
        }
        assert _normalize_collection_metadata(
            metadata, api_version=ComparableVersion("1.0.0"), full=True
        ) == {
            "id": "foobar",
            "stac_version": "0.9.0",
            "stac_extensions": [
                STAC_EXTENSION.DATACUBE,
                STAC_EXTENSION.EO,
            ],
            "description": "foobar",
            "extent": {
                "spatial": {"bbox": [[-180, -56, 180, 83]]},
                "temporal": {"interval": [["2015-07-06T00:00:00Z", None]]},
            },
            "license": "proprietary",
            "cube:dimensions": {
                "x": {"type": "spatial", "axis": "x", "extent": [-180, 180]},
                "y": {"type": "spatial", "axis": "y", "extent": [-56, 83]},
                "t": {"type": "temporal", "extent": ["2015-07-06T00:00:00Z", None]},
            },
            "summaries": {},
            "links": [],
        }

    @pytest.mark.skip(
        "Warning! openeo supports versions greater than or equal to 1.0.0"
    )
    def test_normalize_collection_metadata_dimensions_and_bands_040(self, caplog):
        metadata = {
            "id": "foobar",
            "cube:dimensions": {
                "x": {"type": "spatial"},
                "b": {"type": "bands", "values": ["B02", "B03"]},
            },
            "summaries": {"eo:bands": [{"name": "B02"}, {"name": "B03"}]},
        }
        res = _normalize_collection_metadata(
            metadata, api_version=ComparableVersion("0.4.0"), full=True
        )
        assert res["properties"]["cube:dimensions"] == {
            "x": {"type": "spatial"},
            "b": {"type": "bands", "values": ["B02", "B03"]},
        }
        assert res["properties"]["eo:bands"] == [{"name": "B02"}, {"name": "B03"}]

    def test_normalize_collection_metadata_dimensions_and_bands_100(self, caplog):
        metadata = {
            "id": "foobar",
            "properties": {
                "cube:dimensions": {
                    "x": {"type": "spatial"},
                    "b": {"type": "bands", "values": ["B02", "B03"]},
                },
                "eo:bands": [{"name": "B02"}, {"name": "B03"}],
            },
        }
        res = _normalize_collection_metadata(
            metadata, api_version=ComparableVersion("1.0.0"), full=True
        )
        assert res["cube:dimensions"] == {
            "x": {"type": "spatial"},
            "b": {"type": "bands", "values": ["B02", "B03"]},
        }
        assert res["summaries"]["eo:bands"] == [{"name": "B02"}, {"name": "B03"}]

    def test_normalize_collection_metadata_datetime(self, caplog):
        metadata = {
            "id": "foobar",
            "extent": {
                "temporal": {
                    "interval": [
                        ["2009-08-07", "2009-10-11"],
                        ["2011-12-13 14:15:16", None],
                    ],
                }
            },
        }
        res = _normalize_collection_metadata(
            metadata, api_version=ComparableVersion("1.0.0"), full=True
        )
        assert res["extent"]["temporal"]["interval"] == [
            ["2009-08-07T00:00:00Z", "2009-10-11T00:00:00Z"],
            ["2011-12-13T14:15:16Z", None],
        ]

    def test_collections(self, api: ApiTester):
        if api.api_version_compare.at_least("1.0.0"):
            with patch.object(
                Client,
                "open",
                return_value=MockPystacClient(),
            ):
                collection_items = get_collection_items(
                    collection_id="", parameters=None
                )
                with patch.object(
                    TensorLakehouseCollectionCatalog,
                    "get_collection_items",
                    return_value=collection_items,
                ):
                    resp = api.get("/collections").assert_status_code(200).json
                    assert "links" in resp
                    assert "collections" in resp
                    for collection in resp["collections"]:
                        validate_STAC_Collection(collection)
                        validate_OpenEO_collection(collection=collection, full=False)
        else:
            pytest.skip("API versions less than 1.0.0 are not supported")

    @pytest.mark.skip()
    def test_collection_items(self, api: ApiTester):
        collection = make_pystac_client_collection()
        collection_id = collection.id
        pystac_collections = [make_pystac_client_collection()]
        if api.api_version_compare.at_least("1.0.0"):
            with mock.patch.object(
                Client, "get_collections", return_value=pystac_collections
            ):
                with mock.patch.object(
                    TensorLakehouseCollectionCatalog,
                    "get_collection_items",
                    return_value=FEATURE_COLLECTION_JSON,
                ):
                    resp = (
                        api.get(f"/collections/{collection_id}/items")
                        .assert_status_code(200)
                        .json
                    )
                    assert "links" in resp
                    assert "collections" in resp
                    for coll in resp["collections"]:
                        validate_STAC_Collection(coll)
        else:
            pytest.skip("API versions less than 1.0.0 are not supported")

    @pytest.mark.skip("not implemented")
    def test_collections_caching(self, api):
        if api.api_version_compare.at_least("1.0.0"):
            resp = api.get("/collections").assert_status_code(200)
            assert resp.headers["Cache-Control"] == "max-age=900, public"
        else:
            pytest.skip("API versions less than 1.0.0 are not supported")

    @pytest.mark.skip("not implemented")
    def test_strip_private_fields(self, api):
        assert "_private" in dummy_backend.DummyCatalog().get_collection_metadata(
            "S2_FOOBAR"
        )
        # All metadata
        collections = (
            api.get("/collections").assert_status_code(200).json["collections"]
        )
        (metadata,) = (c for c in collections if c["id"] == "S2_FOOBAR")
        assert "_private" not in metadata
        # Single collection metadata
        metadata = api.get("/collections/S2_FOOBAR").assert_status_code(200).json
        assert "_private" not in metadata

    @pytest.mark.skip("not implemented")
    def test_collections_detail_invalid_collection(self, api):
        error = (
            api.get("/collections/FOOBOO").assert_error(404, "CollectionNotFound").json
        )
        assert error["message"] == "Collection 'FOOBOO' does not exist."

    def test_collections_detail(self, api: ApiTester):
        collection_id = "fake-collection-id"
        if api.api_version_compare.at_least("1.0.0"):
            with patch.object(
                Client,
                "open",
                return_value=MockPystacClient(collection_id=collection_id),
            ):
                with patch.object(
                    TensorLakehouseCollectionCatalog,
                    "get_collection_items",
                    return_value=FEATURE_COLLECTION_JSON,
                ):
                    collection = (
                        api.get(f"/collections/{collection_id}")
                        .assert_status_code(200)
                        .json
                    )
                    assert collection["id"] == collection_id
                    validate_STAC_Collection(collection)
                    validate_OpenEO_collection(collection=collection, full=True)
        else:
            pytest.skip("API versions less than 1.0.0 are not supported")

    @pytest.mark.skip("not implemented")
    def test_collections_detail_caching(self, api):
        resp = api.get("/collections/S2_FOOBAR").assert_status_code(200)
        assert resp.headers["Cache-Control"] == "max-age=900, public"

    @pytest.mark.skip("not implemented")
    def test_collections_detail_invalid_caching(self, api):
        resp = api.get("/collections/FOOBOO").assert_error(404, "CollectionNotFound")
        assert "Cache-Control" not in resp.headers


POST_RESULT_PAYLOADS = [
    (
        "api110",
        {
            "process": {
                "process_graph": {
                    "loadcollection1": {
                        "process_id": "load_collection",
                        "arguments": {
                            "bands": DAILY_PRISM["bands"],
                            "id": DAILY_PRISM["collection"],
                            "spatial_extent": {
                                "west": -91.0,
                                "south": 41.0,
                                "east": -90.95,
                                "north": 41.05,
                            },
                            "temporal_extent": ["2010-05-01", "2010-05-30"],
                        },
                    },
                    "reducedimension1": {
                        "process_id": "reduce_dimension",
                        "arguments": {
                            "data": {"from_node": "loadcollection1"},
                            "dimension": DEFAULT_TIME_DIMENSION,
                            "reducer": {
                                "process_graph": {
                                    "max1": {
                                        "process_id": "max",
                                        "arguments": {
                                            "data": {"from_parameter": "data"}
                                        },
                                        "result": True,
                                    }
                                }
                            },
                        },
                    },
                    "saveresult1": {
                        "process_id": "save_result",
                        "arguments": {
                            "data": {"from_node": "reducedimension1"},
                            "format": "netCDF",
                            "options": {},
                        },
                        "result": True,
                    },
                }
            }
        },
        {DEFAULT_Y_DIMENSION: 32, DEFAULT_X_DIMENSION: 32},
    ),
    (
        "api110",
        {
            "process": {
                "process_graph": {
                    "loadcollection1": {
                        "process_id": "load_collection",
                        "arguments": {
                            "bands": DAILY_PRISM["bands"],
                            "id": DAILY_PRISM["collection"],
                            "spatial_extent": {
                                "west": -91.0,
                                "south": 41.0,
                                "east": -90.95,
                                "north": 41.05,
                            },
                            "temporal_extent": ["2010-05-01", "2010-05-30"],
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
        {DEFAULT_TIME_DIMENSION: 59, DEFAULT_Y_DIMENSION: 32, DEFAULT_X_DIMENSION: 32},
    ),
    (
        "api110",
        {
            "process": {
                "process_graph": {
                    "loadcollection1": {
                        "process_id": "load_collection",
                        "arguments": {
                            "bands": DAILY_PRISM["bands"],
                            "id": DAILY_PRISM["collection"],
                            "spatial_extent": {
                                "west": -91.0,
                                "south": 41.0,
                                "east": -90.95,
                                "north": 41.05,
                            },
                            "temporal_extent": ["2010-05-01", "2010-05-30"],
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
        {DEFAULT_Y_DIMENSION: 32, DEFAULT_X_DIMENSION: 32, DEFAULT_TIME_DIMENSION: 10},
    ),
    (
        "api110",
        {
            "process": {
                "process_graph": {
                    "loadcollection1": {
                        "process_id": "load_collection",
                        "arguments": {
                            "bands": CROPSCAPE["bands"],
                            "id": CROPSCAPE["collection"],
                            "spatial_extent": {
                                "south": 41.5,
                                "west": -93.64,
                                "north": 41.83,
                                "east": -93.31,
                            },
                            "temporal_extent": ["2021-01-01", "2021-01-01"],
                        },
                    },
                    "reducedimension1": {
                        "process_id": "reduce_dimension",
                        "arguments": {
                            "data": {"from_node": "loadcollection1"},
                            "dimension": DEFAULT_TIME_DIMENSION,
                            "reducer": {
                                "process_graph": {
                                    "min1": {
                                        "process_id": "min",
                                        "arguments": {
                                            "data": {"from_parameter": "data"}
                                        },
                                        "result": True,
                                    }
                                }
                            },
                        },
                    },
                    "saveresult1": {
                        "process_id": "save_result",
                        "arguments": {
                            "data": {"from_node": "reducedimension1"},
                            "format": "netCDF",
                            "options": {},
                        },
                        "result": True,
                    },
                }
            }
        },
        {DEFAULT_X_DIMENSION: 1290, DEFAULT_Y_DIMENSION: 1290},
    ),
]


# @pytest.mark.skip()
@pytest.mark.parametrize(
    "api110, payload, expected_dims", POST_RESULT_PAYLOADS, indirect=["api110"]
)
class TestSynchronousPostResult:
    AUTH_HEADER = TEST_USER_AUTH_HEADER

    def test_post_result(self, api110, payload, expected_dims):
        bands = payload["process"]["process_graph"]["loadcollection1"]["arguments"][
            "bands"
        ]

        spatial_extent = payload["process"]["process_graph"]["loadcollection1"][
            "arguments"
        ]["spatial_extent"]
        latmin = spatial_extent.get("south")
        latmax = spatial_extent.get("north")
        lonmin = spatial_extent.get("west")
        lonmax = spatial_extent.get("east")
        temporal_extent = payload["process"]["process_graph"]["loadcollection1"][
            "arguments"
        ]["temporal_extent"]
        size_time = 10
        mock_data_array = generate_xarray_datarray(
            bands=bands,
            latmax=latmax,
            latmin=latmin,
            lonmax=lonmax,
            lonmin=lonmin,
            timestamps=temporal_extent,
            size_x=expected_dims.get(DEFAULT_X_DIMENSION, 100),
            size_y=expected_dims.get(DEFAULT_Y_DIMENSION, 100),
            num_periods=expected_dims.get(DEFAULT_TIME_DIMENSION, size_time),
            crs=GEODN_DISCOVERY_CRS,
            freq=None,
        )

        with patch.object(Client, "open", return_value=MockPystacClient()):
            with patch.object(
                LoadCollectionFromCOS, "load_collection", return_value=mock_data_array
            ):
                resp = api110.post(
                    "/result",
                    headers=self.AUTH_HEADER,
                    json=payload,
                )
                assert resp.status_code in [200, 201]
                data = resp.data

                prefix = "test_post_result_"
                path = Path(TEST_DATA_ROOT) / f"{prefix}{uuid.uuid4().hex}.nc"
                # test_files = [f for f in os.listdir(os.getcwd()) if f.startswith(prefix)]

                with open(path, "wb") as s:
                    s.write(data)
                    validate_downloaded_file(
                        path=path,
                        expected_dimension_size=expected_dims,
                        band_names=bands,
                        expected_crs=GEODN_DISCOVERY_CRS,
                    )
                path.unlink()


class TestBatchJobs:
    AUTH_HEADER = TEST_USER_AUTH_HEADER

    @staticmethod
    @contextmanager
    def _fresh_job_registry(
        next_job_id="job-1234",
        output_root: Optional[Path] = None,
        jobs: Optional[dict] = None,
    ):
        """Set up a fresh job registry and predefine next job id"""

        with ExitStack() as exit_stack:
            # Conditional setup of some mock contexts
            if next_job_id:
                exit_stack.enter_context(
                    mock.patch.object(
                        dummy_backend.DummyBatchJobs,
                        "generate_job_id",
                        return_value=next_job_id,
                    )
                )
            if output_root:
                exit_stack.enter_context(
                    mock.patch.object(
                        dummy_backend.DummyBatchJobs,
                        "_output_root",
                        return_value=output_root,
                    )
                )

            dummy_backend.DummyBatchJobs._job_registry = {
                (TEST_USER, "07024ee9-7847-4b8a-b260-6c879a2b3cdc"): BatchJobMetadata(
                    id="07024ee9-7847-4b8a-b260-6c879a2b3cdc",
                    status="running",
                    process={
                        "process_graph": {"foo": {"process_id": "foo", "arguments": {}}}
                    },
                    created=datetime(2017, 1, 1, 9, 32, 12),
                ),
                (TEST_USER, "53c71345-09b4-46b4-b6b0-03fd6fe1f199"): BatchJobMetadata(
                    id="53c71345-09b4-46b4-b6b0-03fd6fe1f199",
                    title="Your title here.",
                    description="Your description here.",
                    status="finished",
                    progress=100,
                    process={
                        "process_graph": {"foo": {"process_id": "foo", "arguments": {}}}
                    },
                    created=datetime(2020, 6, 11, 11, 51, 29),
                    updated=datetime(2020, 6, 11, 11, 55, 15),
                    started=datetime(2020, 6, 11, 11, 55, 9),
                    finished=datetime(2020, 6, 11, 11, 55, 15),
                    memory_time_megabyte=timedelta(seconds=18704944),
                    cpu_time=timedelta(seconds=1621),
                    geometry={
                        "type": "Polygon",
                        "coordinates": [
                            [
                                [-180, -90],
                                [180, -90],
                                [180, 90],
                                [-180, 90],
                                [-180, -90],
                            ]
                        ],
                    },
                    bbox=[-180, -90, 180, 90],
                    start_datetime=datetime(1981, 4, 24, 3, 0, 0),
                    end_datetime=datetime(1981, 4, 24, 3, 0, 0),
                    instruments=["MSI"],
                    epsg=4326,
                    plan="some_plan",
                    costs=1.23,
                    budget=4.56,
                ),
            }
            if jobs:
                for job_id, job_settings in jobs.items():
                    key = (job_settings.get("user", TEST_USER), job_id)
                    dummy_backend.DummyBatchJobs._job_registry[key] = BatchJobMetadata(
                        id=job_id,
                        status=job_settings.get("status", "running"),
                        process={
                            "process_graph": {
                                "foo": {"process_id": "foo", "arguments": {}}
                            }
                        },
                        created=datetime(2017, 1, 1, 9, 32, 12),
                    )
            yield dummy_backend.DummyBatchJobs._job_registry

    @pytest.mark.skip("not implemented yet")
    def test_create_job_040(self, api040):
        with self._fresh_job_registry(next_job_id="job-220"):
            resp = api040.post(
                "/jobs",
                headers=self.AUTH_HEADER,
                json={
                    "title": "foo job",
                    "process_graph": {"foo": {"process_id": "foo", "arguments": {}}},
                },
            ).assert_status_code(201)
        assert resp.headers["Location"] == "http://oeo.net/openeo/0.4.0/jobs/job-220"
        assert resp.headers["OpenEO-Identifier"] == "job-220"
        job_info = dummy_backend.DummyBatchJobs._job_registry[TEST_USER, "job-220"]
        assert job_info.id == "job-220"
        assert job_info.process == {
            "process_graph": {"foo": {"process_id": "foo", "arguments": {}}}
        }
        assert job_info.status == "created"
        assert job_info.created == dummy_backend.DEFAULT_DATETIME
        assert job_info.job_options is None

    @pytest.mark.skip("unsupported version 0.4.0")
    def test_create_job_with_options_040(self, api040):
        with self._fresh_job_registry(next_job_id="job-230"):
            resp = api040.post(
                "/jobs",
                headers=self.AUTH_HEADER,
                json={
                    "title": "foo job",
                    "process_graph": {"foo": {"process_id": "foo", "arguments": {}}},
                    "job_options": {"driver-memory": "3g", "executor-memory": "5g"},
                },
            ).assert_status_code(201)
        assert resp.headers["Location"] == "http://oeo.net/openeo/0.4.0/jobs/job-230"
        assert resp.headers["OpenEO-Identifier"] == "job-230"
        job_info = dummy_backend.DummyBatchJobs._job_registry[TEST_USER, "job-230"]
        assert job_info.job_options == {"driver-memory": "3g", "executor-memory": "5g"}

    def test_create_job_100(self, api100):
        with self._fresh_job_registry(next_job_id="job-245"):
            resp = api100.post(
                "/jobs",
                headers=self.AUTH_HEADER,
                # headers=None,
                json={
                    "process": {
                        "process_graph": {
                            "foo": {"process_id": "foo", "arguments": {}}
                        },
                        "summary": "my foo job",
                    },
                    "title": "Foo job",
                    "description": "Run the `foo` process!",
                },
            ).assert_status_code(201)
        # assert resp.headers["Location"] == "http://oeo.net/openeo/1.0.0/jobs/job-245"
        job_id = resp.headers["Location"].replace(
            "http://oeo.net/openeo/1.0.0/jobs/", ""
        )
        assert isinstance(job_id, str)
        assert len(job_id) > 0
        # assert resp.headers["OpenEO-Identifier"] == "job-245"
        # job_info = dummy_backend.DummyBatchJobs._job_registry[TEST_USER, "job-245"]
        # assert job_info.id == "job-245"
        # assert job_info.process == {
        #     "process_graph": {"foo": {"process_id": "foo", "arguments": {}}}
        # }
        # assert job_info.status == "created"
        # assert job_info.created == dummy_backend.DEFAULT_DATETIME
        # assert job_info.job_options is None
        # assert job_info.title == "Foo job"
        # assert job_info.description == "Run the `foo` process!"

    @pytest.mark.skip("not implemented yet")
    def test_create_job_100_with_options(self, api100):
        with self._fresh_job_registry(next_job_id="job-256"):
            resp = api100.post(
                "/jobs",
                headers=self.AUTH_HEADER,
                json={
                    "process": {
                        "process_graph": {
                            "foo": {"process_id": "foo", "arguments": {}}
                        },
                        "summary": "my foo job",
                    },
                    "job_options": {"driver-memory": "3g", "executor-memory": "5g"},
                },
            ).assert_status_code(201)
        assert resp.headers["Location"] == "http://oeo.net/openeo/1.0.0/jobs/job-256"
        assert resp.headers["OpenEO-Identifier"] == "job-256"
        job_info = dummy_backend.DummyBatchJobs._job_registry[TEST_USER, "job-256"]
        assert job_info.job_options == {"driver-memory": "3g", "executor-memory": "5g"}

    @pytest.mark.skip("not implemented yet")
    def test_create_job_100_with_options_inline(self, api100):
        with self._fresh_job_registry(next_job_id="job-256"):
            resp = api100.post(
                "/jobs",
                headers=self.AUTH_HEADER,
                json={
                    "process": {
                        "process_graph": {
                            "foo": {"process_id": "foo", "arguments": {}}
                        },
                        "summary": "my foo job",
                    },
                    "driver-memory": "3g",
                    "executor-memory": "5g",
                },
            ).assert_status_code(201)
        assert resp.headers["Location"] == "http://oeo.net/openeo/1.0.0/jobs/job-256"
        assert resp.headers["OpenEO-Identifier"] == "job-256"
        job_info = dummy_backend.DummyBatchJobs._job_registry[TEST_USER, "job-256"]
        assert job_info.job_options == {"driver-memory": "3g", "executor-memory": "5g"}

    @pytest.mark.skip("not implemented yet")
    def test_start_job(self, api):
        with self._fresh_job_registry(next_job_id="job-267") as registry:
            api.post(
                "/jobs",
                headers=self.AUTH_HEADER,
                json=api.get_process_graph_dict(
                    {"foo": {"process_id": "foo", "arguments": {}}},
                ),
            ).assert_status_code(201)
            assert registry[TEST_USER, "job-267"].status == "created"
            api.post(
                "/jobs/job-267/results", headers=self.AUTH_HEADER, json={}
            ).assert_status_code(202)
            assert registry[TEST_USER, "job-267"].status == "running"

    @pytest.mark.parametrize(
        ["orig_status", "start"],
        [
            ("created", True),
            ("queued", False),
            ("running", False),
            ("finished", False),
            ("error", False),
            ("canceled", True),
        ],
    )
    @pytest.mark.skip("not implemented yet")
    def test_start_job_existing_status(self, api, orig_status, start):
        """Only really start jobs that are started (or canceled)"""
        with self._fresh_job_registry() as registry:
            registry[TEST_USER, "job-267"] = BatchJobMetadata(
                id="job-267",
                status=orig_status,
                created=datetime(2017, 1, 1, 9, 32, 12),
            )
            # Try to start job
            with mock.patch.object(
                dummy_backend.DummyBatchJobs, "start_job"
            ) as start_job:
                api.post(
                    "/jobs/job-267/results", headers=self.AUTH_HEADER, json={}
                ).assert_status_code(202)
                assert start_job.call_count == (1 if start else 0)

    @pytest.mark.skip("not implemented yet")
    def test_start_job_invalid(self, api):
        resp = api.post("/jobs/deadbeef-f00/results", headers=self.AUTH_HEADER)
        resp.assert_error(404, "JobNotFound")
        assert resp.json["message"] == "The batch job 'deadbeef-f00' does not exist."

    @pytest.mark.skip("not implemented yet")
    def test_get_job_info_040(self, api040):
        with self._fresh_job_registry():
            resp = api040.get(
                "/jobs/07024ee9-7847-4b8a-b260-6c879a2b3cdc", headers=self.AUTH_HEADER
            )
        assert resp.assert_status_code(200).json == {
            "id": "07024ee9-7847-4b8a-b260-6c879a2b3cdc",
            "status": "running",
            "submitted": "2017-01-01T09:32:12Z",
            "process_graph": {"foo": {"process_id": "foo", "arguments": {}}},
        }

    @pytest.mark.skip("not implemented yet")
    def test_get_job_info_metrics_100(self, api100):
        resp = api100.get(
            "/jobs/53c71345-09b4-46b4-b6b0-03fd6fe1f199", headers=self.AUTH_HEADER
        )
        assert resp.assert_status_code(200).json == {
            "id": "53c71345-09b4-46b4-b6b0-03fd6fe1f199",
            "title": "Your title here.",
            "description": "Your description here.",
            "process": {
                "process_graph": {"foo": {"process_id": "foo", "arguments": {}}}
            },
            "status": "finished",
            "progress": 100,
            "created": "2020-06-11T11:51:29Z",
            "updated": "2020-06-11T11:55:15Z",
            "plan": "some_plan",
            "costs": 1.23,
            "budget": 4.56,
            "usage": {
                "cpu": {"value": 1621, "unit": "cpu-seconds"},
                "duration": {"value": 6, "unit": "seconds"},
                "memory": {"value": 18704944, "unit": "mb-seconds"},
            },
        }

    @pytest.mark.skip("not implemented yet")
    def test_get_job_info_100(self, api100):
        with self._fresh_job_registry():
            resp = api100.get(
                "/jobs/07024ee9-7847-4b8a-b260-6c879a2b3cdc", headers=self.AUTH_HEADER
            )
        assert resp.assert_status_code(200).json == {
            "id": "07024ee9-7847-4b8a-b260-6c879a2b3cdc",
            "status": "running",
            "created": "2017-01-01T09:32:12Z",
            "process": {
                "process_graph": {"foo": {"process_id": "foo", "arguments": {}}}
            },
        }

    @pytest.mark.skip("not implemented yet")
    def test_get_job_info_invalid(self, api):
        resp = api.get("/jobs/deadbeef-f00", headers=self.AUTH_HEADER).assert_error(
            404, "JobNotFound"
        )
        assert resp.json["message"] == "The batch job 'deadbeef-f00' does not exist."

    @pytest.mark.skip("not implemented yet")
    def test_list_user_jobs_040(self, api040):
        with self._fresh_job_registry():
            resp = api040.get("/jobs", headers=self.AUTH_HEADER)
        assert resp.assert_status_code(200).json == {
            "jobs": [
                {
                    "id": "07024ee9-7847-4b8a-b260-6c879a2b3cdc",
                    "status": "running",
                    "submitted": "2017-01-01T09:32:12Z",
                },
                {
                    "id": "53c71345-09b4-46b4-b6b0-03fd6fe1f199",
                    "title": "Your title here.",
                    "description": "Your description here.",
                    "status": "finished",
                    "progress": 100,
                    "submitted": "2020-06-11T11:51:29Z",
                    "updated": "2020-06-11T11:55:15Z",
                    "plan": "some_plan",
                    "costs": 1.23,
                    "budget": 4.56,
                },
            ],
            "links": [],
        }

    @pytest.mark.skip("not implemented yet")
    def test_list_user_jobs_100(self, api100):
        with self._fresh_job_registry():
            resp = api100.get("/jobs", headers=self.AUTH_HEADER)
        assert resp.assert_status_code(200).json == {
            "jobs": [
                {
                    "id": "07024ee9-7847-4b8a-b260-6c879a2b3cdc",
                    "status": "running",
                    "created": "2017-01-01T09:32:12Z",
                },
                {
                    "id": "53c71345-09b4-46b4-b6b0-03fd6fe1f199",
                    "title": "Your title here.",
                    "description": "Your description here.",
                    "status": "finished",
                    "progress": 100,
                    "created": "2020-06-11T11:51:29Z",
                    "updated": "2020-06-11T11:55:15Z",
                    "plan": "some_plan",
                    "costs": 1.23,
                    "budget": 4.56,
                },
            ],
            "links": [],
        }

    @pytest.mark.skip("not implemented yet")
    def test_list_user_jobs_100_extra(self, api100):
        """
        `get_user_jobs` returns a richer `dict` instead of just `List[BatchJobMetadata]`
        """

        def get_user_jobs(self, user_id: str):
            return {
                "jobs": [
                    BatchJobMetadata(
                        id="id-123",
                        status="running",
                        created=datetime(2017, 1, 1, 9, 32, 12),
                    )
                ],
                "links": [{"rel": "info", "href": "https://info.test"}],
                "federation:missing": ["b4"],
                "something else": "ignore me",
            }

        with mock.patch.object(
            dummy_backend.DummyBatchJobs, "get_user_jobs", new=get_user_jobs
        ):
            resp = api100.get("/jobs", headers=self.AUTH_HEADER)

        assert resp.assert_status_code(200).json == {
            "jobs": [
                {
                    "id": "id-123",
                    "status": "running",
                    "created": "2017-01-01T09:32:12Z",
                },
            ],
            "links": [{"rel": "info", "href": "https://info.test"}],
            "federation:missing": ["b4"],
        }

    @pytest.mark.skip("not implemented yet")
    def test_get_job_results_unfinished(self, api):
        with self._fresh_job_registry(next_job_id="job-345"):
            resp = api.get(
                "/jobs/07024ee9-7847-4b8a-b260-6c879a2b3cdc/results",
                headers=self.AUTH_HEADER,
            )
        resp.assert_error(400, "JobNotFinished")

    @pytest.mark.skip("not implemented yet")
    def test_get_job_results_040(self, api040):
        with self._fresh_job_registry(next_job_id="job-349"):
            dummy_backend.DummyBatchJobs._update_status(
                job_id="07024ee9-7847-4b8a-b260-6c879a2b3cdc",
                user_id=TEST_USER,
                status="finished",
            )
            resp = api040.get(
                "/jobs/07024ee9-7847-4b8a-b260-6c879a2b3cdc/results",
                headers=self.AUTH_HEADER,
            )
        assert resp.assert_status_code(200).json == {
            "links": [
                {
                    "href": "http://oeo.net/openeo/0.4.0/jobs/07024ee9-7847-4b8a-b260-6c879a2b3cdc/results/assets/output.tiff"
                },
                {
                    "href": "http://oeo.net/openeo/0.4.0/jobs/07024ee9-7847-4b8a-b260-6c879a2b3cdc/results/assets/randomforest.model"
                },
                {
                    "href": "http://oeo.net/openeo/0.4.0/jobs/07024ee9-7847-4b8a-b260-6c879a2b3cdc/results/assets/ml_model_metadata.json"
                },
            ]
        }

    @pytest.mark.skip("not implemented yet")
    def test_get_job_results_100(self, api100):
        with self._fresh_job_registry(next_job_id="job-362"):
            dummy_backend.DummyBatchJobs._update_status(
                job_id="07024ee9-7847-4b8a-b260-6c879a2b3cdc",
                user_id=TEST_USER,
                status="finished",
            )
            resp = api100.get(
                "/jobs/07024ee9-7847-4b8a-b260-6c879a2b3cdc/results",
                headers=self.AUTH_HEADER,
            )
            assert resp.assert_status_code(200).json == {
                "assets": {
                    "output.tiff": {
                        "roles": ["data"],
                        "title": "output.tiff",
                        "href": "http://oeo.net/openeo/1.0.0/jobs/07024ee9-7847-4b8a-b260-6c879a2b3cdc/results/assets/output.tiff",
                        "type": "image/tiff; application=geotiff",
                        "eo:bands": [{"name": "NDVI", "center_wavelength": 1.23}],
                        "file:nodata": [123],
                    },
                    "randomforest.model": {
                        "href": "http://oeo.net/openeo/1.0.0/jobs/07024ee9-7847-4b8a-b260-6c879a2b3cdc/results/assets/randomforest.model",
                        "roles": ["data"],
                        "title": "randomforest.model",
                        "type": "application/octet-stream",
                    },
                },
                "geometry": None,
                "id": "07024ee9-7847-4b8a-b260-6c879a2b3cdc",
                "links": [
                    {
                        "rel": "self",
                        "href": "http://oeo.net/openeo/1.0.0/jobs/07024ee9-7847-4b8a-b260-6c879a2b3cdc/results",
                        "type": "application/json",
                    },
                    {
                        "rel": "canonical",
                        "href": "http://oeo.net/openeo/1.0.0/jobs/07024ee9-7847-4b8a-b260-6c879a2b3cdc/results",
                        "type": "application/json",
                    },
                    {
                        "rel": "card4l-document",
                        "href": "http://ceos.org/ard/files/PFS/SR/v5.0/CARD4L_Product_Family_Specification_Surface_Reflectance-v5.0.pdf",
                        "type": "application/pdf",
                    },
                ],
                "properties": {
                    "created": "2017-01-01T09:32:12Z",
                    "datetime": None,
                    "card4l:processing_chain": {
                        "process_graph": {"foo": {"process_id": "foo", "arguments": {}}}
                    },
                    "card4l:specification": "SR",
                    "card4l:specification_version": "5.0",
                    "processing:facility": "VITO - SPARK",
                    "processing:software": "openeo-geotrellis-0.0.1",
                },
                "stac_extensions": [
                    "processing",
                    "card4l-eo",
                    "https://stac-extensions.github.io/file/v1.0.0/schema.json",
                    "eo",
                ],
                "stac_version": "0.9.0",
                "type": "Feature",
            }

            resp = api100.get(
                "/jobs/53c71345-09b4-46b4-b6b0-03fd6fe1f199/results",
                headers=self.AUTH_HEADER,
            )

            assert resp.assert_status_code(200).json == {
                "assets": {
                    "output.tiff": {
                        "roles": ["data"],
                        "title": "output.tiff",
                        "href": "http://oeo.net/openeo/1.0.0/jobs/53c71345-09b4-46b4-b6b0-03fd6fe1f199/results/assets/output.tiff",
                        "type": "image/tiff; application=geotiff",
                        "eo:bands": [{"name": "NDVI", "center_wavelength": 1.23}],
                        "file:nodata": [123],
                    },
                    "randomforest.model": {
                        "href": "http://oeo.net/openeo/1.0.0/jobs/53c71345-09b4-46b4-b6b0-03fd6fe1f199/results/assets/randomforest.model",
                        "roles": ["data"],
                        "title": "randomforest.model",
                        "type": "application/octet-stream",
                    },
                },
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [[-180, -90], [180, -90], [180, 90], [-180, 90], [-180, -90]]
                    ],
                },
                "bbox": [-180, -90, 180, 90],
                "id": "53c71345-09b4-46b4-b6b0-03fd6fe1f199",
                "links": [
                    {
                        "rel": "self",
                        "href": "http://oeo.net/openeo/1.0.0/jobs/53c71345-09b4-46b4-b6b0-03fd6fe1f199/results",
                        "type": "application/json",
                    },
                    {
                        "rel": "canonical",
                        "href": "http://oeo.net/openeo/1.0.0/jobs/53c71345-09b4-46b4-b6b0-03fd6fe1f199/results",
                        "type": "application/json",
                    },
                    {
                        "rel": "card4l-document",
                        "href": "http://ceos.org/ard/files/PFS/SR/v5.0/CARD4L_Product_Family_Specification_Surface_Reflectance-v5.0.pdf",
                        "type": "application/pdf",
                    },
                ],
                "properties": {
                    "created": "2020-06-11T11:51:29Z",
                    "updated": "2020-06-11T11:55:15Z",
                    "datetime": "1981-04-24T03:00:00Z",
                    "title": "Your title here.",
                    "description": "Your description here.",
                    "instruments": ["MSI"],
                    "proj:epsg": 4326,
                    "card4l:processing_chain": {
                        "process_graph": {"foo": {"process_id": "foo", "arguments": {}}}
                    },
                    "card4l:specification": "SR",
                    "card4l:specification_version": "5.0",
                    "processing:facility": "VITO - SPARK",
                    "processing:software": "openeo-geotrellis-0.0.1",
                },
                "stac_extensions": [
                    "processing",
                    "card4l-eo",
                    "https://stac-extensions.github.io/file/v1.0.0/schema.json",
                    "eo",
                    "projection",
                ],
                "stac_version": "0.9.0",
                "type": "Feature",
            }

    @pytest.mark.skip("not implemented yet")
    def test_get_job_results_public_href_asset_100(
        self, api100, backend_implementation
    ):
        import numpy as np

        results_data = {
            "output.tiff": {
                BatchJobs.ASSET_PUBLIC_HREF: "http://storage.test/r362/res.tiff?sgn=23432ldf348fl4r349",
                "asset": True,
                "type": "application/tiff",
                "nodata": np.nan,
            }
        }
        with (
            self._fresh_job_registry(
                jobs={"07024ee9-7847-4b8a-b260-6c879a2b3cdc": {"status": "finished"}}
            ),
            mock.patch.object(
                backend_implementation.batch_jobs,
                "get_results",
                return_value=results_data,
            ),
        ):
            resp = api100.get(
                "/jobs/07024ee9-7847-4b8a-b260-6c879a2b3cdc/results",
                headers=self.AUTH_HEADER,
            )
        res = resp.assert_status_code(200).json
        assert res["assets"] == {
            "output.tiff": {
                "href": "http://storage.test/r362/res.tiff?sgn=23432ldf348fl4r349",
                "roles": ["data"],
                "title": "output.tiff",
                "file:nodata": ["nan"],
                "type": "application/tiff",
            }
        }

    @pytest.mark.skip("not implemented yet")
    def test_get_job_results_signed_040(self, api040, flask_app):
        app_config = {"SIGNED_URL": "TRUE", "SIGNED_URL_SECRET": "123&@#"}
        with mock.patch.dict(flask_app.config, app_config), self._fresh_job_registry():
            dummy_backend.DummyBatchJobs._update_status(
                job_id="07024ee9-7847-4b8a-b260-6c879a2b3cdc",
                user_id=TEST_USER,
                status="finished",
            )
            resp = api040.get(
                "/jobs/07024ee9-7847-4b8a-b260-6c879a2b3cdc/results",
                headers=self.AUTH_HEADER,
            )
        assert resp.assert_status_code(200).json == {
            "links": [
                {
                    "href": "http://oeo.net/openeo/0.4.0/jobs/07024ee9-7847-4b8a-b260-6c879a2b3cdc/results/assets/TXIuVGVzdA%3D%3D/50afb0cad129e61d415278c4ffcd8a83/output.tiff"
                },
                {
                    "href": "http://oeo.net/openeo/0.4.0/jobs/07024ee9-7847-4b8a-b260-6c879a2b3cdc/results/assets/TXIuVGVzdA%3D%3D/741cfd7379a9eda4bc1c8b0c5155bfe9/randomforest.model"
                },
                {
                    "href": "http://oeo.net/openeo/0.4.0/jobs/07024ee9-7847-4b8a-b260-6c879a2b3cdc/results/assets/TXIuVGVzdA%3D%3D/272d7aa46727ee3f11a7211d5be953e4/ml_model_metadata.json"
                },
            ]
        }

    @pytest.mark.skip("not implemented yet")
    def test_get_job_results_signed_100(self, api100, flask_app):
        app_config = {"SIGNED_URL": "TRUE", "SIGNED_URL_SECRET": "123&@#"}
        with mock.patch.dict(flask_app.config, app_config), self._fresh_job_registry():
            dummy_backend.DummyBatchJobs._update_status(
                job_id="07024ee9-7847-4b8a-b260-6c879a2b3cdc",
                user_id=TEST_USER,
                status="finished",
            )
            resp = api100.get(
                "/jobs/07024ee9-7847-4b8a-b260-6c879a2b3cdc/results",
                headers=self.AUTH_HEADER,
            )
            assert resp.assert_status_code(200).json == {
                "assets": {
                    "output.tiff": {
                        "roles": ["data"],
                        "title": "output.tiff",
                        "href": "http://oeo.net/openeo/1.0.0/jobs/07024ee9-7847-4b8a-b260-6c879a2b3cdc/results/assets/TXIuVGVzdA%3D%3D/50afb0cad129e61d415278c4ffcd8a83/output.tiff",
                        "type": "image/tiff; application=geotiff",
                        "eo:bands": [{"name": "NDVI", "center_wavelength": 1.23}],
                        "file:nodata": [123],
                    },
                    "randomforest.model": {
                        "href": "http://oeo.net/openeo/1.0.0/jobs/07024ee9-7847-4b8a-b260-6c879a2b3cdc/results/assets/TXIuVGVzdA%3D%3D/741cfd7379a9eda4bc1c8b0c5155bfe9/randomforest.model",
                        "roles": ["data"],
                        "title": "randomforest.model",
                        "type": "application/octet-stream",
                    },
                },
                "geometry": None,
                "id": "07024ee9-7847-4b8a-b260-6c879a2b3cdc",
                "links": [
                    {
                        "rel": "self",
                        "href": "http://oeo.net/openeo/1.0.0/jobs/07024ee9-7847-4b8a-b260-6c879a2b3cdc/results",
                        "type": "application/json",
                    },
                    {
                        "rel": "canonical",
                        "href": "http://oeo.net/openeo/1.0.0/jobs/07024ee9-7847-4b8a-b260-6c879a2b3cdc/results/TXIuVGVzdA%3D%3D/05cb8b78f20c68a5aa9eb05249928d24",
                        "type": "application/json",
                    },
                    {
                        "rel": "card4l-document",
                        "href": "http://ceos.org/ard/files/PFS/SR/v5.0/CARD4L_Product_Family_Specification_Surface_Reflectance-v5.0.pdf",
                        "type": "application/pdf",
                    },
                ],
                "properties": {
                    "created": "2017-01-01T09:32:12Z",
                    "datetime": None,
                    "card4l:processing_chain": {
                        "process_graph": {"foo": {"process_id": "foo", "arguments": {}}}
                    },
                    "card4l:specification": "SR",
                    "card4l:specification_version": "5.0",
                    "processing:facility": "VITO - SPARK",
                    "processing:software": "openeo-geotrellis-0.0.1",
                },
                "stac_extensions": [
                    "processing",
                    "card4l-eo",
                    "https://stac-extensions.github.io/file/v1.0.0/schema.json",
                    "eo",
                ],
                "stac_version": "0.9.0",
                "type": "Feature",
            }

    @pytest.mark.skip("not implemented yet")
    @mock.patch("time.time", mock.MagicMock(return_value=1234))
    def test_get_job_results_signed_with_expiration_040(self, api040, flask_app):
        app_config = {
            "SIGNED_URL": "TRUE",
            "SIGNED_URL_SECRET": "123&@#",
            "SIGNED_URL_EXPIRATION": "1000",
        }
        with mock.patch.dict(flask_app.config, app_config), self._fresh_job_registry():
            dummy_backend.DummyBatchJobs._update_status(
                job_id="07024ee9-7847-4b8a-b260-6c879a2b3cdc",
                user_id=TEST_USER,
                status="finished",
            )
            resp = api040.get(
                "/jobs/07024ee9-7847-4b8a-b260-6c879a2b3cdc/results",
                headers=self.AUTH_HEADER,
            )
        assert resp.assert_status_code(200).json == {
            "links": [
                {
                    "href": "http://oeo.net/openeo/0.4.0/jobs/07024ee9-7847-4b8a-b260-6c879a2b3cdc/results/assets/TXIuVGVzdA%3D%3D/fd0ca65e29c6d223da05b2e73a875683/output.tiff?expires=2234"
                },
                {
                    "href": "http://oeo.net/openeo/0.4.0/jobs/07024ee9-7847-4b8a-b260-6c879a2b3cdc/results/assets/TXIuVGVzdA%3D%3D/22b76413158c59acaccc74e74841a473/randomforest.model?expires=2234"
                },
                {
                    "href": "http://oeo.net/openeo/0.4.0/jobs/07024ee9-7847-4b8a-b260-6c879a2b3cdc/results/assets/TXIuVGVzdA%3D%3D/a23629392982e57e7312e34de4bdba95/ml_model_metadata.json?expires=2234"
                },
            ]
        }

    @pytest.mark.skip("not implemented yet")
    @mock.patch("time.time", mock.MagicMock(return_value=1234))
    def test_get_job_results_signed_with_expiration_100(self, api100, flask_app):
        app_config = {
            "SIGNED_URL": "TRUE",
            "SIGNED_URL_SECRET": "123&@#",
            "SIGNED_URL_EXPIRATION": "1000",
        }
        with mock.patch.dict(flask_app.config, app_config), self._fresh_job_registry():
            dummy_backend.DummyBatchJobs._update_status(
                job_id="07024ee9-7847-4b8a-b260-6c879a2b3cdc",
                user_id=TEST_USER,
                status="finished",
            )
            resp = api100.get(
                "/jobs/07024ee9-7847-4b8a-b260-6c879a2b3cdc/results",
                headers=self.AUTH_HEADER,
            )
            assert resp.assert_status_code(200).json == {
                "assets": {
                    "output.tiff": {
                        "roles": ["data"],
                        "title": "output.tiff",
                        "href": "http://oeo.net/openeo/1.0.0/jobs/07024ee9-7847-4b8a-b260-6c879a2b3cdc/results/assets/TXIuVGVzdA%3D%3D/fd0ca65e29c6d223da05b2e73a875683/output.tiff?expires=2234",
                        "type": "image/tiff; application=geotiff",
                        "eo:bands": [{"name": "NDVI", "center_wavelength": 1.23}],
                        "file:nodata": [123],
                    },
                    "randomforest.model": {
                        "href": "http://oeo.net/openeo/1.0.0/jobs/07024ee9-7847-4b8a-b260-6c879a2b3cdc/results/assets/TXIuVGVzdA%3D%3D/22b76413158c59acaccc74e74841a473/randomforest.model?expires=2234",
                        "roles": ["data"],
                        "title": "randomforest.model",
                        "type": "application/octet-stream",
                    },
                },
                "geometry": None,
                "id": "07024ee9-7847-4b8a-b260-6c879a2b3cdc",
                "links": [
                    {
                        "rel": "self",
                        "href": "http://oeo.net/openeo/1.0.0/jobs/07024ee9-7847-4b8a-b260-6c879a2b3cdc/results",
                        "type": "application/json",
                    },
                    {
                        "rel": "canonical",
                        "href": "http://oeo.net/openeo/1.0.0/jobs/07024ee9-7847-4b8a-b260-6c879a2b3cdc/results/TXIuVGVzdA%3D%3D/9fea29cd94195399cc4d902388a3c32c?expires=2234",
                        "type": "application/json",
                    },
                    {
                        "rel": "card4l-document",
                        "href": "http://ceos.org/ard/files/PFS/SR/v5.0/CARD4L_Product_Family_Specification_Surface_Reflectance-v5.0.pdf",
                        "type": "application/pdf",
                    },
                ],
                "properties": {
                    "created": "2017-01-01T09:32:12Z",
                    "datetime": None,
                    "card4l:processing_chain": {
                        "process_graph": {"foo": {"process_id": "foo", "arguments": {}}}
                    },
                    "card4l:specification": "SR",
                    "card4l:specification_version": "5.0",
                    "processing:facility": "VITO - SPARK",
                    "processing:software": "openeo-geotrellis-0.0.1",
                },
                "stac_extensions": [
                    "processing",
                    "card4l-eo",
                    "https://stac-extensions.github.io/file/v1.0.0/schema.json",
                    "eo",
                ],
                "stac_version": "0.9.0",
                "type": "Feature",
            }

    @pytest.mark.skip("not implemented yet")
    @mock.patch("time.time", mock.MagicMock(return_value=1234))
    def test_get_job_results_signed_with_expiration_110(self, api110, flask_app):
        app_config = {
            "SIGNED_URL": "TRUE",
            "SIGNED_URL_SECRET": "123&@#",
            "SIGNED_URL_EXPIRATION": "1000",
        }
        with (
            mock.patch.dict(flask_app.config, app_config),
            self._fresh_job_registry(next_job_id="job-373"),
        ):
            dummy_backend.DummyBatchJobs._update_status(
                job_id="07024ee9-7847-4b8a-b260-6c879a2b3cdc",
                user_id=TEST_USER,
                status="finished",
            )
            resp = api110.get(
                "/jobs/53c71345-09b4-46b4-b6b0-03fd6fe1f199/results",
                headers=self.AUTH_HEADER,
            )
            assert resp.assert_status_code(200).json == {
                "type": "Collection",
                "stac_version": "1.0.0",
                "stac_extensions": [
                    "eo",
                    "file",
                    "https://stac-extensions.github.io/ml-model/v1.0.0/schema.json",
                ],
                "id": "53c71345-09b4-46b4-b6b0-03fd6fe1f199",
                "title": "Your title here.",
                "description": "Your description here.",
                "license": "proprietary",
                "extent": {
                    "spatial": {"bbox": [[-180, -90, 180, 90]]},
                    "temporal": {
                        "interval": [["1981-04-24T03:00:00Z", "1981-04-24T03:00:00Z"]]
                    },
                },
                "links": [
                    {
                        "rel": "self",
                        "href": "http://oeo.net/openeo/1.1.0/jobs/53c71345-09b4-46b4-b6b0-03fd6fe1f199/results",
                        "type": "application/json",
                    },
                    {
                        "rel": "canonical",
                        "href": "http://oeo.net/openeo/1.1.0/jobs/53c71345-09b4-46b4-b6b0-03fd6fe1f199/results/TXIuVGVzdA%3D%3D/748b8c91160fbb6e137c91d7d33b0c4a?expires=2234",
                        "type": "application/json",
                    },
                    {
                        "rel": "card4l-document",
                        "href": "http://ceos.org/ard/files/PFS/SR/v5.0/CARD4L_Product_Family_Specification_Surface_Reflectance-v5.0.pdf",
                        "type": "application/pdf",
                    },
                    {
                        "rel": "item",
                        "href": "http://oeo.net/openeo/1.1.0/jobs/53c71345-09b4-46b4-b6b0-03fd6fe1f199/results/items/TXIuVGVzdA%3D%3D/f5d336336d36e3e987ba6a34b87cde01/output.tiff?expires=2234",
                        "type": "application/geo+json",
                    },
                    {
                        "href": "http://oeo.net/openeo/1.1.0/jobs/53c71345-09b4-46b4-b6b0-03fd6fe1f199/results/items/TXIuVGVzdA%3D%3D/30fac5af7fe96123c923e94c2732f9aa/ml_model_metadata.json?expires=2234",
                        "rel": "item",
                        "type": "application/json",
                    },
                ],
                "summaries": {
                    "ml-model:architecture": ["random-forest"],
                    "ml-model:learning_approach": ["supervised"],
                    "ml-model:prediction_type": ["classification"],
                },
                "assets": {
                    "output.tiff": {
                        "title": "output.tiff",
                        "href": "http://oeo.net/openeo/1.1.0/jobs/53c71345-09b4-46b4-b6b0-03fd6fe1f199/results/assets/TXIuVGVzdA%3D%3D/f5d336336d36e3e987ba6a34b87cde01/output.tiff?expires=2234",
                        "type": "image/tiff; application=geotiff",
                        "eo:bands": [{"center_wavelength": 1.23, "name": "NDVI"}],
                        "file:nodata": [123],
                        "roles": ["data"],
                    },
                    "randomforest.model": {
                        "href": "http://oeo.net/openeo/1.1.0/jobs/53c71345-09b4-46b4-b6b0-03fd6fe1f199/results/assets/TXIuVGVzdA%3D%3D/18fd2346c52945c0caba7b13246f5a63/randomforest.model?expires=2234",
                        "roles": ["data"],
                        "title": "randomforest.model",
                        "type": "application/octet-stream",
                    },
                },
            }

    @pytest.mark.skip("not implemented yet")
    def test_get_job_results_invalid_job(self, api):
        api.get("/jobs/deadbeef-f00/results", headers=self.AUTH_HEADER).assert_error(
            404, "JobNotFound"
        )

    @pytest.mark.skip("not implemented yet")
    def test_download_result_invalid_job(self, api):
        api.get(
            "/jobs/deadbeef-f00/results/assets/some_file", headers=self.AUTH_HEADER
        ).assert_error(404, "JobNotFound")

    @pytest.mark.skip("not implemented yet")
    def test_download_result(self, api, tmp_path):
        output_root = Path(tmp_path)
        jobs = {"07024ee9-7847-4b8a-b260-6c879a2b3cdc": {"status": "finished"}}
        with self._fresh_job_registry(output_root=output_root, jobs=jobs):
            output = (
                output_root / "07024ee9-7847-4b8a-b260-6c879a2b3cdc" / "output.tiff"
            )
            output.parent.mkdir(parents=True)
            with output.open("wb") as f:
                f.write(b"tiffdata")
            resp = api.get(
                "/jobs/07024ee9-7847-4b8a-b260-6c879a2b3cdc/results/assets/output.tiff",
                headers=self.AUTH_HEADER,
            )
        assert resp.assert_status_code(200).data == b"tiffdata"
        assert resp.headers["Content-Type"] == "image/tiff; application=geotiff"

    @pytest.mark.skip("not implemented yet")
    def test_download_result_with_s3_object_storage(self, api, mock_s3_resource):
        job_id = "07024ee9-7847-4b8a-b260-6c879a2b3cdc"
        s3_bucket_name = "openeo-test-bucket"
        output_root = f"s3://{s3_bucket_name}/some-data-dir"
        s3_key = f"some-data-dir/{job_id}/output.tiff"

        # Simulate that we have a large file so we would need to stream the download in chunks.
        # A size that is at least one byte larger than the STREAM_CHUNK_SIZE_DEFAULT should trigger streaming.
        # We should still receive the entire file contents in streaming mode.
        large_tiff_data = b"tiffdata" * STREAM_CHUNK_SIZE_DEFAULT

        jobs = {job_id: {"status": "finished"}}
        with self._fresh_job_registry(output_root=output_root, jobs=jobs):
            s3_bucket = create_s3_bucket(mock_s3_resource, s3_bucket_name)
            s3_bucket.put_object(Key=s3_key, Body=large_tiff_data)
            resp = api.get(
                f"/jobs/{job_id}/results/assets/output.tiff", headers=self.AUTH_HEADER
            )

        assert resp.assert_status_code(200).data == large_tiff_data
        assert resp.headers["Content-Type"] == "image/tiff; application=geotiff"

    @pytest.mark.skip("not implemented yet")
    def test_download_result_signed(self, api, tmp_path, flask_app):
        output_root = Path(tmp_path)
        app_config = {"SIGNED_URL": "TRUE", "SIGNED_URL_SECRET": "123&@#"}
        jobs = {"07024ee9-7847-4b8a-b260-6c879a2b3cdc": {"status": "finished"}}
        with (
            mock.patch.dict(flask_app.config, app_config),
            self._fresh_job_registry(output_root=output_root, jobs=jobs),
        ):
            output = (
                output_root / "07024ee9-7847-4b8a-b260-6c879a2b3cdc" / "output.tiff"
            )
            output.parent.mkdir(parents=True)
            with output.open("wb") as f:
                f.write(b"tiffdata")
            resp = api.get(
                "/jobs/07024ee9-7847-4b8a-b260-6c879a2b3cdc/results/assets/TXIuVGVzdA%3D%3D/50afb0cad129e61d415278c4ffcd8a83/output.tiff"
            )
        assert resp.assert_status_code(200).data == b"tiffdata"
        assert resp.headers["Content-Type"] == "image/tiff; application=geotiff"

    @pytest.mark.skip("not implemented yet")
    def test_download_result_signed_invalid(self, api, flask_app):
        app_config = {"SIGNED_URL": "TRUE", "SIGNED_URL_SECRET": "123&@#"}
        jobs = {"07024ee9-7847-4b8a-b260-6c879a2b3cdc": {"status": "finished"}}
        with mock.patch.dict(flask_app.config, app_config), self._fresh_job_registry(
            jobs=jobs
        ):
            resp = api.get(
                "/jobs/07024ee9-7847-4b8a-b260-6c879a2b3cdc/results/assets/TXIuVGVzdA%3D%3D/test123/output.tiff"
            )
        assert resp.assert_error(403, "CredentialsInvalid")

    @pytest.mark.skip("not implemented yet")
    @mock.patch("time.time", mock.MagicMock(return_value=1234))
    def test_download_result_signed_with_expiration(self, api, tmp_path, flask_app):
        output_root = Path(tmp_path)
        app_config = {
            "SIGNED_URL": "TRUE",
            "SIGNED_URL_SECRET": "123&@#",
            "SIGNED_URL_EXPIRATION": "1000",
        }
        jobs = {"07024ee9-7847-4b8a-b260-6c879a2b3cdc": {"status": "finished"}}
        with (
            mock.patch.dict(flask_app.config, app_config),
            self._fresh_job_registry(output_root=output_root, jobs=jobs),
        ):
            output = (
                output_root / "07024ee9-7847-4b8a-b260-6c879a2b3cdc" / "output.tiff"
            )
            output.parent.mkdir(parents=True)
            with output.open("wb") as f:
                f.write(b"tiffdata")
            resp = api.get(
                "/jobs/07024ee9-7847-4b8a-b260-6c879a2b3cdc/results/assets/TXIuVGVzdA%3D%3D/fd0ca65e29c6d223da05b2e73a875683/output.tiff?expires=2234"
            )
        assert resp.assert_status_code(200).data == b"tiffdata"
        assert resp.headers["Content-Type"] == "image/tiff; application=geotiff"

    @pytest.mark.skip("not implemented yet")
    @mock.patch("time.time", mock.MagicMock(return_value=1234))
    def test_download_result_signed_with_expiration_supports_range_request(
        self, api, tmp_path, flask_app
    ):
        output_root = Path(tmp_path)
        app_config = {
            "SIGNED_URL": "TRUE",
            "SIGNED_URL_SECRET": "123&@#",
            "SIGNED_URL_EXPIRATION": "1000",
        }
        jobs = {"07024ee9-7847-4b8a-b260-6c879a2b3cdc": {"status": "finished"}}
        with (
            mock.patch.dict(flask_app.config, app_config),
            self._fresh_job_registry(output_root=output_root, jobs=jobs),
        ):
            output = (
                output_root / "07024ee9-7847-4b8a-b260-6c879a2b3cdc" / "output.tiff"
            )
            output.parent.mkdir(parents=True)
            with output.open("wb") as f:
                f.write(b"tiffdata")

            head_resp = api.head(
                "/jobs/07024ee9-7847-4b8a-b260-6c879a2b3cdc/results/assets/TXIuVGVzdA%3D%3D/fd0ca65e29c6d223da05b2e73a875683/output.tiff?expires=2234"
            )
            assert head_resp.assert_status_code(200).data == b""
            assert (
                head_resp.headers["Content-Type"] == "image/tiff; application=geotiff"
            )
            assert head_resp.headers["Accept-Ranges"] == "bytes"

            get_resp = api.get(
                "/jobs/07024ee9-7847-4b8a-b260-6c879a2b3cdc/results/assets/TXIuVGVzdA%3D%3D/fd0ca65e29c6d223da05b2e73a875683/output.tiff?expires=2234",
                headers={"Range": "bytes=0-3"},
            )
            assert get_resp.assert_status_code(206).data == b"tiff"

    @pytest.mark.skip("not implemented yet")
    @mock.patch("time.time", mock.MagicMock(return_value=3456))
    def test_download_result_signed_with_expiration_invalid(
        self, api, tmp_path, flask_app
    ):
        app_config = {
            "SIGNED_URL": "TRUE",
            "SIGNED_URL_SECRET": "123&@#",
            "SIGNED_URL_EXPIRATION": "1000",
        }
        jobs = {"07024ee9-7847-4b8a-b260-6c879a2b3cdc": {"status": "finished"}}
        with mock.patch.dict(flask_app.config, app_config), self._fresh_job_registry(
            jobs=jobs
        ):
            resp = api.get(
                "/jobs/07024ee9-7847-4b8a-b260-6c879a2b3cdc/results/assets/TXIuVGVzdA%3D%3D/fd0ca65e29c6d223da05b2e73a875683/output.tiff?expires=2234"
            )
        assert resp.assert_error(410, "ResultLinkExpired")

    @pytest.mark.skip("not implemented yet")
    @mock.patch("time.time", mock.MagicMock(return_value=1234))
    def test_get_job_result_item(self, flask_app, api110):
        app_config = {
            "SIGNED_URL": "TRUE",
            "SIGNED_URL_SECRET": "123&@#",
            "SIGNED_URL_EXPIRATION": "1000",
        }
        with mock.patch.dict(flask_app.config, app_config), self._fresh_job_registry():
            resp = api110.get(
                "/jobs/53c71345-09b4-46b4-b6b0-03fd6fe1f199/results/items/output.tiff",
                headers=self.AUTH_HEADER,
            )

        assert resp.assert_status_code(200).json == {
            "type": "Feature",
            "stac_version": "0.9.0",
            "stac_extensions": ["eo", "file"],
            "id": "output.tiff",
            "geometry": None,
            "bbox": None,
            "properties": {"datetime": "1981-04-24T03:00:00Z"},
            "links": [
                {
                    "rel": "self",
                    "href": "http://oeo.net/openeo/1.1.0/jobs/53c71345-09b4-46b4-b6b0-03fd6fe1f199/results/items/output.tiff",
                    "type": "application/geo+json",
                },
                {
                    "rel": "collection",
                    "href": "http://oeo.net/openeo/1.1.0/jobs/53c71345-09b4-46b4-b6b0-03fd6fe1f199/results",
                    "type": "application/json",
                },
            ],
            "assets": {
                "output.tiff": {
                    "title": "output.tiff",
                    "href": "http://oeo.net/openeo/1.1.0/jobs/53c71345-09b4-46b4-b6b0-03fd6fe1f199/results/assets/TXIuVGVzdA%3D%3D/f5d336336d36e3e987ba6a34b87cde01/output.tiff?expires=2234",
                    "type": "image/tiff; application=geotiff",
                    "eo:bands": [{"center_wavelength": 1.23, "name": "NDVI"}],
                    "file:nodata": [123],
                    "roles": ["data"],
                }
            },
            "collection": "53c71345-09b4-46b4-b6b0-03fd6fe1f199",
        }
        assert resp.headers["Content-Type"] == "application/geo+json"

    @pytest.mark.skip("not implemented yet")
    @mock.patch("time.time", mock.MagicMock(return_value=1234))
    def test_download_ml_model_metadata(self, flask_app, api110):
        app_config = {
            "SIGNED_URL": "TRUE",
            "SIGNED_URL_SECRET": "123&@#",
            "SIGNED_URL_EXPIRATION": "1000",
        }
        with mock.patch.dict(flask_app.config, app_config), self._fresh_job_registry():
            resp = api110.get(
                "/jobs/53c71345-09b4-46b4-b6b0-03fd6fe1f199/results/items/ml_model_metadata.json",
                headers=self.AUTH_HEADER,
            )
        random_id = resp.assert_status_code(200).json["id"]
        assert resp.assert_status_code(200).json == {
            "id": random_id,
            "type": "Feature",
            "stac_version": "1.0.0",
            "stac_extensions": [
                "https://stac-extensions.github.io/ml-model/v1.0.0/schema.json"
            ],
            "assets": {
                "model": {
                    "href": "http://oeo.net/openeo/1.1.0/jobs/53c71345-09b4-46b4-b6b0-03fd6fe1f199/results/assets/randomforest.model",
                    "roles": ["ml-model:checkpoint"],
                    "title": "org.apache.spark.mllib.tree.model.RandomForestModel",
                    "type": "application/octet-stream",
                }
            },
            "bbox": [-179.999, -89.999, 179.999, 89.999],
            "collection": "53c71345-09b4-46b4-b6b0-03fd6fe1f199",
            "geometry": {
                "coordinates": [
                    [
                        [-179.999, -89.999],
                        [179.999, -89.999],
                        [179.999, 89.999],
                        [-179.999, 89.999],
                        [-179.999, -89.999],
                    ]
                ],
                "type": "Polygon",
            },
            "links": [],
            "properties": {
                "datetime": None,
                "end_datetime": "9999-12-31T23:59:59Z",
                "ml-model:architecture": "random-forest",
                "ml-model:learning_approach": "supervised",
                "ml-model:prediction_type": "classification",
                "ml-model:training-os": "linux",
                "ml-model:training-processor-type": "cpu",
                "ml-model:type": "ml-model",
                "start_datetime": "1970-01-01T00:00:00Z",
            },
        }

    @pytest.mark.skip("not implemented yet")
    def test_get_batch_job_logs(self, api):
        with self._fresh_job_registry():
            resp = api.get(
                "/jobs/07024ee9-7847-4b8a-b260-6c879a2b3cdc/logs",
                headers=self.AUTH_HEADER,
            )
        assert resp.assert_status_code(200).json == {
            "logs": [{"id": "1", "level": "info", "message": "hello world"}],
            "links": [],
        }

    @pytest.mark.skip("not implemented yet")
    def test_get_batch_job_logs_failure(self, api):
        with self._fresh_job_registry():
            with mock.patch.dict(
                dummy_backend.DummyBatchJobs._custom_job_logs,
                {"07024ee9-7847-4b8a-b260-6c879a2b3cdc": [RuntimeError("nope")]},
            ):
                resp = api.get(
                    "/jobs/07024ee9-7847-4b8a-b260-6c879a2b3cdc/logs",
                    headers=self.AUTH_HEADER,
                )
                assert resp.assert_status_code(200).json == {
                    "logs": [
                        {
                            "id": "-1",
                            "code": "Internal",
                            "level": "error",
                            "message": "Log collection failed: RuntimeError('nope')",
                        }
                    ],
                    "links": [],
                }

    @pytest.mark.skip("not implemented yet")
    def test_cancel_job(self, api):
        with self._fresh_job_registry():
            resp = api.delete(
                "/jobs/07024ee9-7847-4b8a-b260-6c879a2b3cdc/results",
                headers=self.AUTH_HEADER,
            )
        assert resp.status_code == 204

    @pytest.mark.skip("not implemented yet")
    def test_cancel_job_invalid(self, api):
        with self._fresh_job_registry():
            resp = api.delete("/jobs/deadbeef-f00/results", headers=self.AUTH_HEADER)
        resp.assert_error(404, "JobNotFound")

    @pytest.mark.skip("not implemented yet")
    def test_delete_job(self, api):
        with self._fresh_job_registry():
            resp = api.delete(
                "/jobs/07024ee9-7847-4b8a-b260-6c879a2b3cdc", headers=self.AUTH_HEADER
            )
        assert resp.status_code == 204


class TestSecondaryServices:
    AUTH_HEADER = TEST_USER_AUTH_HEADER

    def test_service_types_v100(self, api):
        resp = api.get("/service_types").assert_status_code(200)
        service_types = resp.json
        assert list(service_types.keys()) == ["WMTS"]
        wmts = service_types["WMTS"]
        assert wmts["configuration"]["version"]["default"] == "1.0.0"
        assert wmts["process_parameters"] == []
        assert wmts["links"] == []

    def test_create_unsupported_service_type_returns_400_BadRequest(self, api):
        resp = api.post(
            "/services",
            json={
                "process": {"process_graph": {"product_id": "S2"}},
                "type": "???",
            },
            headers=self.AUTH_HEADER,
        )
        resp.assert_status_code(400)

    def test_unsupported_services_methods_return_405_MethodNotAllowed(self, api):
        res = api.put(
            "/services",
            json={
                "process": {"process_graph": {"product_id": "S2"}},
                "type": "WMTS",
            },
        )
        res.assert_status_code(405)

    @pytest.mark.skip("v0.4.0 is no longer supported")
    def test_list_services_040(self, api040):
        metadata = api040.get("/services", headers=self.AUTH_HEADER).json
        assert metadata == {
            "services": [
                {
                    "id": "wmts-foo",
                    "type": "WMTS",
                    "enabled": True,
                    "url": "https://oeo.net/wmts/foo",
                    "submitted": "2020-04-09T15:05:08Z",
                    "title": "Test service",
                    "parameters": {"version": "0.5.8"},
                }
            ],
            "links": [],
        }

    def test_list_services_100(self, api100):
        metadata = api100.get("/services", headers=self.AUTH_HEADER).json
        assert metadata == {
            "services": [
                {
                    "id": "wmts-foo",
                    "type": "WMTS",
                    "enabled": True,
                    "url": "https://oeo.net/wmts/foo",
                    "title": "Test service",
                    "created": "2020-04-09T15:05:08Z",
                    "configuration": {"version": "0.5.8"},
                }
            ],
            "links": [],
        }

    @pytest.mark.skip("v0.4.0 is no longer supported")
    def test_get_service_metadata_040(self, api040):
        metadata = api040.get("/services/wmts-foo", headers=self.AUTH_HEADER).json
        assert metadata == {
            "id": "wmts-foo",
            "process_graph": {"foo": {"process_id": "foo", "arguments": {}}},
            "url": "https://oeo.net/wmts/foo",
            "type": "WMTS",
            "enabled": True,
            "parameters": {"version": "0.5.8"},
            "attributes": {},
            "title": "Test service",
            "submitted": "2020-04-09T15:05:08Z",
        }

    def test_get_service_metadata_100(self, api100):
        metadata = api100.get("/services/wmts-foo", headers=self.AUTH_HEADER).json
        assert metadata == {
            "id": "wmts-foo",
            "process": {
                "process_graph": {"foo": {"process_id": "foo", "arguments": {}}}
            },
            "url": "https://oeo.net/wmts/foo",
            "type": "WMTS",
            "enabled": True,
            "configuration": {"version": "0.5.8"},
            "attributes": {},
            "title": "Test service",
            "created": "2020-04-09T15:05:08Z",
        }

    def test_get_service_metadata_wrong_id(self, api):
        api.get("/services/wmts-invalid", headers=self.AUTH_HEADER).assert_error(
            404, "ServiceNotFound"
        )

    def test_services_requires_authentication(self, api):
        api.get("/services").assert_error(401, "AuthenticationRequired")

    def test_get_service_requires_authentication(self, api):
        api.get("/services/wmts-foo").assert_error(401, "AuthenticationRequired")

    def test_patch_service_requires_authentication(self, api):
        api.patch("/services/wmts-foo").assert_error(401, "AuthenticationRequired")

    def test_delete_service_requires_authentication(self, api):
        api.delete("/services/wmts-foo").assert_error(401, "AuthenticationRequired")

    def test_service_logs_100(self, api):
        logs = api.get("/services/wmts-foo/logs", headers=self.AUTH_HEADER).json
        assert logs == {
            "logs": [
                {"id": 3, "level": "info", "message": "Loaded data."},
            ],
            "links": [],
        }


def test_credentials_basic_no_headers(api):
    api.get("/credentials/basic").assert_error(401, "AuthenticationRequired")


@pytest.mark.skip("Warning! auth has not been implemented")
def test_credentials_basic_wrong_password(api):
    headers = {
        "Authorization": build_basic_http_auth_header(
            username="john", password="password123"
        )
    }
    api.get("/credentials/basic", headers=headers).assert_error(
        403, "CredentialsInvalid"
    )


@pytest.mark.skip("Warning! auth has not been implemented")
def test_credentials_basic(api):
    headers = {
        "Authorization": build_basic_http_auth_header(
            username="Alice", password="alice123"
        )
    }
    resp = api.get("/credentials/basic", headers=headers)
    response = resp.assert_status_code(200).json
    expected = {"access_token"}
    if api.api_version_compare.below("1.0.0"):
        expected.add("user_id")
    assert set(response.keys()) == expected


def test_endpoint_registry():
    app = flask.Flask(__name__)
    bp = flask.Blueprint("test", __name__)
    endpoint = EndpointRegistry()

    @bp.route("/hello")
    def hello():
        return "not an endpoint"

    @endpoint
    @bp.route("/foo")
    def foo():
        return "simple endpoint"

    @endpoint(hidden=True)
    @bp.route("/secret")
    def secret():
        return "secret endpoint"

    @endpoint(version=ComparableVersion("1.0.0").accept_lower)
    @bp.route("/old")
    def old():
        return "old endpoint"

    app.register_blueprint(bp, url_prefix="/bar")

    result = endpoint.get_path_metadata(bp)

    # Check metadata
    assert len(result) == 3
    paths, methods, metadatas = zip(*sorted(result))
    assert paths == ("/foo", "/old", "/secret")
    assert methods == ({"GET"},) * 3
    assert metadatas[0].hidden is False
    assert metadatas[0].for_version is None
    assert metadatas[1].for_version("0.4.0") is True
    assert metadatas[1].for_version("1.0.0") is False
    assert metadatas[2].hidden is True

    # Check that view functions still work
    client = app.test_client()
    assert b"not an endpoint" == client.get("/bar/hello").data
    assert b"simple endpoint" == client.get("/bar/foo").data
    assert b"secret endpoint" == client.get("/bar/secret").data
    assert b"old endpoint" == client.get("/bar/old").data


def test_endpoint_registry_multiple_methods():
    bp = flask.Blueprint("test", __name__)
    endpoint = EndpointRegistry()

    @endpoint
    @bp.route("/foo", methods=["GET"])
    def foo_get():
        return "get"

    @endpoint
    @bp.route("/foo", methods=["POST"])
    def foo_post():
        return "post"

    result = endpoint.get_path_metadata(bp)

    # Check metadata
    assert len(result) == 2
    paths, methods, metadatas = zip(*sorted(result))
    assert paths == ("/foo", "/foo")
    assert methods == ({"GET"}, {"POST"})


@pytest.fixture
def udp_store(backend_implementation) -> dummy_backend.DummyUserDefinedProcesses:
    udps = backend_implementation.user_defined_processes
    assert isinstance(udps, dummy_backend.DummyUserDefinedProcesses)
    udps.reset(
        {
            ("Mr.Test", "udp1"): UserDefinedProcessMetadata(
                id="udp1",
                process_graph={
                    "add23": {
                        "process_id": "add",
                        "arguments": {"x": 2, "y": 3},
                        "result": True,
                    }
                },
            ),
            ("Mr.Test", "udp2"): UserDefinedProcessMetadata(
                id="udp2",
                process_graph={
                    "add58": {
                        "process_id": "add",
                        "arguments": {"x": 5, "y": 8},
                        "result": True,
                    }
                },
                public=True,
            ),
        }
    )
    return udps


class TestUserDefinedProcesses:
    @pytest.mark.skip("not implemented")
    @pytest.mark.parametrize("body_id", [None, "evi", "meh"])
    def test_add_udp(self, api100, udp_store, body_id):
        spec = {
            "parameters": [{"name": "red"}],
            "process_graph": {"sub": {}},
            "public": True,
        }
        if body_id:
            spec["id"] = body_id
        api100.put(
            "/process_graphs/evi", headers=TEST_USER_AUTH_HEADER, json=spec
        ).assert_status_code(200)

        new_udp = udp_store._processes["Mr.Test", "evi"]
        assert new_udp.id == "evi"
        assert new_udp.parameters == [{"name": "red"}]
        assert new_udp.process_graph == {"sub": {}}
        assert new_udp.public

    def test_add_udp_no_pg(self, api100):
        spec = {"id": "evi", "parameters": [{"name": "red"}]}
        res = api100.put(
            "/process_graphs/evi", headers=TEST_USER_AUTH_HEADER, json=spec
        )
        res.assert_error(400, "ProcessGraphMissing")

    def test_add_udp_invalid_id(self, api100):
        spec = {"id": "foob@r", "process_graph": {"sub": {}}}
        res = api100.put(
            "/process_graphs/foob@r", headers=TEST_USER_AUTH_HEADER, json=spec
        )
        res.assert_error(400, "InvalidId")

    @pytest.mark.skip("not implemented")
    @pytest.mark.parametrize("body_id", [None, "udp1", "meh"])
    def test_update_udp(self, api100, udp_store, body_id):
        spec = {
            "parameters": [{"name": "blue"}],
            "process_graph": {"add": {}},
            "public": True,
        }
        if body_id:
            spec["id"] = body_id
        api100.put(
            "/process_graphs/udp1", headers=TEST_USER_AUTH_HEADER, json=spec
        ).assert_status_code(200)

        modified_udp = udp_store._processes["Mr.Test", "udp1"]
        assert modified_udp.id == "udp1"
        assert modified_udp.process_graph == {"add": {}}
        assert modified_udp.parameters == [{"name": "blue"}]
        assert modified_udp.public

    @pytest.mark.skip("not implemented")
    def test_list_udps(self, api100, udp_store):
        resp = api100.get(
            "/process_graphs", headers=TEST_USER_AUTH_HEADER
        ).assert_status_code(200)

        udps = resp.json["processes"]
        assert udps == [{"id": "udp1"}, {"id": "udp2"}]

    @pytest.mark.skip("not implemented")
    def test_get_udp(self, api100, udp_store):
        udp = (
            api100.get("/process_graphs/udp1", headers=TEST_USER_AUTH_HEADER)
            .assert_status_code(200)
            .json
        )
        assert udp == {
            "id": "udp1",
            "process_graph": {
                "add23": {
                    "process_id": "add",
                    "arguments": {"x": 2, "y": 3},
                    "result": True,
                }
            },
            "public": False,
        }

    @pytest.mark.skip("not implemented")
    def test_get_udp_public(self, api100, udp_store):
        udp = (
            api100.get("/process_graphs/udp2", headers=TEST_USER_AUTH_HEADER)
            .assert_status_code(200)
            .json
        )
        assert udp == {
            "id": "udp2",
            "process_graph": {
                "add58": {
                    "process_id": "add",
                    "arguments": {"x": 5, "y": 8},
                    "result": True,
                }
            },
            "links": [
                {
                    "rel": "canonical",
                    "href": "http://oeo.net/openeo/1.0.0/processes/u:Mr.Test/udp2",
                    "title": "Public URL for user-defined process 'udp2'",
                }
            ],
            "public": True,
        }

    @pytest.mark.skip("not implemented")
    def test_get_unknown_udp(self, api100, udp_store):
        res = api100.get("/process_graphs/unknown", headers=TEST_USER_AUTH_HEADER)
        res.assert_error(404, "ProcessGraphNotFound")

    @pytest.mark.skip("not implemented")
    def test_get_invalid_id(self, api100, udp_store):
        res = api100.get("/process_graphs/foob@r", headers=TEST_USER_AUTH_HEADER)
        res.assert_error(400, "InvalidId")

    @pytest.mark.skip("not implemented")
    def test_delete_udp(self, api100, udp_store):
        assert ("Mr.Test", "udp2") in udp_store._processes

        api100.delete(
            "/process_graphs/udp2", headers=TEST_USER_AUTH_HEADER
        ).assert_status_code(204)

        assert ("Mr.Test", "udp1") in udp_store._processes
        assert ("Mr.Test", "udp2") not in udp_store._processes

    @pytest.mark.skip("not implemented")
    def test_delete_unknown_udp(self, api100, udp_store):
        res = api100.delete("/process_graphs/unknown", headers=TEST_USER_AUTH_HEADER)
        res.assert_error(404, "ProcessGraphNotFound")

    @pytest.mark.skip("not implemented")
    def test_delete_invalid_id(self, api100, udp_store):
        res = api100.delete("/process_graphs/foob@r", headers=TEST_USER_AUTH_HEADER)
        res.assert_error(400, "InvalidId")

    @pytest.mark.skip("not implemented")
    def test_public_udp(self, api100, udp_store):
        api100.put(
            "/process_graphs/evi",
            headers=TEST_USER_AUTH_HEADER,
            json={
                "parameters": [{"name": "red"}],
                "process_graph": {"sub": {}},
                "links": [{"rel": "about", "href": "https://wikipedia.test/evi"}],
                "public": True,
            },
        ).assert_status_code(200)
        api100.put(
            "/process_graphs/secret",
            headers=TEST_USER_AUTH_HEADER,
            json={
                "parameters": [{"name": "red"}],
                "process_graph": {"sub": {}},
                "public": False,
            },
        ).assert_status_code(200)

        r = api100.get("/processes/u:Mr.Test").assert_status_code(200)
        assert r.json == {
            "processes": [
                {"id": "udp2"},
                {"id": "evi", "parameters": [{"name": "red"}]},
            ],
            "links": [],
        }

    @pytest.mark.skip("not implemented")
    def test_public_udp_link(self, api100, udp_store):
        api100.put(
            "/process_graphs/evi",
            headers=TEST_USER_AUTH_HEADER,
            json={
                "parameters": [{"name": "red"}],
                "process_graph": {
                    "add35": {
                        "process_id": "add",
                        "arguments": {"x": 3, "y": 5},
                        "result": True,
                    }
                },
                "links": [{"rel": "about", "href": "https://wikipedia.test/evi"}],
                "public": True,
            },
        ).assert_status_code(200)

        expected = {
            "id": "evi",
            "parameters": [{"name": "red"}],
            "process_graph": {
                "add35": {
                    "process_id": "add",
                    "arguments": {"x": 3, "y": 5},
                    "result": True,
                }
            },
            "public": True,
            "links": [
                {"rel": "about", "href": "https://wikipedia.test/evi"},
                {
                    "rel": "canonical",
                    "href": "http://oeo.net/openeo/1.0.0/processes/u:Mr.Test/evi",
                    "title": "Public URL for user-defined process 'evi'",
                },
            ],
        }

        api100.get("/process_graphs/evi").assert_error(401, "AuthenticationRequired")
        udp = (
            api100.get("/process_graphs/evi", headers=TEST_USER_AUTH_HEADER)
            .assert_status_code(200)
            .json
        )
        assert udp == expected

        udp = api100.get("/processes/u:Mr.Test/evi").assert_status_code(200).json
        assert udp == expected

    @pytest.mark.skip("not implemented")
    @pytest.mark.parametrize("body_id", [None, "evi", "meh"])
    def test_add_and_get_udp(self, api100, udp_store, body_id):
        spec = {
            "parameters": [{"name": "red"}],
            "returns": {"schema": {"type": "number"}},
            "process_graph": {"sub": {}},
            "summary": "evify it",
            "description": "Calculate the EVI",
            "public": True,
        }
        if body_id:
            spec["id"] = body_id
        api100.put(
            "/process_graphs/evi", headers=TEST_USER_AUTH_HEADER, json=spec
        ).assert_status_code(200)

        udp = udp_store._processes["Mr.Test", "evi"]
        assert udp.id == "evi"
        assert udp.parameters == [{"name": "red"}]
        assert udp.returns == {"schema": {"type": "number"}}
        assert udp.process_graph == {"sub": {}}
        assert udp.summary == "evify it"
        assert udp.description == "Calculate the EVI"
        assert udp.public is True

        udp = (
            api100.get("/process_graphs/evi", headers=TEST_USER_AUTH_HEADER)
            .assert_status_code(200)
            .json
        )
        assert udp["id"] == "evi"
        assert udp["parameters"] == [{"name": "red"}]
        assert udp["returns"] == {"schema": {"type": "number"}}
        assert udp["process_graph"] == {"sub": {}}
        assert udp["summary"] == "evify it"
        assert udp["description"] == "Calculate the EVI"
        assert udp["public"] is True


def test_debug_echo_get(api):
    res = (
        api.get("/_debug/echo?xev=lol", headers={"foo": "bar"})
        .assert_status_code(200)
        .json
    )
    assert res["method"] == "GET"
    assert res["args"] == {"xev": "lol"}
    assert res["data"] == "b''"
    assert res["headers"]["Foo"] == "bar"


def test_debug_echo_post(api):
    res = api.post("/_debug/echo", json={"foo": "bar"}).assert_status_code(200).json
    assert res["method"] == "POST"
    assert res["data"] == 'b\'{"foo": "bar"}\''
