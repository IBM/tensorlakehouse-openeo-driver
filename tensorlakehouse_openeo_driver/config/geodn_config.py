import os
from typing import Callable, Dict, List, Optional, Tuple

import attrs

from openeo_driver.server import build_backend_deploy_metadata
from openeo_driver.urlsigning import UrlSigner
from openeo_driver.users.oidc import OidcProvider


class ConfigException(ValueError):
    pass


@attrs.frozen(
    # Note: `kw_only=True` enforces "kwargs" based construction (which is good for readability/maintainability)
    # and allows defining mandatory fields (fields without default) after optional fields.
    kw_only=True
)
class OpenEoBackendConfig:
    """
    Configuration for openEO backend.
    """

    # identifier for this config
    id: Optional[str] = "0.0.1"

    # Generic indicator describing the environment the code is deployed in
    # (e.g. "prod", "dev", "staging", "test", "integration", ...)
    deploy_env: str = os.environ.get("OPENEO_DEPLOY_ENV") or os.environ.get("OPENEO_ENV") or "dev"

    capabilities_service_id: Optional[str] = None
    capabilities_title: str = "GeoDN openEO Backend"
    capabilities_description: str = "This is the GeoDN openEO Backend"
    capabilities_backend_version: str = "0.0.1"
    capabilities_deploy_metadata: dict = attrs.Factory(
        lambda: build_backend_deploy_metadata(packages=["openeo", "openeo_driver"])
    )

    processing_facility: str = "openEO"
    processing_software: str = "openeo-python-driver"

    # TODO: merge `enable_basic_auth` and `valid_basic_auth` into a single config field.
    enable_basic_auth: bool = True
    # `valid_basic_auth`: function that takes a username and password and returns a boolean indicating if password is correct.
    valid_basic_auth: Optional[Callable[[str, str], bool]] = None

    enable_oidc_auth: bool = True

    oidc_providers: List[OidcProvider] = attrs.Factory(list)

    oidc_token_introspection: bool = False

    # Mapping of `(oidc_provider id, token_sub)`
    # to user info, as a dictionary with at least a "user_id" field.
    # `token_sub` is the  OIDC token "sub" field, which usually identifies a user,
    # but could also identify a OIDC client authenticated through the client credentials grant.
    # TODO: allow it to be a callable instead of a dictionary?
    oidc_user_map: Dict[Tuple[str, str], dict] = attrs.Factory(dict)

    # General Flask related settings
    # (e.g. see https://flask.palletsprojects.com/en/2.3.x/config/#builtin-configuration-values)
    flask_settings: dict = attrs.Factory(
        lambda: {
            "MAX_CONTENT_LENGTH": 2 * 1024 * 1024,  # bytes
        }
    )

    url_signer: Optional[UrlSigner] = None

    collection_exclusion_list: Dict[str, List[str]] = {}  # e.g. {"1.1.0":["my_collection_id"]}
    processes_exclusion_list: Dict[str, List[str]] = {}  # e.g. {"1.1.0":["my_process_id"]}
