import openeo

# Connect to the backend
connection = openeo.connect("https://open-id-connect-protected-openeo.endpoint")

# Authenticate using OpenID Connect
connection.authenticate_oidc(
    client_id="your-client-id",
    client_secret="your-client-secret",
    provider_id="https://your-oidc-provider.url"
)
