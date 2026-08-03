"""SSL certificate handling test --- clients.

Scenario:
1. Client uses TLS without trusted root cert -> must fail.
2. Client uses matching root cert -> must connect and complete.
"""

import sys

import grpc

from pookiepy.baseclient import BaseClient, ClientConfig
from pookiepy.exceptions import GrpcConnectionError
from pookiepy.tools import generate_message
from tests.integration._interface import get_args

SERVER_NAME_OVERRIDE = "waterzooi.test.google.be"

# Must match server_ssl_certificate_handling.py cert material.
DUMMY_CERT_PEM = b"""-----BEGIN CERTIFICATE-----
MIIDtDCCApygAwIBAgIUbJfTREJ6k6/+oInWhV1O1j3ZT0IwDQYJKoZIhvcNAQEL
BQAwVjELMAkGA1UEBhMCQVUxEzARBgNVBAgMClNvbWUtU3RhdGUxITAfBgNVBAoM
GEludGVybmV0IFdpZGdpdHMgUHR5IEx0ZDEPMA0GA1UEAwwGdGVzdGNhMB4XDTIw
MDMxODAzMTA0MloXDTMwMDMxNjAzMTA0MlowZTELMAkGA1UEBhMCVVMxETAPBgNV
BAgMCElsbGlub2lzMRAwDgYDVQQHDAdDaGljYWdvMRUwEwYDVQQKDAxFeGFtcGxl
LCBDby4xGjAYBgNVBAMMESoudGVzdC5nb29nbGUuY29tMIIBIjANBgkqhkiG9w0B
AQEFAAOCAQ8AMIIBCgKCAQEA5xOONxJJ8b8Qauvob5/7dPYZfIcd+uhAWL2ZlTPz
Qvu4oF0QI4iYgP5iGgry9zEtCM+YQS8UhiAlPlqa6ANxgiBSEyMHH/xE8lo/+caY
GeACqy640Jpl/JocFGo3xd1L8DCawjlaj6eu7T7T/tpAV2qq13b5710eNRbCAfFe
8yALiGQemx0IYhlZXNbIGWLBNhBhvVjJh7UvOqpADk4xtl8o5j0xgMIRg6WJGK6c
6ffSIg4eP1XmovNYZ9LLEJG68tF0Q/yIN43B4dt1oq4jzSdCbG4F1EiykT2TmwPV
YDi8tml6DfOCDGnit8svnMEmBv/fcPd31GSbXjF8M+KGGQIDAQABo2swaTAJBgNV
HRMEAjAAMAsGA1UdDwQEAwIF4DBPBgNVHREESDBGghAqLnRlc3QuZ29vZ2xlLmZy
ghh3YXRlcnpvb2kudGVzdC5nb29nbGUuYmWCEioudGVzdC55b3V0dWJlLmNvbYcE
wKgBAzANBgkqhkiG9w0BAQsFAAOCAQEAS8hDQA8PSgipgAml7Q3/djwQ644ghWQv
C2Kb+r30RCY1EyKNhnQnIIh/OUbBZvh0M0iYsy6xqXgfDhCB93AA6j0i5cS8fkhH
Jl4RK0tSkGQ3YNY4NzXwQP/vmUgfkw8VBAZ4Y4GKxppdATjffIW+srbAmdDruIRM
wPeikgOoRrXf0LA1fi4TqxARzeRwenQpayNfGHTvVF9aJkl8HoaMunTAdG5pIVcr
9GKi/gEMpXUJbbVv3U5frX1Wo4CFo+rZWJ/LyCMeb0jciNLxSdMwj/E/ZuExlyeZ
gc9ctPjSMvgSyXEKv6Vwobleeg88V2ZgzenziORoWj4KszG/lbQZvg==
-----END CERTIFICATE-----
"""


def build_client_config(root_certificates: bytes | None) -> ClientConfig:
    """Create client config for TLS with deterministic server-name override."""
    return ClientConfig(
        connection_check_timeout=1.5,
        ssl_credentials=grpc.ssl_channel_credentials(root_certificates=root_certificates),
        grpc_options=[
            ("grpc.keepalive_time_ms", 180000),
            ("grpc.keepalive_timeout_ms", 10000),
            ("grpc.keepalive_without_calls", True),
            ("grpc.ssl_target_name_override", SERVER_NAME_OVERRIDE),
            ("grpc.default_authority", SERVER_NAME_OVERRIDE),
        ],
    )


class SslClient(BaseClient):
    """Simple client for SSL integration flow."""

    def __init__(self, name: str, port: int, ip: str, cfg: ClientConfig):
        super().__init__(
            port,
            name=name,
            provides=["server-exit"],
            requires=[],
            ip=ip,
            config=cfg,
        )


if __name__ == "__main__":
    args = get_args("SSL certificate handling test")

    bad_client = None
    try:
        bad_client = SslClient(
            "tls_fail_missing_cert",
            args.port,
            args.ip,
            build_client_config(root_certificates=None),
        )
        assert False, "Expected GrpcConnectionError for missing trusted certificate"
    except GrpcConnectionError:
        pass
    finally:
        if bad_client is not None:
            bad_client.disconnect()

    good_client = None
    try:
        good_client = SslClient(
            "tls_ok_with_cert",
            args.port,
            args.ip,
            build_client_config(root_certificates=DUMMY_CERT_PEM),
        )
        assert good_client.server_session_id, "Expected successful TLS connection"
        assert good_client.run_event.is_set(), "Client run_event should be set after connect"

        good_client.send_data(generate_message("server-exit"))
        good_client.wait_done()
    finally:
        if good_client is not None:
            good_client.disconnect()

    assert not good_client.run_event.is_set(), "Client should be disconnected at end"
    sys.exit(0)
