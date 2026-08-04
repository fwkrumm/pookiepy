"""Schema version metadata key for interface compatibility checks.

Schema values are supplied explicitly through ``ClientConfig.schema_version``
and ``ServerConfig.schema_version``. The framework transports the configured
string via gRPC metadata and compares client/server values on connect.
"""

# gRPC metadata key --- lowercase, no underscores (gRPC convention)
SCHEMA_VERSION_METADATA_KEY: str = "x-schema-version"
