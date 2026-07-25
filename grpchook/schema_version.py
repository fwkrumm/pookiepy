"""
Schema fingerprint for proto interface compatibility verification.

The fingerprint is a SHA-256 hash of the compiled FileDescriptorProto --- the
canonical binary representation of the .proto file as seen by the Python runtime.
Both server and client compute it independently at import time; mismatches mean
the two sides were generated from different .proto files.

Usage
-----
- BaseClient sends SCHEMA_VERSION as gRPC call metadata on connect.
- BaseServer reads it from context and aborts FAILED_PRECONDITION on mismatch.

NOTE that this is not yet optimal since the dev has add some documentation for mapping
schema to version; however this is a start.
"""

import hashlib

from grpchook import message_pb2

# 16-hex-char prefix is sufficient for collision resistance in this context
SCHEMA_VERSION: str = hashlib.sha256(
    message_pb2.DESCRIPTOR.serialized_pb
).hexdigest()[:16]

# gRPC metadata key --- lowercase, no underscores (gRPC convention)
SCHEMA_VERSION_METADATA_KEY: str = "x-schema-version"
