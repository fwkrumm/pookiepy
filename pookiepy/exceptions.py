"""
Custom exception hierarchy used across the gRPC framework.
"""
import queue


class GrpcTimeoutError(TimeoutError):
    """custom TimeoutError for gRPC operations."""

class GrpcConnectionError(ConnectionError):
    """custom ConnectionError for gRPC connection issues."""

class GrpcResourceExhaustedError(Exception):
    """custom exception for gRPC resource exhaustion if auto chunking is disabled."""
class GrpcServerNoAnswerReceivedError(Exception):
    """custom exception for empty gRPC answer from server."""

class GrpcValueError(ValueError):
    """custom ValueError for gRPC related value errors."""

class ClientExit(Exception):
    """custom SystemExit for server exit."""

class StopSpin(Exception):
    """Signal that BaseClient.spin_forever should stop processing messages."""

class GrpcEmpty(queue.Empty):
    """custom Empty exception for gRPC client queues."""

class GrpcCustomInterfaceError(GrpcValueError):
    """Raised when runtime custom proto schema is incompatible with helper utilities."""
