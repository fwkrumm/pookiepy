"""Password Auth Test --- Server
==============================
Demonstrates simple password-based client authentication using gRPC call metadata.

.. warning::

    Passwords in gRPC metadata are transmitted as plaintext on insecure channels.
    This pattern is intentionally labelled as **NOT best practice**.
    Use TLS certificate-based authentication for production workloads.
"""
from grpchook import message_pb2
from tests.integration._interface import get_args
from tests.integration._server_base import IntegrationServer

CORRECT_PASSWORD = "s3cr3t"
PASSWORD_METADATA_KEY = "x-password"


class PasswordServer(IntegrationServer):
    """Rejects clients that omit or present the wrong ``x-password`` metadata key."""

    def on_client_connect(self, data: message_pb2.Message, context) -> bool:
        """Check the ``x-password`` gRPC call metadata before accepting the client.

        Args:
            data: First message from the connecting client (carries clientInfo).
            context: gRPC servicer context --- used to read call metadata.

        Returns:
            True if the password matches, False to reject the connection.
        """
        metadata = dict(context.invocation_metadata())
        provided = metadata.get(PASSWORD_METADATA_KEY, "")
        if provided != CORRECT_PASSWORD:
            self.logger.warning(
                "Client '%s' presented wrong/missing password --- rejecting",
                data.metaInfo.clientInfo.name,
            )
            return False
        return True


if __name__ == "__main__":
    args = get_args("Password auth test: gRPC call-metadata password check")
    s = PasswordServer(args.port)
    s.serve_forever()
