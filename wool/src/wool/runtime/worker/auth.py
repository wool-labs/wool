from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import grpc


@dataclass(frozen=True)
class WorkerCredentials:
    """Container for worker TLS/mTLS credentials.

    Manages certificate files and provides credentials for both server-side
    (accepting connections) and client-side (making connections) operations.
    Designed for peer-to-peer networks where workers act as both servers
    and clients.

    **Mutual TLS (recommended for worker pools):**

    .. code-block:: python

        creds = WorkerCredentials.from_files(
            ca_path="certs/ca-cert.pem",
            key_path="certs/worker-key.pem",
            cert_path="certs/worker-cert.pem",
            mutual=True,  # Default: both parties authenticate
        )

        worker = LocalWorker("my-pool", credentials=creds)

    **One-way TLS (client anonymous):**

    .. code-block:: python

        creds = WorkerCredentials.from_files(
            ca_path="certs/ca-cert.pem",
            key_path="certs/worker-key.pem",
            cert_path="certs/worker-cert.pem",
            mutual=False,  # Server authenticated, client anonymous
        )

    :param ca_cert:
        PEM-encoded CA certificate for verifying peers.
    :param worker_key:
        PEM-encoded private key for this worker.
    :param worker_cert:
        PEM-encoded certificate for this worker.
    :param mutual:
        Whether to use mutual TLS (mTLS). If True (default), both server
        and client authenticate. If False, only server is authenticated.
    """

    ca_cert: bytes
    worker_key: bytes
    worker_cert: bytes
    mutual: bool = True

    @classmethod
    def from_files(
        cls,
        ca_path: str,
        key_path: str,
        cert_path: str,
        mutual: bool = True,
    ) -> WorkerCredentials:
        """Load credentials from PEM files.

        Reads certificate files from the filesystem and creates a
        WorkerCredentials instance. All files must be PEM-encoded.

        :param ca_path:
            Path to CA certificate file.
        :param key_path:
            Path to worker private key file.
        :param cert_path:
            Path to worker certificate file.
        :param mutual:
            Whether to use mutual TLS (mTLS). If True (default), both
            server and client authenticate. If False, only server is
            authenticated and clients remain anonymous at the transport
            layer.
        :returns:
            WorkerCredentials instance with loaded certificates.
        :raises FileNotFoundError:
            If any certificate file doesn't exist.
        :raises OSError:
            If any file cannot be read.
        """
        with open(ca_path, "rb") as f:
            ca_cert = f.read()
        with open(key_path, "rb") as f:
            worker_key = f.read()
        with open(cert_path, "rb") as f:
            worker_cert = f.read()

        return cls(
            ca_cert=ca_cert,
            worker_key=worker_key,
            worker_cert=worker_cert,
            mutual=mutual,
        )

    @property
    def server_credentials(self) -> grpc.ServerCredentials:
        """Server credentials for accepting connections.

        Returns server credentials configured based on the ``mutual`` flag.
        Use when the worker is acting as a server accepting connections.

        **Mutual TLS (mutual=True):**
            Only clients with valid CA-signed certificates can connect.
            Provides mutual authentication at the transport layer.

        **One-way TLS (mutual=False):**
            Any client can establish an encrypted connection. The server's
            identity is verified, but **clients are NOT authenticated** at
            the transport layer. If using one-way TLS, you MUST implement
            authentication at a higher level (e.g., API keys, OAuth tokens)
            to verify client identity.

        :returns:
            Server credentials configured for mTLS or one-way TLS.

        .. warning::
            When ``mutual=False``, clients are not authenticated at the
            transport layer. Ensure application-level authentication is
            implemented.
        """
        import grpc

        return grpc.ssl_server_credentials(
            private_key_certificate_chain_pairs=[(self.worker_key, self.worker_cert)],
            root_certificates=self.ca_cert if self.mutual else None,
            require_client_auth=self.mutual,
        )

    @property
    def client_credentials(self) -> grpc.ChannelCredentials:
        """Client credentials for making connections.

        Returns client credentials configured based on the ``mutual`` flag.
        Use when the worker is acting as a client connecting to servers.

        **Mutual TLS (mutual=True):**
            The worker presents its certificate to the server for mutual
            authentication. Required when connecting to servers configured
            with mutual TLS.

        **One-way TLS (mutual=False):**
            The worker verifies the server's certificate but does NOT
            present its own certificate. The worker remains anonymous to
            the server at the transport layer.

        :returns:
            Client credentials configured for mTLS or one-way TLS.
        """
        import grpc

        return grpc.ssl_channel_credentials(
            root_certificates=self.ca_cert,
            private_key=self.worker_key if self.mutual else None,
            certificate_chain=self.worker_cert if self.mutual else None,
        )
