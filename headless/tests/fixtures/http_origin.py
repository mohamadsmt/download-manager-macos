"""Synthetic loopback origin used only by network-policy tests.

The helper never binds a socket or transfers a body; T07 validates URL policy
without a transport.
"""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class LocalHttpOrigin:
    """A deterministic literal loopback origin for an explicit test grant."""

    host: str = "127.0.0.1"
    port: int = 18080

    @property
    def origin(self) -> str:
        return f"http://{self.host}:{self.port}"

    def url(self, path: str = "/fixture") -> str:
        if not path.startswith("/"):
            raise ValueError("fixture path must start with a slash")
        return f"{self.origin}{path}"
