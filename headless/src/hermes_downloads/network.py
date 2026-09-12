"""Bounded trusted-source URL and credential policy.

This module validates only the initial submitted URL.  It does not resolve DNS,
follow redirects, or provide a custom transport/network sandbox.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import ipaddress
import socket
from typing import Final, Self
import unicodedata
from urllib.parse import SplitResult, urlsplit, urlunsplit

__all__ = [
    "CredentialPolicyError",
    "CredentialScope",
    "LocalOriginGrant",
    "MAX_SOURCE_URL_BYTES",
    "NetworkPolicy",
    "Origin",
    "SourcePolicyError",
    "SourceURL",
    "redact_url",
    "validate_source_url",
]

MAX_SOURCE_URL_BYTES: Final = 8192
_ALLOWED_SCHEMES: Final = frozenset({"http", "https"})
_DEFAULT_PORTS: Final = {"http": 80, "https": 443}
_HEX_DIGITS: Final = frozenset(b"0123456789abcdefABCDEF")
_REDACTED_URL: Final = "<redacted-url>"


class SourcePolicyError(ValueError):
    """A bounded source URL violates the trusted-source input policy."""


class CredentialPolicyError(SourcePolicyError):
    """Credential or ambient-network state violates the safe policy."""


@dataclass(frozen=True, slots=True)
class Origin:
    """A parsed HTTP origin used only for exact local/credential scope checks."""

    scheme: str
    host: str
    port: int


@dataclass(frozen=True, slots=True, repr=False, init=False)
class SourceURL:
    """Validated source bytes with a display value that never includes a query."""

    raw_url: bytes = field(repr=False)
    origin: Origin

    def __init__(self, *args: object, **kwargs: object) -> None:
        raise TypeError("SourceURL values must be created by validate_source_url")

    @property
    def public_url(self) -> str:
        return _redact_parts(urlsplit(self.raw_url.decode("utf-8")))

    def __str__(self) -> str:
        return self.public_url

    def __repr__(self) -> str:
        return f"SourceURL(public_url={self.public_url!r})"


def _source_url_from_validated_parts(raw_url: bytes, origin: Origin) -> SourceURL:
    source = object.__new__(SourceURL)
    object.__setattr__(source, "raw_url", raw_url)
    object.__setattr__(source, "origin", origin)
    return source


@dataclass(frozen=True, slots=True)
class LocalOriginGrant:
    """An exact test-only grant for one literal local origin."""

    origin: Origin

    def __post_init__(self) -> None:
        if type(self.origin) is not Origin:
            raise TypeError("origin must be a parsed Origin")
        if not _is_literal_local_host(self.origin.host):
            raise SourcePolicyError("local origin grants require a literal local host")

    @classmethod
    def for_url(cls, value: str | bytes | bytearray) -> Self:
        source = _parse_source_url(value)
        if not _is_literal_local_host(source.origin.host):
            raise SourcePolicyError("local origin grants require a literal local host")
        return cls(source.origin)


@dataclass(frozen=True, slots=True)
class NetworkPolicy:
    """Safe defaults for a later engine integration; no transport is created here."""

    use_netrc: bool = False
    use_cookies: bool = False
    use_environment_proxies: bool = False
    verify_tls: bool = True

    def __post_init__(self) -> None:
        for name in (
            "use_netrc",
            "use_cookies",
            "use_environment_proxies",
            "verify_tls",
        ):
            if type(getattr(self, name)) is not bool:
                raise TypeError(f"{name} must be a boolean")
        if self.use_netrc or self.use_cookies or self.use_environment_proxies:
            raise CredentialPolicyError("ambient credentials and proxies are disabled")
        if self.verify_tls is not True:
            raise CredentialPolicyError("TLS verification is required")


@dataclass(frozen=True, slots=True, init=False)
class CredentialScope:
    """Explicit origin-bound credential consent without any credential material."""

    origin: Origin

    def __init__(self, *args: object, **kwargs: object) -> None:
        raise TypeError("CredentialScope values must be created by for_source")

    @classmethod
    def for_source(cls, source: SourceURL, *, user_consented: bool) -> CredentialScope:
        if type(source) is not SourceURL:
            raise TypeError("source must be a validated SourceURL")
        if user_consented is not True:
            raise CredentialPolicyError("credentials require explicit user consent")
        return _credential_scope_from_validated_source(source.origin)

    def permits(self, source: SourceURL) -> bool:
        if type(source) is not SourceURL:
            raise TypeError("source must be a validated SourceURL")
        return self.origin == source.origin


def _credential_scope_from_validated_source(origin: Origin) -> CredentialScope:
    scope = object.__new__(CredentialScope)
    object.__setattr__(scope, "origin", origin)
    return scope


def validate_source_url(
    value: str | bytes | bytearray,
    *,
    local_origin_grant: LocalOriginGrant | None = None,
) -> SourceURL:
    """Validate an initial HTTP(S) URL without changing its raw bytes."""

    source = _parse_source_url(value)
    if local_origin_grant is not None and type(local_origin_grant) is not LocalOriginGrant:
        raise TypeError("local_origin_grant must be a LocalOriginGrant")
    if _is_literal_local_host(source.origin.host) and (
        local_origin_grant is None or local_origin_grant.origin != source.origin
    ):
        raise SourcePolicyError("literal local sources require an exact origin grant")
    return source


def redact_url(value: SourceURL | str | bytes | bytearray) -> str:
    """Render a URL for public output without userinfo, query, or fragment."""

    if isinstance(value, SourceURL):
        return value.public_url
    try:
        _, text = _coerce_url(value)
        return _redact_parts(urlsplit(text))
    except (SourcePolicyError, UnicodeError, ValueError):
        return _REDACTED_URL


def _parse_source_url(value: str | bytes | bytearray) -> SourceURL:
    raw_url, text = _coerce_url(value)
    _require_complete_percent_escapes(raw_url)
    try:
        parts = urlsplit(text)
        scheme = parts.scheme.lower()
        host = parts.hostname
        port = parts.port
    except ValueError:
        raise SourcePolicyError("source URL is malformed") from None
    if scheme not in _ALLOWED_SCHEMES:
        raise SourcePolicyError("source URL must use HTTP or HTTPS")
    if not parts.netloc or not host or "@" in parts.netloc:
        raise SourcePolicyError("source URL must have an authority without userinfo")
    if "/" in host or "\\" in host:
        raise SourcePolicyError("source URL has an invalid host")
    _reject_ambiguous_numeric_ipv4_host(host)
    origin = Origin(scheme=scheme, host=host, port=port if port is not None else _DEFAULT_PORTS[scheme])
    return _source_url_from_validated_parts(raw_url, origin)


def _coerce_url(value: str | bytes | bytearray) -> tuple[bytes, str]:
    if type(value) is str:
        raw_url = value.encode("utf-8")
    elif isinstance(value, (bytes, bytearray)):
        raw_url = bytes(value)
    else:
        raise TypeError("source URL must be text or bytes")
    if not raw_url or len(raw_url) > MAX_SOURCE_URL_BYTES:
        raise SourcePolicyError("source URL has an invalid length")
    try:
        text = raw_url.decode("utf-8")
    except UnicodeDecodeError:
        raise SourcePolicyError("source URL is not valid UTF-8") from None
    if any(
        character.isspace() or unicodedata.category(character) in {"Cc", "Cf"}
        for character in text
    ):
        raise SourcePolicyError("source URL contains control or format characters")
    return raw_url, text


def _require_complete_percent_escapes(raw_url: bytes) -> None:
    for index, value in enumerate(raw_url):
        if value == ord("%") and (
            index + 2 >= len(raw_url)
            or raw_url[index + 1] not in _HEX_DIGITS
            or raw_url[index + 2] not in _HEX_DIGITS
        ):
            raise SourcePolicyError("source URL has an incomplete percent escape")


def _reject_ambiguous_numeric_ipv4_host(host: str) -> None:
    try:
        canonical_host = socket.inet_ntoa(socket.inet_aton(host.rstrip(".")))
    except (OSError, UnicodeError):
        return
    if canonical_host != host:
        raise SourcePolicyError("source URL has an ambiguous numeric IPv4 host")


def _is_literal_local_host(host: str) -> bool:
    if host.rstrip(".").casefold() == "localhost":
        return True
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        return False
    return address.is_loopback or address.is_private


def _redact_parts(parts: SplitResult) -> str:
    try:
        host = parts.hostname
        port = parts.port
    except ValueError:
        return _REDACTED_URL
    if not parts.scheme or not host:
        return _REDACTED_URL
    authority = f"[{host}]" if ":" in host else host
    if port is not None:
        authority = f"{authority}:{port}"
    return urlunsplit((parts.scheme.lower(), authority, parts.path, "", ""))
