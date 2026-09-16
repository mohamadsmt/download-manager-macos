"""Trusted-source URL and credential-policy integration tests."""

from __future__ import annotations

from dataclasses import fields
import importlib
import importlib.util
from pathlib import Path
import sys

import pytest


_FIXTURE_SPEC = importlib.util.spec_from_file_location(
    "_network_policy_http_origin",
    Path(__file__).resolve().parents[1] / "fixtures" / "http_origin.py",
)
assert _FIXTURE_SPEC is not None and _FIXTURE_SPEC.loader is not None
_FIXTURE_MODULE = importlib.util.module_from_spec(_FIXTURE_SPEC)
sys.modules[_FIXTURE_SPEC.name] = _FIXTURE_MODULE
_FIXTURE_SPEC.loader.exec_module(_FIXTURE_MODULE)
LocalHttpOrigin = _FIXTURE_MODULE.LocalHttpOrigin


def _network():
    spec = importlib.util.find_spec("hermes_downloads.network")
    assert spec is not None, "hermes_downloads.network must provide trusted-source policy"
    return importlib.import_module("hermes_downloads.network")


@pytest.fixture
def http_origin() -> LocalHttpOrigin:
    return LocalHttpOrigin()


def test_valid_signed_url_preserves_exact_raw_query_bytes() -> None:
    network = _network()
    submitted = bytearray(
        b"https://downloads.example.test/release%2Ffile?"
        b"X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Signature=a%2Bb%3D&"
        b"duplicate=1&duplicate=1"
    )

    source = network.validate_source_url(submitted)
    submitted[-1] = ord("2")

    assert source.raw_url == (
        b"https://downloads.example.test/release%2Ffile?"
        b"X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Signature=a%2Bb%3D&"
        b"duplicate=1&duplicate=1"
    )
    assert source.public_url == "https://downloads.example.test/release%2Ffile"
    assert str(source) == source.public_url
    assert network.redact_url(source) == source.public_url
    assert "X-Amz-Signature" not in repr(source)
    assert "duplicate=1" not in repr(source)


def test_source_url_cannot_be_directly_constructed_to_leak_private_url_parts() -> None:
    network = _network()
    forged_public_url = (
        "https://alice:example-password@downloads.example.test/release?"
        "X-Amz-Signature=synthetic-secret#fragment-secret"
    )

    with pytest.raises(TypeError):
        network.SourceURL(
            raw_url=forged_public_url.encode("utf-8"),
            origin=network.Origin(
                scheme="https", host="downloads.example.test", port=443
            ),
            public_url=forged_public_url,
        )

    source = network.validate_source_url(
        b"https://downloads.example.test/release?X-Amz-Signature=synthetic-secret"
    )

    assert source.public_url == "https://downloads.example.test/release"
    assert str(source) == "https://downloads.example.test/release"
    assert network.redact_url(source) == "https://downloads.example.test/release"
    assert "synthetic-secret" not in repr(source)


def test_source_url_rejects_input_beyond_its_fixed_byte_bound() -> None:
    network = _network()
    submitted = b"https://downloads.example.test/?" + b"a" * network.MAX_SOURCE_URL_BYTES

    with pytest.raises(network.SourcePolicyError):
        network.validate_source_url(submitted)


@pytest.mark.parametrize(
    "submitted",
    (
        b"",
        b"https://",
        b"https:/missing-authority",
        b"ftp://downloads.example.test/file",
        b"file:///tmp/file",
        b"https://user:password@downloads.example.test/file",
        b"https://@downloads.example.test/file",
        b"https://downloads.example.test/file\n",
        "https://downloads.example.test/\u200bfile",
        b"https://downloads.example.test/%",
        b"https://downloads.example.test:99999/file",
    ),
)
def test_source_url_rejects_malformed_unsupported_or_credentialed_input(
    submitted: bytes | str,
) -> None:
    network = _network()

    with pytest.raises(network.SourcePolicyError):
        network.validate_source_url(submitted)


def test_local_literal_hosts_require_an_exact_fixture_origin_grant(
    http_origin: LocalHttpOrigin,
) -> None:
    network = _network()
    submitted = http_origin.url("/fixture?signature=synthetic")

    with pytest.raises(network.SourcePolicyError):
        network.validate_source_url(submitted)
    with pytest.raises(network.SourcePolicyError):
        network.validate_source_url("http://10.0.0.1/fixture")
    with pytest.raises(network.SourcePolicyError):
        network.validate_source_url("http://localhost:18080/fixture")

    grant = network.LocalOriginGrant.for_url(http_origin.origin)
    source = network.validate_source_url(submitted, local_origin_grant=grant)

    assert source.raw_url == submitted.encode("utf-8")
    localhost_grant = network.LocalOriginGrant.for_url("http://localhost:18080")
    assert (
        network.validate_source_url(
            "http://localhost:18080/fixture", local_origin_grant=localhost_grant
        ).raw_url
        == b"http://localhost:18080/fixture"
    )
    with pytest.raises(network.SourcePolicyError):
        network.validate_source_url(
            "http://127.0.0.1:18081/fixture", local_origin_grant=grant
        )


@pytest.mark.parametrize(
    "submitted",
    (
        "http://127.0.0.1./fixture",
        "http://127.1/fixture",
        "http://2130706433/fixture",
        "http://0x7f000001/fixture",
    ),
)
def test_source_url_rejects_ambiguous_numeric_ipv4_initial_hosts(
    submitted: str,
) -> None:
    network = _network()

    with pytest.raises(network.SourcePolicyError):
        network.validate_source_url(submitted)


def test_safe_transport_policy_refuses_ambient_state_and_requires_tls() -> None:
    network = _network()
    policy = network.NetworkPolicy()

    assert policy.use_netrc is False
    assert policy.use_cookies is False
    assert policy.use_environment_proxies is False
    assert policy.verify_tls is True
    for overrides in (
        {"use_netrc": True},
        {"use_cookies": True},
        {"use_environment_proxies": True},
        {"verify_tls": False},
    ):
        with pytest.raises(network.CredentialPolicyError):
            network.NetworkPolicy(**overrides)


def test_credentials_need_explicit_consent_and_never_cross_origins() -> None:
    network = _network()
    source = network.validate_source_url(b"https://downloads.example.test/file")
    redirect_target = network.validate_source_url(b"https://other.example.test/file")

    with pytest.raises(TypeError):
        network.CredentialScope(origin=source.origin)
    with pytest.raises(network.CredentialPolicyError):
        network.CredentialScope.for_source(source, user_consented=False)

    scope = network.CredentialScope.for_source(source, user_consented=True)

    assert scope.permits(source) is True
    assert scope.permits(redirect_target) is False
    assert "secret" not in {field.name for field in fields(network.CredentialScope)}


def test_public_redaction_removes_userinfo_query_and_fragment() -> None:
    network = _network()
    submitted = (
        b"https://alice:example-password@downloads.example.test/release?"
        b"X-Amz-Signature=synthetic-secret&download=1#fragment-secret"
    )

    rendered = network.redact_url(submitted)

    assert rendered == "https://downloads.example.test/release"
    assert "alice" not in rendered
    assert "example-password" not in rendered
    assert "X-Amz-Signature" not in rendered
    assert "synthetic-secret" not in rendered
    assert "fragment-secret" not in rendered


def test_non_utf8_source_policy_error_has_no_signed_query_exception_chain() -> None:
    network = _network()
    canary = "T07-SIGNED-QUERY-CANARY"
    submitted = (
        b"https://downloads.example.test/release?X-Amz-Signature="
        + canary.encode("ascii")
        + b"\xff"
    )

    with pytest.raises(network.SourcePolicyError) as raised:
        network.validate_source_url(submitted)

    error = raised.value

    assert canary not in str(error)
    assert canary not in repr(error)
    assert canary not in repr(error.__context__)
    assert canary not in repr(error.__cause__)
    assert error.__context__ is None
    assert error.__cause__ is None


def test_malformed_source_policy_error_has_no_signed_query_exception_chain() -> None:
    network = _network()
    canary = "T07-MALFORMED-SIGNED-QUERY-CANARY"
    submitted = (
        "https://downloads.example.test:99999/release?X-Amz-Signature=" + canary
    )

    with pytest.raises(network.SourcePolicyError) as raised:
        network.validate_source_url(submitted)

    error = raised.value

    assert canary not in str(error)
    assert canary not in repr(error)
    assert canary not in repr(error.__context__)
    assert canary not in repr(error.__cause__)
    assert error.__context__ is None
    assert error.__cause__ is None
