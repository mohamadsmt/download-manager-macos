"""Deterministic whole-queue payload allocation contract."""
from __future__ import annotations

import importlib
import importlib.util

import pytest


def _bandwidth():
    spec = importlib.util.find_spec("hermes_downloads.bandwidth")
    assert spec is not None, "hermes_downloads.bandwidth must provide allocation"
    return importlib.import_module("hermes_downloads.bandwidth")


def test_capped_equal_shares_never_exceed_item_or_global_limit() -> None:
    bandwidth = _bandwidth()

    shares = bandwidth.allocate(global_limit_bps=300, item_caps_bps=[50, None, 500])

    assert shares == [50, 125, 125]
    assert sum(share for share in shares if share is not None) <= 300


def test_zero_share_is_wait_not_unlimited() -> None:
    bandwidth = _bandwidth()

    assert bandwidth.allocate(global_limit_bps=1, item_caps_bps=[None, None]) == [1, 0]
    assert bandwidth.allocate(global_limit_bps=0, item_caps_bps=[None, 5]) == [0, 0]


def test_unlimited_global_limit_preserves_each_explicit_item_cap() -> None:
    bandwidth = _bandwidth()

    assert bandwidth.allocate(global_limit_bps=None, item_caps_bps=[50, None, 0]) == [
        50,
        None,
        0,
    ]


@pytest.mark.parametrize(
    ("global_limit_bps", "item_caps_bps"),
    (
        pytest.param(-1, [None], id="negative-global"),
        pytest.param(True, [None], id="boolean-global"),
        pytest.param(1, [-1], id="negative-item"),
        pytest.param(1, [True], id="boolean-item"),
        pytest.param(1, (None,), id="non-list-items"),
    ),
)
def test_allocation_rejects_invalid_limits(
    global_limit_bps: object, item_caps_bps: object
) -> None:
    bandwidth = _bandwidth()

    with pytest.raises((TypeError, ValueError)):
        bandwidth.allocate(
            global_limit_bps=global_limit_bps,
            item_caps_bps=item_caps_bps,
        )
