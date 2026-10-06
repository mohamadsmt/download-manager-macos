"""Immutable batch expected intent qualifies actual descriptor bytes pre-permit."""
import hashlib
import json

import pytest
from hermes_downloads import ipc
from hermes_downloads.retry import CompletionVerification
from test_success_cleanup import real_cleanup_producer, _effect_rows, _rows, _wait


@pytest.mark.parametrize('expected,verification', [
    ('match', CompletionVerification.CHECKSUM_VERIFIED),
    ('none', CompletionVerification.TRANSPORT_VERIFIED)])
def test_actual_owner_derives_expected_or_hashless_verification(expected, verification):
    with real_cleanup_producer(expected=expected) as fixture:
        _wait(lambda: _rows(fixture['database'], 'SELECT phase FROM direct_cleanup_claims') == [('finished',)])
        assert fixture['final'].read_bytes() == fixture['origin'].payload
        assert hashlib.sha256(fixture['final'].read_bytes()).hexdigest() == fixture['digest']
        rows = _effect_rows(fixture)
        derived = [row for row in rows if row['kind'] == 'actual-owner-derived-verification']
        assert len(derived) == 1
        assert derived[0]['verification'] == verification.value
        assert derived[0]['actual_sha256'] == fixture['digest']
        assert len([row for row in rows if row['kind'] == 'engine-add-transport-only']) == 1
        assert fixture['origin'].ledger.request_count == 1
        assert fixture['origin'].ledger.response_body_bytes == len(fixture['origin'].payload)
        assert _rows(fixture['database'], "SELECT count(*) FROM events WHERE kind='job_completed'") == [(1,)]


def _assert_no_publication_after_contained_body(fixture):
    _wait(lambda: ipc.request_jobs_page(fixture['sock']).jobs[0].state == 'paused')
    ledger = fixture['evidence'] / f"fixture-{fixture['process'].pid}.jsonl"
    _wait(lambda: any(json.loads(line)['kind'] == 'engine-waitable-child-reaped'
        for line in ledger.read_text().splitlines()))
    assert fixture['origin'].ledger.request_count == 1
    assert fixture['origin'].ledger.response_body_bytes == len(fixture['origin'].payload)
    assert fixture['partial'].read_bytes() == fixture['origin'].payload
    assert fixture['partial'].stat().st_nlink == 1
    assert fixture['marker'].exists()
    assert (fixture['partial'].parent / 'unknown.sidecar').read_bytes() == b'unknown artifact is preserved'
    assert not fixture['final'].exists()
    for table in ('direct_publication_attempts', 'closed_direct_publication_attempts',
                  'final_publication_bindings', 'direct_cleanup_claims'):
        assert _rows(fixture['database'], f'SELECT count(*) FROM {table}') == [(0,)]
    assert _rows(fixture['database'], "SELECT count(*) FROM events WHERE kind='job_completed'") == [(0,)]
    assert not any(row['kind'] == 'actual-owner-derived-verification' for row in _effect_rows(fixture))
    records = [json.loads(line) for line in ledger.read_text().splitlines()]
    assert len([row for row in records if row['kind'] == 'engine-birth']) == 1
    assert _rows(fixture['database'], 'SELECT * FROM direct_dispatch_commands WHERE request_id=?',
        ('cleanup-start',))[0] == fixture['original_receipt']


def test_wrong_same_length_hash_pauses_once_before_attempt_permit_or_publication():
    with real_cleanup_producer(expected='wrong') as fixture:
        assert len(fixture['origin'].changed_payload) == len(fixture['origin'].payload)
        assert hashlib.sha256(fixture['origin'].changed_payload).hexdigest() != fixture['digest']
        _assert_no_publication_after_contained_body(fixture)


def test_corrupt_hashless_creation_intent_cannot_become_absent_expected():
    with real_cleanup_producer(expected='none', mode='corrupt-intent') as fixture:
        _assert_no_publication_after_contained_body(fixture)
        assert _rows(fixture['database'], 'SELECT creation_intent_blob FROM add_batch_entries') == [(b'{}',)]
