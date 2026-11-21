"""Save-first behaviour of the chain source.

A failed state save must never expose the new head/revision/blocks: reads keep
serving the last persisted chain, the failed operation's blocks stay
invisible, and retrying the same operation — before or after a restart —
regenerates the identical revision and block hashes.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from chain_source.app import ChainState, create_app
from tests.conftest import LiveServer, Stack
from tests.test_service import _sync, _transfers

INITIAL_BLOCKS = 5
TRANSFERS_PER_BLOCK = 2


@pytest.fixture()
def source_http(tmp_path: Path):
    """A live chain-source server with a known state path."""
    state_path = tmp_path / "source" / "chain.json"
    state = ChainState(
        state_path=str(state_path),
        initial_blocks=INITIAL_BLOCKS,
        initial_transfers_per_block=TRANSFERS_PER_BLOCK,
    )
    server = LiveServer(create_app(state))
    try:
        yield state, server, state_path
    finally:
        server.stop()


def _reference_state(tmp_path: Path, name: str = "ref") -> ChainState:
    """A never-failed reference chain with the same genesis parameters."""
    return ChainState(
        state_path=str(tmp_path / f"{name}.json"),
        initial_blocks=INITIAL_BLOCKS,
        initial_transfers_per_block=TRANSFERS_PER_BLOCK,
    )


# ------------------------------------------------------------------ unit level
def test_failed_advance_does_not_consume_revision_nonce_or_serial(tmp_path):
    path = tmp_path / "chain.json"
    state = ChainState(
        state_path=str(path),
        initial_blocks=INITIAL_BLOCKS,
        initial_transfers_per_block=TRANSFERS_PER_BLOCK,
    )
    before = (state.revision, state.nonce, state.tx_serial, len(state.blocks))
    state.set_faults({**state.get_faults(), "save_failures": 1})
    with pytest.raises(OSError):
        state.advance(2, TRANSFERS_PER_BLOCK)
    after = (state.revision, state.nonce, state.tx_serial, len(state.blocks))
    assert after == before
    # The file on disk is untouched and no tmp file is left behind.
    on_disk = json.loads(path.read_text())
    assert on_disk["revision"] == before[0]
    assert len(on_disk["blocks"]) == before[3]
    assert not path.with_suffix(".json.tmp").exists()


def test_retry_after_failed_save_matches_never_failed_run(tmp_path):
    state = ChainState(
        state_path=str(tmp_path / "chain.json"),
        initial_blocks=INITIAL_BLOCKS,
        initial_transfers_per_block=TRANSFERS_PER_BLOCK,
    )
    reference = _reference_state(tmp_path)
    state.set_faults({**state.get_faults(), "save_failures": 1})
    with pytest.raises(OSError):
        state.advance(2, TRANSFERS_PER_BLOCK)
    retried = state.advance(2, TRANSFERS_PER_BLOCK)
    expected = reference.advance(2, TRANSFERS_PER_BLOCK)
    assert [b.hash for b in retried] == [b.hash for b in expected]
    assert state.revision == reference.revision
    assert state.head_hash == reference.head_hash


def test_retry_after_restart_matches_never_failed_run(tmp_path):
    path = tmp_path / "chain.json"
    state = ChainState(
        state_path=str(path),
        initial_blocks=INITIAL_BLOCKS,
        initial_transfers_per_block=TRANSFERS_PER_BLOCK,
    )
    reference = _reference_state(tmp_path)
    state.set_faults({**state.get_faults(), "save_failures": 1})
    with pytest.raises(OSError):
        state.advance(2, TRANSFERS_PER_BLOCK)
    # Restart from the persisted state; the failed attempt left no trace.
    restarted = ChainState(state_path=str(path))
    retried = restarted.advance(2, TRANSFERS_PER_BLOCK)
    expected = reference.advance(2, TRANSFERS_PER_BLOCK)
    assert [b.hash for b in retried] == [b.hash for b in expected]
    assert restarted.revision == reference.revision
    assert restarted.head_hash == reference.head_hash


def test_failed_fork_and_rollback_are_invisible_and_retryable(tmp_path):
    path = tmp_path / "chain.json"
    state = ChainState(
        state_path=str(path),
        initial_blocks=INITIAL_BLOCKS,
        initial_transfers_per_block=TRANSFERS_PER_BLOCK,
    )
    reference = _reference_state(tmp_path)
    old_canonical = dict(state.canonical)
    old_head = state.head_hash

    state.set_faults({**state.get_faults(), "save_failures": 1})
    with pytest.raises(OSError):
        state.fork(3, 2, TRANSFERS_PER_BLOCK, salt="branch-x")
    assert dict(state.canonical) == old_canonical
    assert state.head_hash == old_head
    # Immutable blocks from the abandoned attempt were never added.
    assert len(state.blocks) == INITIAL_BLOCKS + 1

    retried = state.fork(3, 2, TRANSFERS_PER_BLOCK, salt="branch-x")
    expected = reference.fork(3, 2, TRANSFERS_PER_BLOCK, salt="branch-x")
    assert [b.hash for b in retried] == [b.hash for b in expected]
    assert state.revision == reference.revision

    state.set_faults({**state.get_faults(), "save_failures": 1})
    head_before = state.head_hash
    with pytest.raises(OSError):
        state.rollback(2)
    assert state.head_hash == head_before
    assert max(state.canonical) == INITIAL_BLOCKS

    state.rollback(2)
    reference.rollback(2)
    assert state.head_hash == reference.head_hash
    assert state.revision == reference.revision


def test_save_failure_counter_decrements_per_attempt(tmp_path):
    state = ChainState(
        state_path=str(tmp_path / "chain.json"),
        initial_blocks=INITIAL_BLOCKS,
        initial_transfers_per_block=TRANSFERS_PER_BLOCK,
    )
    state.set_faults({**state.get_faults(), "save_failures": 2})
    with pytest.raises(OSError):
        state.advance(1, 1)
    with pytest.raises(OSError):
        state.advance(1, 1)
    created = state.advance(1, 1)  # third attempt succeeds
    assert len(created) == 1
    assert state.faults["save_failures"] == 0


def test_state_file_format_is_unchanged_and_tolerant(tmp_path):
    path = tmp_path / "chain.json"
    state = ChainState(
        state_path=str(path),
        initial_blocks=2,
        initial_transfers_per_block=1,
    )
    raw = json.loads(path.read_text())
    assert set(raw) == {
        "version",
        "revision",
        "head_hash",
        "nonce",
        "tx_serial",
        "canonical",
        "blocks",
    }
    assert raw["version"] == 1
    # Files written before nonce/tx_serial existed still load (defaults: 0).
    legacy = dict(raw)
    legacy.pop("nonce")
    legacy.pop("tx_serial")
    legacy_path = tmp_path / "legacy.json"
    legacy_path.write_text(json.dumps(legacy))
    loaded = ChainState(state_path=str(legacy_path))
    assert loaded.head_hash == state.head_hash
    assert loaded.revision == state.revision


# ------------------------------------------------------------------ HTTP level
def test_failed_advance_keeps_last_saved_chain_visible(source_http):
    _state, server, _ = source_http
    client = server.client()
    head_before = client.get("/head").json()
    stats_before = client.get("/admin/stats").json()

    client.post("/admin/faults", json={"save_failures": 1})
    response = client.post(
        "/admin/advance", json={"count": 2, "transfers_per_block": 2}
    )
    assert response.status_code == 503
    assert "save failed" in response.json()["detail"]

    head_after = client.get("/head").json()
    assert head_after == head_before
    stats_after = client.get("/admin/stats").json()
    assert stats_after["block_count"] == stats_before["block_count"]
    assert stats_after["revision"] == stats_before["revision"]
    # The canonical tip is still the previously saved block.
    canonical = client.get(f"/canonical/{head_before['height']}").json()
    assert canonical["hash"] == head_before["hash"]


def test_failed_fork_hides_new_blocks_and_keeps_old_suffix(source_http, tmp_path):
    _state, server, _ = source_http
    client = server.client()
    reference = _reference_state(tmp_path)
    expected = reference.fork(3, 2, TRANSFERS_PER_BLOCK, salt="hidden")
    old_suffix = {
        h: client.get(f"/canonical/{h}").json()["hash"] for h in (4, INITIAL_BLOCKS)
    }

    client.post("/admin/faults", json={"save_failures": 1})
    response = client.post(
        "/admin/fork",
        json={
            "from_height": 3,
            "count": 2,
            "transfers_per_block": TRANSFERS_PER_BLOCK,
            "salt": "hidden",
        },
    )
    assert response.status_code == 503
    # Old canonical suffix is still served; the failed branch is invisible.
    for height, old_hash in old_suffix.items():
        assert client.get(f"/canonical/{height}").json()["hash"] == old_hash
    for block in expected:
        assert client.get(f"/blocks/{block.hash}").status_code == 404

    # Retry succeeds and exposes exactly the reference branch.
    response = client.post(
        "/admin/fork",
        json={
            "from_height": 3,
            "count": 2,
            "transfers_per_block": TRANSFERS_PER_BLOCK,
            "salt": "hidden",
        },
    )
    assert response.status_code == 200
    created = response.json()["created"]
    assert [b["hash"] for b in created] == [b.hash for b in expected]
    # The abandoned old branch remains fetchable by hash.
    for old_hash in old_suffix.values():
        assert client.get(f"/blocks/{old_hash}").status_code == 200


def test_failed_rollback_keeps_head_and_canonical_suffix(source_http):
    _state, server, _ = source_http
    client = server.client()
    head_before = client.get("/head").json()

    client.post("/admin/faults", json={"save_failures": 1})
    response = client.post("/admin/rollback", json={"to_height": 2})
    assert response.status_code == 503
    assert client.get("/head").json() == head_before
    assert client.get(f"/canonical/{INITIAL_BLOCKS}").status_code == 200

    response = client.post("/admin/rollback", json={"to_height": 2})
    assert response.status_code == 200
    head_after = client.get("/head").json()
    assert head_after["height"] == 2
    assert head_after["revision"] == head_before["revision"] + 1
    assert client.get(f"/canonical/{INITIAL_BLOCKS}").status_code == 404


def test_failed_save_leaves_state_file_byte_identical(source_http):
    _state, server, state_path = source_http
    client = server.client()
    before_bytes = state_path.read_bytes()

    client.post("/admin/faults", json={"save_failures": 1})
    assert client.post("/admin/advance", json={"count": 1}).status_code == 503
    assert state_path.read_bytes() == before_bytes
    assert not state_path.with_suffix(".json.tmp").exists()


# --------------------------------------------------------------- end to end
def test_follower_never_sees_an_unsaved_revision(synced: Stack):
    stack = synced
    before = _transfers(stack, "addr-alice")
    published_revision = before["head_basis"]["revision"]

    # The save fails: the source keeps serving the last saved head, so the
    # round finds nothing new and the published view is untouched.
    stack.source_client().post("/admin/faults", json={"save_failures": 1})
    assert (
        stack.source_client()
        .post("/admin/advance", json={"count": 2, "transfers_per_block": 2})
        .status_code
        == 503
    )
    head = stack.source_client().get("/head").json()
    assert head["revision"] == published_revision
    report = _sync(stack)
    assert report["outcome"] == "stale"
    assert _transfers(stack, "addr-alice")["chain_view"] == before["chain_view"]

    # Once the fault clears, the retry commits and the follower catches up.
    stack.source_client().post("/admin/faults", json={})
    assert (
        stack.source_client()
        .post("/admin/advance", json={"count": 2, "transfers_per_block": 2})
        .status_code
        == 200
    )
    report = _sync(stack)
    assert report["outcome"] == "committed"
    after = _transfers(stack, "addr-alice")
    assert after["head_basis"]["revision"] == published_revision + 2
    assert after["head_basis"]["height"] == 7


def test_source_restart_after_failed_save_does_not_wedge_follower(synced: Stack):
    """The exact failure from the bug report: failed save, source restart,
    retried operation — the follower must accept the retried revision."""
    stack = synced
    before = _transfers(stack, "addr-alice")
    published_revision = before["head_basis"]["revision"]

    stack.source_client().post("/admin/faults", json={"save_failures": 1})
    assert (
        stack.source_client()
        .post("/admin/advance", json={"count": 2, "transfers_per_block": 2})
        .status_code
        == 503
    )
    # The unsaved revision was never exposed, so the follower stays on the
    # last saved one — nothing to unwind after the restart.
    stack.restart_source()
    head = stack.source_client().get("/head").json()
    assert head["revision"] == published_revision

    # The retried operation reuses the same revision numbers and hashes; the
    # follower has never seen them and publishes the view normally.
    created = (
        stack.source_client()
        .post("/admin/advance", json={"count": 2, "transfers_per_block": 2})
        .json()
    )
    head = stack.source_client().get("/head").json()
    assert head["revision"] == published_revision + 2
    assert head["hash"] == created["head"]["hash"]
    report = _sync(stack)
    assert report["outcome"] == "committed"
    after = _transfers(stack, "addr-alice")
    assert after["head_basis"]["revision"] == published_revision + 2
    assert after["head_basis"]["hash"] == created["head"]["hash"]
    assert after["head_basis"]["height"] == 7
