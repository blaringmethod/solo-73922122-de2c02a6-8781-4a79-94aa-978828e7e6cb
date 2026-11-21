"""A small, deterministic, controllable local chain source over HTTP.

The source models a chain as an append-only set of immutable blocks plus a
canonical height -> hash mapping whose head can move.  Every head change bumps
a monotonically increasing revision number.  Blocks are addressed by hash, so
old branches remain readable after a reorg (the source never deletes blocks).

Persistence is save-first: advance/fork/rollback stage their change, write the
resulting state to disk, and only then make the new head, revision and blocks
visible.  A failed save leaves the last persisted chain fully served, keeps
the failed operation's blocks invisible, and does not consume revision, nonce
or tx serial numbers — so retrying the same operation (before or after a
restart) regenerates the identical revision and block hashes.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import os
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

GENESIS_PARENT = "0" * 64
DEFAULT_ADDRESSES = ["addr-alice", "addr-bob", "addr-carol", "addr-dave"]


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


@dataclass
class Transfer:
    txid: str
    from_address: str
    to_address: str
    amount: int

    def as_dict(self) -> dict[str, Any]:
        # Both spellings are exposed on purpose: `from`/`to` is convenient in
        # JSON, while `from_address`/`to_address` is unambiguous in typed clients.
        return {
            "txid": self.txid,
            "from": self.from_address,
            "to": self.to_address,
            "from_address": self.from_address,
            "to_address": self.to_address,
            "amount": self.amount,
        }


@dataclass
class Block:
    height: int
    hash: str
    parent_hash: str
    transfers: list[Transfer] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "height": self.height,
            "hash": self.hash,
            "parent_hash": self.parent_hash,
            "transfers": [t.as_dict() for t in self.transfers],
        }


@dataclass
class Head:
    revision: int
    height: int
    hash: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "revision": self.revision,
            "height": self.height,
            "hash": self.hash,
        }


@dataclass
class _PendingChange:
    """A staged chain mutation, fully described before it is persisted.

    Everything needed to undo/discard a failed change lives here and only
    here, so the extra memory grows with the number of blocks touched by this
    one operation — never with the size of the chain.
    """

    new_blocks: list[Block]
    canonical_updates: dict[int, str]
    canonical_removals: set[int]
    head_hash: str
    revision: int
    nonce: int
    tx_serial: int


class ChainState:
    """Thread-safe chain state with optional JSON persistence."""

    def __init__(
        self,
        state_path: str | None = None,
        initial_blocks: int = 0,
        initial_transfers_per_block: int = 2,
        addresses: list[str] | None = None,
    ) -> None:
        self.lock = threading.RLock()
        self.state_path = Path(state_path) if state_path else None
        self.addresses = addresses or list(DEFAULT_ADDRESSES)
        self.blocks: dict[str, Block] = {}
        self.canonical: dict[int, str] = {}
        self.head_hash: str = ""
        self.revision: int = 0
        self.nonce: int = 0
        self.tx_serial: int = 0
        # Stats are intentionally not persisted.
        self.stats: dict[str, Any] = {
            "head_requests": 0,
            "block_requests": 0,
            "block_requests_by_hash": {},
            "failed_requests": 0,
        }
        self.faults: dict[str, Any] = {
            "delay_ms": 0,
            "head_failures": 0,
            "block_failures": 0,
            "save_failures": 0,
            "fail_all": False,
            "status_code": 503,
        }

        loaded = self._load()
        if not loaded:
            self._init_genesis()
            if initial_blocks > 0:
                self.advance(initial_blocks, initial_transfers_per_block)
            else:
                self._save()

    # ------------------------------------------------------------------ state
    @property
    def head(self) -> Block:
        return self.blocks[self.head_hash]

    def _init_genesis(self) -> None:
        self.blocks = {}
        self.canonical = {}
        self.revision = 1
        self.nonce = 0
        self.tx_serial = 0
        genesis = self._make_block(0, GENESIS_PARENT, [], "genesis")
        self.blocks[genesis.hash] = genesis
        self.canonical[0] = genesis.hash
        self.head_hash = genesis.hash

    def _make_block(
        self,
        height: int,
        parent_hash: str,
        transfers: list[Transfer],
        salt: str,
    ) -> Block:
        payload = {
            "height": height,
            "parent_hash": parent_hash,
            "salt": salt,
            "transfers": [
                {
                    "txid": t.txid,
                    "from_address": t.from_address,
                    "to_address": t.to_address,
                    "amount": t.amount,
                }
                for t in transfers
            ],
        }
        block_hash = _sha256_text(
            json.dumps(payload, sort_keys=True, separators=(",", ":"))
        )
        return Block(
            height=height, hash=block_hash, parent_hash=parent_hash, transfers=transfers
        )

    def _generate_transfers(
        self, height: int, count: int, salt: str, tx_serial: int
    ) -> tuple[list[Transfer], int]:
        """Build deterministic transfers without mutating any state.

        Returns the transfers plus the tx serial that would result; the caller
        only stores it when the staged change is committed, so a failed save
        never consumes serials and a retry reproduces identical txids.
        """
        transfers: list[Transfer] = []
        for i in range(count):
            tx_serial += 1
            seed = f"tx:{salt}:{height}:{i}:{tx_serial}"
            digest = _sha256_text(seed)
            sender_idx = (height + i) % len(self.addresses)
            receiver_idx = (sender_idx + 1) % len(self.addresses)
            if len(self.addresses) > 2:
                receiver_idx = (sender_idx + 1 + (i % (len(self.addresses) - 1))) % len(
                    self.addresses
                )
            if receiver_idx == sender_idx:
                receiver_idx = (sender_idx + 1) % len(self.addresses)
            amount = int(digest[:8], 16) % 900 + 1
            transfers.append(
                Transfer(
                    txid=digest[:40],
                    from_address=self.addresses[sender_idx],
                    to_address=self.addresses[receiver_idx],
                    amount=amount,
                )
            )
        return transfers, tx_serial

    def _stage_extension(
        self,
        ancestor: Block,
        count: int,
        transfers_per_block: int,
        salt_prefix: str,
    ) -> tuple[list[Block], int, int]:
        """Build ``count`` new blocks on top of ``ancestor`` without mutating.

        Returns the created blocks plus the nonce/tx_serial values that would
        result from committing them.  Because nothing is consumed here, a
        failed save followed by a retry of the same operation regenerates the
        identical salts, and therefore identical block hashes.
        """
        created: list[Block] = []
        nonce = self.nonce
        tx_serial = self.tx_serial
        current = ancestor
        for _ in range(count):
            height = current.height + 1
            nonce += 1
            salt = f"{salt_prefix}:{nonce}:{height}"
            transfers, tx_serial = self._generate_transfers(
                height, transfers_per_block, salt, tx_serial
            )
            block = self._make_block(height, current.hash, transfers, salt)
            created.append(block)
            current = block
        return created, nonce, tx_serial

    def _commit(self, pending: _PendingChange) -> None:
        """Persist a staged change, then — and only then — make it visible.

        If the save raises, the live state is left exactly as it was: reads
        keep serving the last persisted chain, the staged blocks stay
        invisible, and revision/nonce/tx_serial are not consumed.
        """
        self._save(pending)
        for block in pending.new_blocks:
            self.blocks[block.hash] = block
        for height in pending.canonical_removals:
            self.canonical.pop(height, None)
        self.canonical.update(pending.canonical_updates)
        self.head_hash = pending.head_hash
        self.revision = pending.revision
        self.nonce = pending.nonce
        self.tx_serial = pending.tx_serial

    # --------------------------------------------------------------- controls
    def advance(self, count: int, transfers_per_block: int = 0) -> list[Block]:
        with self.lock:
            created, nonce, tx_serial = self._stage_extension(
                self.head, count, transfers_per_block, "advance"
            )
            self._commit(
                _PendingChange(
                    new_blocks=created,
                    canonical_updates={b.height: b.hash for b in created},
                    canonical_removals=set(),
                    head_hash=created[-1].hash if created else self.head_hash,
                    revision=self.revision + len(created),
                    nonce=nonce,
                    tx_serial=tx_serial,
                )
            )
            return created

    def fork(
        self,
        from_height: int,
        count: int,
        transfers_per_block: int = 0,
        salt: str | None = None,
    ) -> list[Block]:
        with self.lock:
            if from_height not in self.canonical:
                raise ValueError(f"no canonical block at height {from_height}")
            ancestor = self.blocks[self.canonical[from_height]]
            if not (0 <= from_height <= self.head.height):
                raise ValueError("from_height out of range")
            # The superseded canonical suffix is dropped; immutable blocks stay.
            removals = {h for h in self.canonical if h > from_height}
            created, nonce, tx_serial = self._stage_extension(
                ancestor, count, transfers_per_block, f"fork:{salt or 'branch'}"
            )
            self._commit(
                _PendingChange(
                    new_blocks=created,
                    canonical_updates={b.height: b.hash for b in created},
                    canonical_removals=removals,
                    head_hash=created[-1].hash if created else ancestor.hash,
                    revision=self.revision + len(created),
                    nonce=nonce,
                    tx_serial=tx_serial,
                )
            )
            return created

    def rollback(self, to_height: int) -> Block:
        with self.lock:
            if to_height not in self.canonical:
                raise ValueError(f"no canonical block at height {to_height}")
            if to_height >= self.head.height:
                raise ValueError("rollback target must be below the current head")
            removals = {h for h in self.canonical if h > to_height}
            self._commit(
                _PendingChange(
                    new_blocks=[],
                    canonical_updates={},
                    canonical_removals=removals,
                    head_hash=self.canonical[to_height],
                    revision=self.revision + 1,
                    nonce=self.nonce,
                    tx_serial=self.tx_serial,
                )
            )
            return self.head

    def set_faults(self, config: dict[str, Any]) -> dict[str, Any]:
        with self.lock:
            self.faults = config
            return dict(self.faults)

    def get_faults(self) -> dict[str, Any]:
        with self.lock:
            return dict(self.faults)

    # ------------------------------------------------------------- persistence
    def _save(self, pending: _PendingChange | None = None) -> None:
        """Persist the chain state, optionally overlaid with a staged change.

        The write is atomic (tmp file + ``os.replace``), so the state file
        always holds the last *complete* save.  When ``pending`` is given the
        file reflects the staged change while the live in-memory state is only
        updated after the save succeeds (see ``_commit``); on failure nothing
        visible changes.
        """
        if not self.state_path:
            return
        save_failures = int(self.faults.get("save_failures", 0))
        if save_failures > 0:
            self.faults["save_failures"] = save_failures - 1
            raise OSError("simulated chain source save failure")
        blocks = self.blocks
        canonical = self.canonical
        revision = self.revision
        head_hash = self.head_hash
        nonce = self.nonce
        tx_serial = self.tx_serial
        if pending is not None:
            blocks = dict(self.blocks)
            for block in pending.new_blocks:
                blocks[block.hash] = block
            canonical = {
                height: block_hash
                for height, block_hash in self.canonical.items()
                if height not in pending.canonical_removals
            }
            canonical.update(pending.canonical_updates)
            revision = pending.revision
            head_hash = pending.head_hash
            nonce = pending.nonce
            tx_serial = pending.tx_serial
        payload = {
            "version": 1,
            "revision": revision,
            "head_hash": head_hash,
            "nonce": nonce,
            "tx_serial": tx_serial,
            "canonical": {str(h): hh for h, hh in sorted(canonical.items())},
            "blocks": {h: b.as_dict() for h, b in blocks.items()},
        }
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.state_path.with_suffix(self.state_path.suffix + ".tmp")
        try:
            tmp.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
            os.replace(tmp, self.state_path)
        except OSError:
            # Never leave a partial state file behind: the previous complete
            # save stays untouched at state_path.
            with contextlib.suppress(OSError):
                tmp.unlink()
            raise

    def _load(self) -> bool:
        if not self.state_path or not self.state_path.exists():
            return False
        raw = json.loads(self.state_path.read_text(encoding="utf-8"))
        blocks: dict[str, Block] = {}
        for h, b in raw["blocks"].items():
            transfers = [
                Transfer(
                    txid=t["txid"],
                    from_address=t["from_address"],
                    to_address=t["to_address"],
                    amount=int(t["amount"]),
                )
                for t in b.get("transfers", [])
            ]
            blocks[h] = Block(
                height=int(b["height"]),
                hash=b["hash"],
                parent_hash=b["parent_hash"],
                transfers=transfers,
            )
        canonical = {int(h): hh for h, hh in raw["canonical"].items()}
        head_hash = raw["head_hash"]
        if not canonical or head_hash not in blocks:
            raise RuntimeError(f"invalid chain source state: {self.state_path}")
        self.blocks = blocks
        self.canonical = canonical
        self.head_hash = head_hash
        self.revision = int(raw["revision"])
        self.nonce = int(raw.get("nonce", 0))
        self.tx_serial = int(raw.get("tx_serial", 0))
        return True


# --------------------------------------------------------------------- models
class AdvanceRequest(BaseModel):
    count: int = Field(default=1, ge=1, le=1000)
    transfers_per_block: int = Field(default=0, ge=0, le=200)


class ForkRequest(BaseModel):
    from_height: int = Field(..., ge=0)
    count: int = Field(default=1, ge=1, le=1000)
    transfers_per_block: int = Field(default=0, ge=0, le=200)
    salt: str | None = None


class RollbackRequest(BaseModel):
    to_height: int = Field(..., ge=0)


class FaultRequest(BaseModel):
    delay_ms: int = Field(default=0, ge=0, le=600_000)
    head_failures: int = Field(default=0, ge=0)
    block_failures: int = Field(default=0, ge=0)
    save_failures: int = Field(default=0, ge=0)
    fail_all: bool = False
    status_code: int = Field(default=503, ge=400, le=599)


# ------------------------------------------------------------------------ app
def create_app(state: ChainState | None = None) -> FastAPI:
    if state is None:
        state = ChainState(
            state_path=os.getenv("SOURCE_STATE_PATH") or None,
            initial_blocks=int(os.getenv("INITIAL_BLOCKS", "0")),
            initial_transfers_per_block=int(
                os.getenv("INITIAL_TRANSFERS_PER_BLOCK", "2")
            ),
        )

    app = FastAPI(
        title="Local Chain Source",
        version="1.0.0",
        description=(
            "Controllable local chain used for demos and tests. Every head change "
            "increments `revision`; blocks are immutable and fetched by hash. "
            "Advance/fork/rollback are save-first: the new head, revision and "
            "blocks become visible only after the state is persisted, and a "
            "failed save leaves the last saved chain served (retry is safe and "
            "deterministic). Use /admin/advance, /admin/fork, /admin/rollback "
            "and /admin/faults to drive the source."
        ),
    )
    app.state.chain = state

    async def _fault_gate(kind: str) -> JSONResponse | None:
        with state.lock:
            delay_ms = int(state.faults.get("delay_ms", 0))
            status_code = int(state.faults.get("status_code", 503))
        if delay_ms:
            await asyncio.sleep(delay_ms / 1000.0)
        with state.lock:
            if state.faults.get("fail_all"):
                state.stats["failed_requests"] += 1
                return JSONResponse(
                    status_code=status_code,
                    content={"detail": "simulated chain source failure"},
                )
            if kind == "head" and state.faults.get("head_failures", 0) > 0:
                state.faults["head_failures"] -= 1
                state.stats["failed_requests"] += 1
                return JSONResponse(
                    status_code=status_code,
                    content={
                        "detail": "simulated head failure",
                        "remaining_head_failures": state.faults["head_failures"],
                    },
                )
            if kind == "block" and state.faults.get("block_failures", 0) > 0:
                state.faults["block_failures"] -= 1
                state.stats["failed_requests"] += 1
                return JSONResponse(
                    status_code=status_code,
                    content={
                        "detail": "simulated block failure",
                        "remaining_block_failures": state.faults["block_failures"],
                    },
                )
        return None

    @app.get("/health", tags=["ops"])
    async def health() -> dict[str, Any]:
        with state.lock:
            return {
                "status": "ok",
                "revision": state.revision,
                "head_height": state.head.height,
                "head_hash": state.head_hash,
            }

    @app.get("/head", tags=["chain"])
    @app.get("/chain/head", tags=["chain"], include_in_schema=False)
    async def head(request: Request):
        fault = await _fault_gate("head")
        if fault is not None:
            return fault
        with state.lock:
            state.stats["head_requests"] += 1
            return {
                "revision": state.revision,
                "height": state.head.height,
                "hash": state.head_hash,
                "head_height": state.head.height,
                "total_height": state.head.height,
            }

    @app.get("/blocks/{block_hash}", tags=["chain"])
    async def get_block(block_hash: str, request: Request):
        fault = await _fault_gate("block")
        if fault is not None:
            return fault
        with state.lock:
            state.stats["block_requests"] += 1
            per_hash = state.stats["block_requests_by_hash"]
            per_hash[block_hash] = per_hash.get(block_hash, 0) + 1
            block = state.blocks.get(block_hash.lower())
            if block is None:
                raise HTTPException(status_code=404, detail="block not found")
            return block.as_dict()

    @app.get("/canonical/{height}", tags=["chain"])
    async def get_canonical(height: int):
        with state.lock:
            block_hash = state.canonical.get(height)
            if block_hash is None:
                raise HTTPException(status_code=404, detail="height not canonical")
            return state.blocks[block_hash].as_dict()

    @app.post("/admin/advance", tags=["admin"])
    async def advance(body: AdvanceRequest):
        with state.lock:
            try:
                created = state.advance(body.count, body.transfers_per_block)
            except OSError as exc:
                raise HTTPException(
                    status_code=503, detail=f"chain state save failed: {exc}"
                ) from exc
            return {
                "head": state.head.as_dict(),
                "created": [
                    {"height": b.height, "hash": b.hash, "tx_count": len(b.transfers)}
                    for b in created
                ],
            }

    @app.post("/admin/fork", tags=["admin"])
    async def fork(body: ForkRequest):
        with state.lock:
            try:
                ancestor = state.blocks[state.canonical[body.from_height]]
                created = state.fork(
                    body.from_height,
                    body.count,
                    body.transfers_per_block,
                    body.salt,
                )
            except KeyError as exc:
                raise HTTPException(status_code=404, detail=str(exc)) from exc
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
            except OSError as exc:
                raise HTTPException(
                    status_code=503, detail=f"chain state save failed: {exc}"
                ) from exc
            return {
                "common_ancestor": {
                    "height": ancestor.height,
                    "hash": ancestor.hash,
                },
                "head": state.head.as_dict(),
                "created": [
                    {"height": b.height, "hash": b.hash, "tx_count": len(b.transfers)}
                    for b in created
                ],
            }

    @app.post("/admin/rollback", tags=["admin"])
    async def rollback(body: RollbackRequest):
        with state.lock:
            try:
                new_head = state.rollback(body.to_height)
            except KeyError as exc:
                raise HTTPException(status_code=404, detail=str(exc)) from exc
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
            except OSError as exc:
                raise HTTPException(
                    status_code=503, detail=f"chain state save failed: {exc}"
                ) from exc
            return {"head": new_head.as_dict()}

    @app.post("/admin/faults", tags=["admin"])
    async def set_faults(body: FaultRequest):
        return state.set_faults(body.model_dump())

    @app.get("/admin/faults", tags=["admin"])
    async def get_faults():
        return state.get_faults()

    @app.get("/admin/stats", tags=["admin"])
    async def stats():
        with state.lock:
            return {
                "revision": state.revision,
                "head": state.head.as_dict(),
                "canonical_height": state.head.height,
                "block_count": len(state.blocks),
                "head_requests": state.stats["head_requests"],
                "block_requests": state.stats["block_requests"],
                "failed_requests": state.stats["failed_requests"],
                "block_requests_by_hash": dict(state.stats["block_requests_by_hash"]),
                "faults": dict(state.faults),
            }

    @app.get("/admin/canonical", tags=["admin"])
    async def canonical():
        with state.lock:
            return {
                "head": state.head.as_dict(),
                "blocks": [
                    {
                        "height": h,
                        "hash": state.canonical[h],
                        "parent_hash": state.blocks[state.canonical[h]].parent_hash,
                    }
                    for h in sorted(state.canonical)
                ],
            }

    return app


app = create_app()
