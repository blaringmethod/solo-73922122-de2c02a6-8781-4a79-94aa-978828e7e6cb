# Confirmed transfers — chain-following delivery API

Delivers **confirmed** address transfers to address-behaviour analysis, keeping
the published chain view consistent with the chain source: reorgs and head
rollbacks withdraw abandoned and insufficiently confirmed transfers, and a slow
or late ingestion round can never overwrite a newer published view.

Two FastAPI + SQLite instances share one volume, plus a controllable local HTTP
chain source. Delivered as launchable Docker Compose.

## Quick start

```bash
docker compose up --build
```

| Service        | URL                          | Purpose                        |
| -------------- | ---------------------------- | ------------------------------ |
| API instance A | http://localhost:8000/docs   | Swagger: query + force a round |
| API instance A | http://localhost:8000/       | browser demo console           |
| API instance B | http://localhost:8001/docs   | second instance, same volume   |
| Chain source   | http://localhost:9100/docs   | advance / fork / rollback / faults |

Both API instances mount the same named volume `api-data` at `/data`; the
source persists to `source-data`. Restarting any container resumes from the
persisted chain view and progress — the chain is never re-collected.

Demo (UI or curl):

```bash
# force one complete ingestion round (also happens automatically each second)
curl -X POST http://localhost:8000/admin/sync

# query confirmed transfers for an address
curl 'http://localhost:8000/transfers?address=addr-alice'

# cause a reorg: replace heights 4..6 of the source chain
curl -X POST http://localhost:9100/admin/fork \
  -H 'content-type: application/json' \
  -d '{"from_height":4,"count":3,"transfers_per_block":2,"salt":"demo"}'
curl -X POST http://localhost:8000/admin/sync
curl 'http://localhost:8000/transfers?address=addr-alice'   # moved head + withdrawn keys

# roll the head back (withdraws even without new blocks)
curl -X POST http://localhost:9100/admin/rollback \
  -H 'content-type: application/json' -d '{"to_height":3}'
curl -X POST http://localhost:8000/admin/sync

# simulate source failures: the published view stays served
curl -X POST http://localhost:9100/admin/faults \
  -H 'content-type: application/json' -d '{"head_failures":2,"block_failures":2}'
curl -X POST http://localhost:8000/admin/sync
curl -X POST http://localhost:9100/admin/faults \
  -H 'content-type: application/json' -d '{}'

# simulate a failing state save on the source: the last saved chain stays
# served, the failed operation is invisible, and a retry is deterministic
curl -X POST http://localhost:9100/admin/faults \
  -H 'content-type: application/json' -d '{"save_failures":1}'
curl -X POST http://localhost:9100/admin/advance \
  -H 'content-type: application/json' -d '{"count":2}'   # 503, chain unchanged
curl http://localhost:9100/head                          # still the saved head
curl -X POST http://localhost:9100/admin/advance \
  -H 'content-type: application/json' -d '{"count":2}'   # retry succeeds
curl -X POST http://localhost:8000/admin/sync
```

Local (no Docker), Python 3.11+:

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt
pytest -q                      # 37 end-to-end tests

# terminals 1-3
SOURCE_STATE_PATH=/tmp/chain-source.json INITIAL_BLOCKS=6 \
  uvicorn chain_source.main:app --port 9100
DB_PATH=/tmp/chain.db CHAIN_SOURCE_URL=http://127.0.0.1:9100 INSTANCE_ID=api-a \
  uvicorn app.main:app --port 8000
DB_PATH=/tmp/chain.db CHAIN_SOURCE_URL=http://127.0.0.1:9100 INSTANCE_ID=api-b \
  uvicorn app.main:app --port 8001
```

## Delivery contract

* **Only confirmed transfers are served.** Confirmations are computed against
  the **published** head: `confirmations = head_height - block_height + 1`.
  The default minimum is 3, i.e. a transfer is served only when it is **at least
  two blocks below the published head**. The floor is enforced in config, the
  DB query, the Pydantic model and the HTTP query parameter — clients can raise
  it but never lower it (`?min_confirmations=2` → `422`).
* **Every address query returns the same complete chain view and its basis.**
  `GET /transfers?address=…` returns `chain_view` (view id, head, every block
  from genesis through head with hash/parent/tx_count), `head_basis` (the
  **published revision** the view is based on, head height/hash, timestamps),
  the confirmation policy, the address's confirmed transfers, and counts. Two
  different addresses in the same revision get byte-identical chain views.
* **Reorgs and rollbacks withdraw.** Abandoned-suffix blocks leave the
  `chain_blocks` view; and because confirmations are recomputed from the
  published head on every query, a rollback withdraws transfers that are still
  canonical but now too shallow **even if no block was fetched**. Withdrawal is
  global: a txid gone from the view is gone from every address answer.
* **Late rounds cannot overwrite newer revisions.** The observed head is fixed
  once per round. `commit_view` runs in one SQLite `BEGIN IMMEDIATE`
  transaction that compares the target revision against `published_revision`
  and refuses (`stale`) when a revision at least as new is already published.
* **Failures keep the published view.** Blocks are fetched into an immutable
  cache first; the published view and progress only change in the final commit.
  Any head/block/validation error leaves the previous view intact and the
  error recorded on `/chain/status` and `/admin/rounds`.
* **No refetch of the known prefix.** The follower walks the two heads down
  `parent_hash` to the common ancestor, cache first. Only changed-suffix blocks
  are fetched; abandoned branches stay cached and are reused if a branch
  returns. Blocks pulled before a crash are reused after restart.
* **Slow pulls do not block queries.** Normal API handlers are synchronous
  functions (threadpool), and the follower runs in its own thread on its own
  SQLite connections. A delayed source only delays the next commit.
* **Persisted view + progress, restart-safe.** `sync_state` (published revision,
  head, status, errors) and `chain_blocks` are written together in the same
  transaction. On restart the follower continues from the persisted head.
* **Two instances, one view.** Both replicas may run the follower. A SQLite
  lease (renewed during long rounds) elects one active follower at a time, and
  the revision guard makes concurrent rounds safe regardless of the lease.

## API

| Method | Path                       | Description |
| ------ | -------------------------- | ----------- |
| GET    | `/transfers?address=…`     | Confirmed transfers + full chain view + head basis |
| GET    | `/chain/blocks`            | Full published chain view + head basis |
| GET    | `/chain/status`            | Progress: revision, head, errors, lease, follower state |
| GET    | `/health`                  | Ops health for Compose |
| POST   | `/admin/sync`              | Run exactly one round synchronously and report it |
| GET    | `/admin/rounds`            | Recent round audit log |
| GET    | `/docs`                    | Swagger UI |
| GET    | `/`                        | Browser demo console |

The demo console has buttons for a round, an address query, advance, fork,
rollback, failure injection and slow-source delay. It calls source controls
through the same-origin `/demo/source/*` passthrough.

### Chain source

Blocks are immutable and fetched by hash; `/head` carries a monotonically
increasing `revision`. Control endpoints:

| Method | Path                | Description |
| ------ | ------------------- | ----------- |
| GET    | `/head`             | `{revision, height, hash}` |
| GET    | `/blocks/{hash}`    | `{height, hash, parent_hash, transfers[]}` |
| GET    | `/canonical/{height}` | Canonical block at height |
| POST   | `/admin/advance`    | Append `count` blocks |
| POST   | `/admin/fork`       | Replace the canonical suffix above `from_height` |
| POST   | `/admin/rollback`   | Move the head down to `to_height` |
| POST   | `/admin/faults`     | `delay_ms`, `head_failures`, `block_failures`, `save_failures`, `fail_all` |
| GET    | `/admin/stats`      | Per-block request counts (proves no refetching) |
| GET    | `/docs`             | Swagger UI |

A transfer is `{txid, from_address, to_address, amount}` with an integer
`amount`; `from`/`to` aliases are also emitted.

**Head changes are save-first.** `advance`/`fork`/`rollback` stage their
change, persist the resulting state (atomic tmp-file + rename), and only then
make the new head, revision and blocks visible. If the save fails (inject with
`save_failures`), the operation answers `503` and:

* `/head`, `/canonical/*` and `/blocks/*` keep serving the last **saved**
  chain — the unsaved revision is never exposed, so followers never publish a
  view the source cannot reproduce after a restart;
* blocks created by the failed operation stay invisible (the immutable
  by-hash cache only gains blocks once their state is persisted), while
  previously saved abandoned branches remain fetchable by hash;
* revision, nonce and tx serials are not consumed, so retrying the same
  operation — before or after a source restart — regenerates the identical
  revision and block hashes, provided nothing else succeeded in between.

The staging area holds only the blocks touched by the operation in flight, so
the extra memory needed to discard a failed change grows with that operation's
block count, not with the chain size. The state file format is unchanged
(`version: 1`), and existing state files load as before.

## Persistence schema

* `blocks` / `block_transfers` — immutable by-hash cache of every block ever
  fetched, including abandoned branches.
* `chain_blocks` — the published canonical height→hash view.
* `sync_state` — published revision, head, status, last round/error, round count.
* `ingest_lease` — one active follower across instances.
* `ingest_rounds` — bounded audit trail (last 500 rounds).

## Verified behaviour (tests)

`pytest` runs 37 end-to-end tests against real HTTP servers on ephemeral
ports and a real SQLite file. They cover: the two-block confirmation cutoff and
that it cannot be lowered; every address seeing the identical complete view;
reorg suffix-only update with no common-prefix refetch (asserted from
`/admin/stats` per-hash counters); global withdrawal of abandoned txids; head
rollback withdrawing both abandoned and now-too-shallow transfers; failures
leaving the published view intact; slow pulls not delaying queries; genuinely
late rounds rejected after a newer publish; two instances converging from a
shared volume; restart resuming without refetching; crash-cached blocks reused;
same-head revision bumps; source state persistence; and `/docs`/console
availability. The save-first guarantee is covered directly: failed
advance/fork/rollback saves leave `/head`, `/canonical/*`, `/blocks/*` and the
state file byte-identical; failed operations never consume revision/nonce/tx
serials; retries — including after a source restart — reproduce the identical
revision and block hashes; and a follower never publishes an unsaved revision
nor wedges on a retried one.

## Known limits

* SQLite with a shared WAL is intended for replicas on the **same host**
  (one Compose volume). It is not a multi-node database; use a real DB server
  for cross-host replicas.
* One global lease means only one follower is actively pulling at a time; the
  other replica serves reads and takes over after the lease expires
  (`INGEST_LEASE_SECONDS`, default 15 s).
* The chain source is a deterministic simulation, not a Bitcoin/EVM node; its
  "blocks" and txids are synthetic and its revision is local to the process
  state file.
* Confirmation depth counts blocks on top of the transaction's block inclusive
  of the head, per the documented formula; there is no separate finality rule.
* The demo console is intentionally unauthenticated and bound to the host's
  published ports; keep it off untrusted networks.
* The chain source rewrites its whole JSON state file on every head change
  (O(chain size) per save); the save is on the admin operation's critical
  path, which is exactly what makes the save-first guarantee possible.
  `save_failures` is a simulation hook — real disk errors surface the same
  way (the operation answers 503 and the last saved chain stays served).
* Docker was unavailable in the build sandbox, so the Compose definition and
  Dockerfiles were validated by YAML inspection and by running both ASGI apps
  under Uvicorn with the exact pinned requirements, not by an actual
  `docker compose up`. See "Verification performed" below.

## Verification performed

| Check | Result |
| --- | --- |
| `pytest` (37 end-to-end tests, real HTTP servers + real SQLite file) | pass, on the exact pinned dependency set from `requirements-dev.txt` (fastapi 0.115.6, uvicorn 0.34.0, httpx 0.28.1, pydantic 2.10.4, pytest 8.3.4) |
| Save-failure fault injection (`save_failures`) over live HTTP | advance/fork/rollback answer 503; `/head`, `/canonical/*`, `/blocks/*` and the state file stay at the last saved chain; retry after clearing the fault succeeds with identical revision and hashes |
| Source restart between failed save and retry | source resumes the last saved revision; the retried operation produces the same revision/hashes and the follower publishes it (no stale wedge) |
| Live three-service run (source + 2 API instances, compose topology, real sockets) | 42 checks passed: `/docs` on all three, initial follow, save-failure cycles for advance/fork/rollback, restart + deterministic retry, abandoned-branch fetch-by-hash, two-instance convergence, single lease owner |
| Both ASGI apps under Uvicorn with pinned deps (`chain_source.main:app`, `app.main:app`) | started, served `/health`, `/docs`, `/`, queries, followed source advances and a fork over real sockets |
| Two instances sharing one SQLite file | identical `/chain/blocks` views; only one held the lease; failed instance kept serving |
| Restart continuation | persisted view served before any round; subsequent rounds fetched only new blocks (per-hash counters on the source unchanged for known blocks) |
| `ruff check` / `ruff format` | clean |
| `docker compose up --build` | **not executed** — no Docker daemon in this environment |

Not implemented / out of scope: authentication and rate limiting on the API and
demo console, a real chain node adapter, cross-host replica coordination,
metrics/tracing, and pruning of the immutable block cache.
