# PositionEngine Design Spec

## Goal

Build a `PositionEngine` module that provides low-latency, synchronously readable position state for strategies and future risk modules. It answers four categories of questions:

1. Filled position: how much token inventory is currently held.
2. Working / pending exposure: how much quantity is currently resting or pending on open orders.
3. Theoretical / worst-case position: what position could become if all working orders fill.
4. PnL basis: realized PnL and cost basis; unrealized/total PnL is computed by callers with their own mark price.

The design is intentionally limited to position state, exposure state, persistence, recovery, and reconciliation. Concrete risk limits, rejection rules, and strategy behavior are out of scope and should be designed separately.

## Naming and module boundaries

`PositionEngine` is the total module. It contains these internal components:

- `PositionIngestor`: the single writer that consumes position events, updates private state, publishes read cells, and enqueues persistence records.
- `PositionKeeper`: writer-private in-memory reducer state. It owns mutable maps and is not directly exposed to readers.
- `PositionReadHandle`: synchronous read API used by strategies and future risk modules.
- `PositionEntryReadCell`: hot-path read cell for one `(strategy_id, token_id)` entry or one global `token_id` entry.
- `PositionEntrySnapshot`: immutable value returned by a single-entry read.
- `PositionStore`: SQLite persistence, snapshot loading, journal replay, and reconciliation storage.

`PositionKeeper` remains the name of the in-memory state component, not the whole module.

## Scope and key decisions

Confirmed decisions:

- Maintain both strategy-level and global-level state.
  - Strategy key: `(strategy_id, token_id)`.
  - Global key: `token_id`.
- Use existing `token_id` as the instrument identifier.
- PositionEngine does not consume market midpoint updates.
- PositionEngine does not calculate unrealized PnL or total PnL.
- PositionEngine returns `cost_basis` and `realized_pnl`; callers compute unrealized PnL with their own mark price.
- Store only primitive state fields; do not store derived theoretical values.
- Store only `cost_basis`; do not store `avg_cost`.
- PositionEngine has a single writer. Writer-private state updates are lock-free and do not use atomics.
- Hot single-entry reads must be very low latency and strongly consistent for that entry.
- Range and whole-table snapshots may be weakly consistent.
- SQLite persistence is required, using an asynchronous writer thread/task and append-only journal records plus periodic snapshots.
- Recovery starts from the latest SQLite snapshot and replays journal records after that snapshot.
- Startup reconciliation queries exchange positions and same-day fills; if local and exchange state differ, exchange state is source of truth and the mismatch is recorded and alerted.
- Risk rules are out of scope for this design.

## State model

Each strategy entry and each global entry stores the same base fields:

- `filled_position`
- `cost_basis`
- `realized_pnl`
- `working_buy_exposure`
- `working_sell_exposure`
- `last_update_seq`
- `last_update_ts_ms`
- `degraded` or status bit inherited from the engine state

The following values are methods on `PositionEntrySnapshot`, not stored fields:

- `avg_cost()`:
  - returns `None` if `filled_position == 0`
  - otherwise returns `cost_basis / filled_position`
- `theoretical_min()`:
  - `filled_position - working_sell_exposure`
- `theoretical_max()`:
  - `filled_position + working_buy_exposure`
- `theoretical_net()`:
  - `filled_position + working_buy_exposure - working_sell_exposure`

Theoretical values may be negative. PositionEngine reports the value as-is. Future risk rules decide whether negative theoretical position is allowed.

## Cost and realized PnL accounting

Use average-cost accounting per token entry.

Buy fill:

- `filled_position += fill_qty`
- `cost_basis += fill_qty * fill_price`
- working buy exposure for that order decreases by the filled quantity

Sell fill:

- compute `avg_cost = cost_basis / filled_position` before reducing the position
- `realized_pnl += fill_qty * (sell_price - avg_cost)`
- `filled_position -= fill_qty`
- `cost_basis -= fill_qty * avg_cost`
- working sell exposure for that order decreases by the filled quantity

If a sell fill exceeds the local filled position, PositionEngine records the resulting state and marks the engine degraded until reconciliation confirms the exchange state. It does not clamp the value silently.

## Open order tracking

`PositionKeeper` must track remaining working quantity per local order so terminal events can release only the still-working quantity.

For each active local order, store:

- `strategy_id`
- `token_id`
- `local_order_id`
- optional exchange order id
- `side`
- `price`
- original size
- remaining working size
- current order state
- last update seq and timestamp

Working exposure is changed only from order state transitions that imply a durable exposure change:

- `Accepted` / `Open`: register the order and increase working exposure by remaining size.
- `PartialFill` / `Fill`: decrease order remaining size and corresponding working exposure by fill quantity.
- `Cancelled` / `Expired` / `RemoteRejected` / `LocalRejected` / `Failed`: release the remaining working quantity.
- `CancelRequested` / `CancelPending` / `Stale` / `UnknownPending`: do not release working exposure.

If both `Accepted` and `Open` can arrive for the same local order, only the first exposure-registering event increases working exposure. The later event updates metadata/state but does not double count.

## Input event model

`PositionIngestor` consumes a serial stream of `PositionEvent` values. The stream may be produced from Order Gateway events, SQLite journal replay, and reconciliation adjustments.

Core event types:

- `OrderWorkingRegistered`
  - Fields: `strategy_id`, `token_id`, `local_order_id`, optional `exchange_order_id`, `side`, `price`, `size`, `seq`, `ts_ms`, `source`, `recovery`.
  - Effect: create/update local order tracking and increase working exposure if not already registered.

- `OrderFillApplied`
  - Fields: `strategy_id`, `token_id`, `local_order_id`, optional `exchange_order_id`, `side`, `fill_qty`, `fill_price`, optional `cum_qty`, `seq`, `ts_ms`, `source`, `recovery`.
  - Effect: decrease working exposure and update filled/cost/realized.

- `OrderTerminalApplied`
  - Fields: `strategy_id`, `token_id`, `local_order_id`, terminal reason, `seq`, `ts_ms`, `source`, `recovery`.
  - Effect: release remaining working exposure and mark local order terminal.

- `ReconciliationAdjustment`
  - Fields: reconciliation id, exchange data boundary, local seq compared, before values, exchange values, adjustment reason, `seq`, `ts_ms`.
  - Effect: replace affected local state with exchange-authoritative values and record warning context.

- `SnapshotLoaded`
  - Internal startup event representing state loaded from SQLite snapshots before journal replay.

Market data is not a `PositionEvent` source.

## Writer model

`PositionIngestor` is the only component that mutates `PositionKeeper`.

Writer invariants:

- All state-changing events enter through the PositionIngestor command queue.
- `PositionKeeper` mutable state is writer-private.
- No mutable references, maps, or shared live state are exposed to readers.
- SQLite writer never reads live `PositionKeeper` state; it receives copied journal/snapshot records.
- Reconciliation results enter as normal PositionIngestor events and are serialized with live order events.

Writer-private updates are lock-free and do not require atomics. Synchronization exists only at module boundaries: input queues, persistence queues, and read-cell publication.

## Read model

PositionEngine exposes three read granularities.

### Single-entry snapshot

This is the hot path for strategy and future risk checks.

APIs:

- `get_entry(strategy_id, token_id) -> Option<PositionEntrySnapshot>`
- `get_global_entry(token_id) -> Option<PositionEntrySnapshot>`

Properties:

- Strong consistency for the single entry.
- Target latency is nanosecond-scale after the read cell has been located.
- Returns base fields only; derived values are methods on the returned snapshot.
- Does not allocate on the hot path after key lookup.

Implementation approach:

- `PositionIngestor` updates private `PositionKeeper` state.
- After updating an entry, it publishes that entry into a `PositionEntryReadCell`.
- The read cell is fixed-layout and contains only copyable numeric/status fields plus version metadata.
- Readers obtain a consistent single-entry snapshot using an entry-level seqlock/acquire validation protocol.
- The read cell must not expose or concurrently read `HashMap`, `String`, `Arc`, or heap-owned live structures.

Seqlock usage is restricted to fixed-layout read cells. Complex state remains writer-private.

### Range snapshot

API example:

- `scan_strategy(strategy_id) -> Vec<(token_id, PositionEntrySnapshot)>`

Properties:

- Weakly consistent across entries.
- Each returned entry is internally consistent.
- Different entries may come from different publication versions.
- Suitable for strategy scans and monitoring.
- Not intended to prove a strong global risk invariant.

### Whole-table snapshot

API example:

- `snapshot_all_weak() -> PositionTableSnapshot`

Properties:

- Weakly consistent across the table.
- Used for debug, logging, monitoring, and building periodic persistence snapshots.
- Strong full-table snapshots are out of scope for the first version.

## Queues and backpressure

PositionEngine still needs queues even though writer-private state updates are lock-free.

Required queues:

- PositionIngestor input queue: serializes multiple producers into the single writer.
- SQLite persistence queue: decouples hot state updates from disk latency.

The input queue is the module boundary between producers and the single writer. It is not used to protect shared mutable state.

Backpressure behavior:

- If the PositionIngestor input queue is full, producers should observe an error and the engine status should become degraded.
- If the SQLite persistence queue is full, PositionEngine marks itself degraded.
- While degraded, PositionEngine continues to serve reads with a degraded flag; future risk modules may reject new orders based on that status.

Concrete risk behavior while degraded is out of scope.

## SQLite persistence

Persistence uses SQLite with an asynchronous writer thread/task.

### `position_journal`

Append-only journal of state-changing events.

Required columns:

- `seq INTEGER PRIMARY KEY`
- `ts_ms INTEGER NOT NULL`
- `event_type TEXT NOT NULL`
- `scope_strategy_id TEXT`
- `token_id TEXT NOT NULL`
- `local_order_id TEXT`
- `exchange_order_id TEXT`
- `side TEXT`
- `qty TEXT`
- `price TEXT`
- `source TEXT NOT NULL`
- `recovery INTEGER NOT NULL`
- `payload_json TEXT NOT NULL`

Decimal values are stored as strings or scaled integers according to the project’s existing Decimal storage convention. The implementation plan should choose one convention consistently with existing storage code.

### `position_snapshots`

Periodic aggregate snapshots used as replay anchors.

Required columns:

- `snapshot_id INTEGER NOT NULL`
- `seq INTEGER NOT NULL`
- `ts_ms INTEGER NOT NULL`
- `scope_type TEXT NOT NULL` with values `strategy` or `global`
- `strategy_id TEXT`
- `token_id TEXT NOT NULL`
- `filled_position TEXT NOT NULL`
- `cost_basis TEXT NOT NULL`
- `realized_pnl TEXT NOT NULL`
- `working_buy_exposure TEXT NOT NULL`
- `working_sell_exposure TEXT NOT NULL`

Theoretical values and avg cost are not stored.

### `position_open_orders_snapshot`

Snapshot of open order remaining state at the same snapshot id.

Required columns:

- `snapshot_id INTEGER NOT NULL`
- `seq INTEGER NOT NULL`
- `strategy_id TEXT NOT NULL`
- `token_id TEXT NOT NULL`
- `local_order_id TEXT NOT NULL`
- `exchange_order_id TEXT`
- `side TEXT NOT NULL`
- `price TEXT NOT NULL`
- `original_size TEXT NOT NULL`
- `remaining_size TEXT NOT NULL`
- `local_state TEXT NOT NULL`

This table lets recovery release future terminal events correctly after loading a snapshot.

### `position_reconciliations`

Records exchange reconciliation attempts and adjustments.

Required columns:

- `reconciliation_id TEXT PRIMARY KEY`
- `started_at_ms INTEGER NOT NULL`
- `exchange_data_as_of_ms INTEGER NOT NULL`
- `last_local_seq_compared INTEGER NOT NULL`
- `status TEXT NOT NULL`
- `mismatch_count INTEGER NOT NULL`
- `adjustment_journal_seq INTEGER`
- `summary_json TEXT NOT NULL`
- `alert_message TEXT`

## Snapshot policy

Snapshots are written periodically by journal count and optionally by wall-clock interval.

Initial policy:

- create a snapshot every N journal events, where N is configurable
- create a snapshot on graceful shutdown if supported
- create a snapshot after a successful reconciliation adjustment

Snapshots are generated from PositionKeeper state by the single writer as copied records and sent to the SQLite writer queue. The SQLite writer stores snapshots in a transaction.

## Recovery flow

Startup recovery has four phases.

1. Load latest complete SQLite snapshot batch.
   - Restore strategy entries, global entries, and open order remaining state.
   - Do not restore theoretical values or avg cost because they are derived.

2. Replay journal after the snapshot seq.
   - Apply records in ascending seq order.
   - Detect seq gaps, corrupt payloads, and unknown event types.
   - On replay inconsistency, mark engine degraded and continue to reconciliation if possible.

3. Publish read cells.
   - After replay, publish all restored entries to their read cells.
   - Engine remains recovering until reconciliation completes.

4. Run exchange reconciliation.
   - Query exchange current positions.
   - Query exchange same-day fills.
   - Query exchange open orders if the API supports it.
   - Compare exchange state with local replay state at a recorded local seq boundary.
   - If different, apply a `ReconciliationAdjustment` through PositionIngestor.
   - Record reconciliation result and alert on mismatch.

Only after replay and reconciliation complete does the engine become `Live`.

## Reconciliation semantics

The exchange is source of truth.

When reconciliation finds a mismatch:

- Record the local state before adjustment.
- Record the exchange-authoritative state.
- Apply exchange values through `ReconciliationAdjustment`.
- Persist the adjustment in `position_journal`.
- Persist the reconciliation summary in `position_reconciliations`.
- Emit or log an alert because mismatch usually means a previous message was lost or not persisted.

Reconciliation must include a time/sequence boundary:

- `reconcile_started_at_ms`
- `exchange_data_as_of_ms`
- `last_local_seq_compared`

The adjustment applies only to the compared boundary. Live events after that boundary continue through the normal PositionIngestor stream.

## Engine status

PositionReadHandle should expose engine status to callers.

Statuses:

- `Recovering`: snapshot replay or startup reconciliation is not complete.
- `Live`: reads are usable and persistence is healthy.
- `Degraded`: reads are available but persistence/replay/reconciliation health is compromised.
- `Stopped`: writer is no longer running.

Single-entry snapshots include enough status metadata for callers to know whether the value came from a degraded engine.

## Integration with Order Gateway

Order Gateway events are one producer of PositionEngine input events.

Mapping:

- `Accepted` / `Open` -> `OrderWorkingRegistered`
- `PartialFill` / `Fill` -> `OrderFillApplied`
- `Cancelled` / `Expired` / `LocalRejected` / `RemoteRejected` / `Failed` -> `OrderTerminalApplied`
- `Stale` and unknown-pending states do not release exposure
- recovery events can be replayed into PositionEngine without double-writing live journal records

This spec does not define risk checks performed by Gateway. It only defines the state and read API future risk checks can use.

## Non-goals

The first PositionEngine version does not include:

- risk limits or order rejection rules
- unrealized PnL calculation
- total PnL calculation
- market midpoint ingestion
- strong consistent whole-table snapshot
- tax-lot, FIFO, or LIFO accounting
- cross-token portfolio margin
- external service API
- UI or dashboard

## Testing requirements for implementation plan

The implementation plan should use TDD and cover these behaviors:

- registering a buy order increases strategy and global working buy exposure
- registering a sell order increases strategy and global working sell exposure
- duplicate `Accepted`/`Open` registration does not double count exposure
- partial fill decreases working exposure and updates filled/cost for buys
- sell fill realizes PnL and reduces cost basis with average-cost accounting
- terminal order releases only remaining working exposure
- cancel-pending and stale events do not release exposure
- theoretical values are computed methods and are not stored in SQLite snapshots
- avg cost is computed from cost basis and filled position
- single-entry read returns a consistent snapshot while writer updates other entries
- strategy scan is weakly consistent across entries but each entry is internally consistent
- journal replay from a snapshot reconstructs entries and open order remaining state
- reconciliation mismatch applies exchange-authoritative adjustment and records alert context
- persistence queue full marks engine degraded

## Implementation constraints for the plan

The implementation plan should choose concrete mechanics within these constraints:

- Decimal SQLite encoding must match the existing storage module convention.
- The seqlock/read-cell representation must remain sound in Rust and must not expose concurrently mutated heap-owned state.
- Snapshot interval must be configurable.
- Exchange reconciliation should reuse existing client capabilities for same-day fills and open orders where available.
