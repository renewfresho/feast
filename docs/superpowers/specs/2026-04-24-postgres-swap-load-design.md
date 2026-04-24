# Postgres Swap Load Design

**Date:** 2026-04-24
**Author:** Rene Wooller
**Status:** Draft

## Summary

Add an opt-in `swap_load` mode to the PostgreSQL online store that replaces the row-by-row upsert during `materialize()` with an atomic table swap: bulk-load into a staging table, build indexes, then rename-swap in a single transaction. Reads are unaffected during the swap. `materialize_incremental()` is unaffected entirely.

## Motivation

The current upsert approach (`INSERT ... ON CONFLICT DO UPDATE`) writes rows one at a time (in batches), holds locks on individual rows during materialisation, and never removes stale entities. For large feature views this is slow and leaves deleted entities in the online store indefinitely.

The swap approach:
- Is faster for bulk loads (plain `INSERT`, no conflict resolution)
- Is atomic — readers never see a partially-loaded table
- Handles deletions correctly for free — entities absent from the offline store simply don't appear in the new table

## Scope

- Opt-in only via `swap_load` set to `True` in `PostgreSQLOnlineStoreConfig`
- Only applies to `materialize()` — `materialize_incremental()` is unchanged
- PostgreSQL online store only — no other backends affected

## Design

### Config

```python
class PostgreSQLOnlineStoreConfig(PostgreSQLConfig, VectorStoreConfig):
    type: Literal["postgres"] = "postgres"
    swap_load: bool = False
```

### New Module: `postgres_swap_load.py`

Lives alongside `postgres.py` in `sdk/python/feast/infra/online_stores/postgres_online_store/`.

Exposes three public functions representing the three phases of a swap load:

```python
def begin_swap_load(table_name: str, config: PostgreSQLOnlineStoreConfig) -> None:
    """Create the staging table. Called once before any batches are written."""

def write_batch(table_name: str, rows: list[tuple], config: PostgreSQLOnlineStoreConfig) -> None:
    """Insert a batch of rows into staging. Called once per batch."""

def commit_swap_load(table_name: str, config: PostgreSQLOnlineStoreConfig) -> None:
    """Build indexes on staging then atomically swap staging → active. Called once after all batches."""
```

**`begin_swap_load`:**
- `CREATE TABLE {table}_staging (LIKE {table} INCLUDING ALL)`

**`write_batch`:**
- Plain `INSERT` (no `ON CONFLICT`) into `{table}_staging`

**`commit_swap_load`:**
- Build indexes on staging:
  - Entity key index: `CREATE INDEX {staging}_ek ON {staging} (entity_key)`
  - GIN index (if string features present): `CREATE INDEX {staging}_fts_idx ON {staging} USING GIN (to_tsvector('english', value_text))`
  - Index builds are synchronous — `CREATE INDEX` blocks until complete
- Atomic swap in a single transaction:
  ```sql
  ALTER TABLE {table} RENAME TO {table}_old;
  ALTER TABLE {table}_staging RENAME TO {table};
  DROP TABLE {table}_old;
  ```

**On failure at any phase:**
- `DROP TABLE IF EXISTS {table}_staging`
- Log which step failed
- Re-raise original exception

### Changes to `postgres.py`

`online_write_batch()` branches on `swap_load`. When `swap_load=True` it lazily creates the staging table on the first batch, then inserts into it:

```python
if config.swap_load:
    if not staging_exists(table_name, config):
        begin_swap_load(table_name, config)
    write_batch(table_name, rows, config)
else:
    # existing upsert path unchanged
```

`PostgreSQLOnlineStore` also overrides `finalize_online_write()` (see below) to call `commit_swap_load()`. `update()` and the read path are untouched.

### New method on `OnlineStore` base class: `finalize_online_write()`

The batching loop in `LocalOutputNode.execute()` (`nodes.py`) calls `online_write_batch()` per batch with no signal for "this is the last one". A new method is added after the loop:

```python
# nodes.py — after the batching loop
for batch in batches:
    online_store.online_write_batch(...)

online_store.finalize_online_write(config=context.repo_config, table=self.feature_view)
```

The base `OnlineStore` class gets a default no-op implementation so all other stores (Redis, DynamoDB, SQLite, etc.) are completely unaffected:

```python
def finalize_online_write(self, config: RepoConfig, table: FeatureView) -> None:
    pass
```

`PostgreSQLOnlineStore` overrides it to call `commit_swap_load()` when `swap_load=True`, otherwise does nothing.

## Data Flow

```
materialize() called
    ├── swap_load=False
    │     └── online_write_batch() per batch → INSERT ... ON CONFLICT DO UPDATE
    └── swap_load=True
          ├── begin_swap_load()
          │     └── CREATE TABLE {table}_staging (LIKE {table} INCLUDING ALL)
          ├── online_write_batch() per batch
          │     └── INSERT rows into {table}_staging (no ON CONFLICT)
          └── commit_swap_load()
                ├── CREATE INDEX {staging}_ek ON {staging} (entity_key)
                ├── CREATE INDEX {staging}_fts_idx ... (if string features)
                ├── BEGIN TRANSACTION
                │     ALTER TABLE {table} RENAME TO {table}_old
                │     ALTER TABLE {table}_staging RENAME TO {table}
                │     DROP TABLE {table}_old
                └── COMMIT
```

## Error Handling

On any exception after staging is created:
- Log which step failed
- `DROP TABLE IF EXISTS {table}_staging`
- Re-raise the exception

The active table is never touched until the rename transaction commits. A failure at any prior step leaves the active table fully intact.

## Testing

### Unit tests — `tests/unit/infra/online_store/test_postgres_swap_load.py`

Mock the DB connection. Assert:
- Staging table created with correct name
- Rows inserted without `ON CONFLICT`
- Indexes replicated before swap
- Rename transaction executed in correct order
- Staging dropped on failure at each step
- `swap_load=False` takes the original upsert path (no regression)

### Integration tests — `tests/integration/online_store/test_postgres_versioning.py`

Using testcontainer (postgres:16). Assert:
- `materialize()` with `swap_load=True` produces correct data in active table
- No staging table left behind after successful run
- A second consecutive run also completes cleanly
- A failed mid-load leaves the active table untouched and no staging table behind

## Files Changed

| File | Change |
|------|--------|
| `sdk/python/feast/infra/online_stores/postgres_online_store/postgres_swap_load.py` | New — swap load logic |
| `sdk/python/feast/infra/online_stores/postgres_online_store/postgres.py` | Branch in `online_write_batch()`, override `finalize_online_write()` |
| `sdk/python/feast/infra/online_stores/postgres_online_store/postgresql.py` | Add `swap_load` config field |
| `sdk/python/feast/infra/online_stores/base.py` | Add no-op `finalize_online_write()` to base class |
| `sdk/python/feast/infra/compute_engines/local/nodes.py` | Call `finalize_online_write()` after batching loop |
| `sdk/python/tests/unit/infra/online_store/test_postgres_swap_load.py` | New — unit tests |
| `sdk/python/tests/integration/online_store/test_postgres_versioning.py` | Add swap load integration tests |
