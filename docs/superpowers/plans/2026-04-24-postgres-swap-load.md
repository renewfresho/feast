# Postgres Swap Load Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add opt-in `swap_load` mode to the PostgreSQL online store that atomically replaces the feature table via a staging table rename instead of row-by-row upsert.

**Architecture:** A new `postgres_swap_load.py` module owns the three-phase lifecycle (begin/write/commit). `PostgreSQLOnlineStore.online_write_batch()` routes into it when `swap_load=True`. A new `finalize_online_write()` method on the `OnlineStore` base class (no-op default) is called after the batch loop in `nodes.py`; `PostgreSQLOnlineStore` overrides it to execute the atomic rename swap.

**Tech Stack:** Python 3.12, psycopg3 (`psycopg`), pytest, testcontainers-python

---

## File Map

| File | Change |
|------|--------|
| `sdk/python/feast/infra/online_stores/online_store.py` | Add no-op `finalize_online_write()` to `OnlineStore` base |
| `sdk/python/feast/infra/online_stores/postgres_online_store/postgres.py` | Add `swap_load` config field; branch `online_write_batch()`; override `finalize_online_write()` |
| `sdk/python/feast/infra/online_stores/postgres_online_store/postgres_swap_load.py` | **New** — `staging_exists`, `begin_swap_load`, `write_batch`, `commit_swap_load`, `drop_staging` |
| `sdk/python/feast/infra/compute_engines/local/nodes.py` | Call `finalize_online_write()` after batch loop |
| `sdk/python/tests/unit/infra/online_store/test_postgres_swap_load.py` | **New** — unit tests for swap module and `online_write_batch` branching |
| `sdk/python/tests/integration/online_store/test_postgres_versioning.py` | Add swap load integration tests |

---

## Task 1: Add `finalize_online_write()` no-op to base class

**Files:**
- Modify: `sdk/python/feast/infra/online_stores/online_store.py:582`
- Test: `sdk/python/tests/unit/infra/online_store/test_postgres_swap_load.py` (new)

- [ ] **Step 1: Write the failing test**

Create `sdk/python/tests/unit/infra/online_store/test_postgres_swap_load.py`:

```python
"""Unit tests for postgres swap_load feature."""
from unittest.mock import MagicMock, patch

import pytest

from feast.infra.online_stores.online_store import OnlineStore


class ConcreteStore(OnlineStore):
    """Minimal concrete subclass to test base class methods."""
    def online_write_batch(self, config, table, data, progress): pass
    def online_read(self, config, table, entity_keys, requested_features=None): return []
    def update(self, config, tables_to_delete, tables_to_keep, entities_to_delete, entities_to_keep, partial): pass
    def teardown(self, config, tables, entities): pass


class TestFinalizeOnlineWrite:
    def test_base_class_finalize_is_noop(self):
        store = ConcreteStore()
        config = MagicMock()
        table = MagicMock()
        # Should not raise
        store.finalize_online_write(config=config, table=table)
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run python -m pytest sdk/python/tests/unit/infra/online_store/test_postgres_swap_load.py::TestFinalizeOnlineWrite -v
```

Expected: `FAILED` — `AttributeError: 'ConcreteStore' object has no attribute 'finalize_online_write'`

- [ ] **Step 3: Add `finalize_online_write()` to `OnlineStore` base class**

In `sdk/python/feast/infra/online_stores/online_store.py`, add after the `close()` method at line 586:

```python
    def finalize_online_write(
        self,
        config: "RepoConfig",
        table: FeatureView,
    ) -> None:
        pass
```

- [ ] **Step 4: Run test to verify it passes**

```bash
uv run python -m pytest sdk/python/tests/unit/infra/online_store/test_postgres_swap_load.py::TestFinalizeOnlineWrite -v
```

Expected: `PASSED`

- [ ] **Step 5: Commit**

```bash
git add sdk/python/feast/infra/online_stores/online_store.py \
        sdk/python/tests/unit/infra/online_store/test_postgres_swap_load.py
git commit -s -m "feat: add finalize_online_write no-op to OnlineStore base class"
```

---

## Task 2: Add `swap_load` config field

**Files:**
- Modify: `sdk/python/feast/infra/online_stores/postgres_online_store/postgres.py:48`
- Test: `sdk/python/tests/unit/infra/online_store/test_postgres_swap_load.py`

- [ ] **Step 1: Write the failing test**

Append to `test_postgres_swap_load.py`:

```python
class TestSwapLoadConfig:
    def test_swap_load_defaults_to_false(self):
        from feast.infra.online_stores.postgres_online_store.postgres import (
            PostgreSQLOnlineStoreConfig,
        )
        config = PostgreSQLOnlineStoreConfig(
            type="postgres",
            host="localhost",
            port=5432,
            database="test",
            user="root",
            password="test",  # pragma: allowlist secret
        )
        assert config.swap_load is False

    def test_swap_load_can_be_enabled(self):
        from feast.infra.online_stores.postgres_online_store.postgres import (
            PostgreSQLOnlineStoreConfig,
        )
        config = PostgreSQLOnlineStoreConfig(
            type="postgres",
            host="localhost",
            port=5432,
            database="test",
            user="root",
            password="test",  # pragma: allowlist secret
            swap_load=True,
        )
        assert config.swap_load is True
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run python -m pytest sdk/python/tests/unit/infra/online_store/test_postgres_swap_load.py::TestSwapLoadConfig -v
```

Expected: `FAILED` — `ValidationError` or `TypeError` — `swap_load` field does not exist.

- [ ] **Step 3: Add `swap_load` field to config**

In `sdk/python/feast/infra/online_stores/postgres_online_store/postgres.py`, replace lines 48–49:

```python
class PostgreSQLOnlineStoreConfig(PostgreSQLConfig, VectorStoreConfig):
    type: Literal["postgres"] = "postgres"
```

with:

```python
class PostgreSQLOnlineStoreConfig(PostgreSQLConfig, VectorStoreConfig):
    type: Literal["postgres"] = "postgres"
    swap_load: bool = False
```

- [ ] **Step 4: Run test to verify it passes**

```bash
uv run python -m pytest sdk/python/tests/unit/infra/online_store/test_postgres_swap_load.py::TestSwapLoadConfig -v
```

Expected: `PASSED`

- [ ] **Step 5: Commit**

```bash
git add sdk/python/feast/infra/online_stores/postgres_online_store/postgres.py \
        sdk/python/tests/unit/infra/online_store/test_postgres_swap_load.py
git commit -s -m "feat: add swap_load config field to PostgreSQLOnlineStoreConfig"
```

---

## Task 3: Create `postgres_swap_load.py` module

**Files:**
- Create: `sdk/python/feast/infra/online_stores/postgres_online_store/postgres_swap_load.py`
- Test: `sdk/python/tests/unit/infra/online_store/test_postgres_swap_load.py`

- [ ] **Step 1: Write failing tests for the module**

Append to `test_postgres_swap_load.py`:

```python
class TestSwapLoadModule:
    def _make_conn(self):
        conn = MagicMock()
        conn.cursor.return_value.__enter__ = lambda s: s
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        return conn

    def test_staging_table_name(self):
        from feast.infra.online_stores.postgres_online_store.postgres_swap_load import (
            staging_table_name,
        )
        assert staging_table_name("myproject_driver_stats") == "myproject_driver_stats_staging"

    def test_begin_swap_load_creates_staging_without_extra_indexes(self):
        from feast.infra.online_stores.postgres_online_store.postgres_swap_load import (
            begin_swap_load,
        )
        conn = self._make_conn()
        begin_swap_load(conn, "proj_driver_stats")

        executed_sql = " ".join(
            str(call.args[0]) for call in conn.cursor.return_value.execute.call_args_list
        )
        assert "proj_driver_stats_staging" in executed_sql
        assert "DROP TABLE IF EXISTS" in executed_sql
        assert "CREATE TABLE" in executed_sql
        assert "LIKE" in executed_sql
        # Must NOT copy extra indexes — we build them after load for bulk performance
        assert "INCLUDING ALL" not in executed_sql

    def test_write_batch_inserts_without_on_conflict(self):
        from feast.infra.online_stores.postgres_online_store.postgres_swap_load import (
            write_batch,
        )
        conn = self._make_conn()
        rows = [(b"key", "feature", b"val", None, None, "2024-01-01", None)]
        write_batch(conn, "proj_driver_stats", rows)

        executed_sql = " ".join(
            str(call.args[0]) for call in conn.cursor.return_value.execute.call_args_list
            + conn.cursor.return_value.executemany.call_args_list
        )
        assert "proj_driver_stats_staging" in executed_sql
        assert "ON CONFLICT" not in executed_sql

    def test_commit_swap_load_builds_indexes_before_rename(self):
        from feast.infra.online_stores.postgres_online_store.postgres_swap_load import (
            commit_swap_load,
        )
        conn = self._make_conn()
        commit_swap_load(conn, "proj_driver_stats", has_string_features=False)

        all_sql = [str(call.args[0]) for call in conn.cursor.return_value.execute.call_args_list]
        index_positions = [i for i, s in enumerate(all_sql) if "CREATE INDEX" in s.upper()]
        rename_positions = [i for i, s in enumerate(all_sql) if "RENAME" in s.upper()]
        assert index_positions, "Expected at least one CREATE INDEX call"
        assert rename_positions, "Expected at least two RENAME calls"
        # All indexes must be built before any rename
        assert max(index_positions) < min(rename_positions)

    def test_commit_swap_load_builds_gin_index_for_string_features(self):
        from feast.infra.online_stores.postgres_online_store.postgres_swap_load import (
            commit_swap_load,
        )
        conn = self._make_conn()
        commit_swap_load(conn, "proj_driver_stats", has_string_features=True)

        all_sql = " ".join(
            str(call.args[0]) for call in conn.cursor.return_value.execute.call_args_list
        )
        assert "GIN" in all_sql
        assert "to_tsvector" in all_sql

    def test_commit_swap_load_skips_gin_index_without_string_features(self):
        from feast.infra.online_stores.postgres_online_store.postgres_swap_load import (
            commit_swap_load,
        )
        conn = self._make_conn()
        commit_swap_load(conn, "proj_driver_stats", has_string_features=False)

        all_sql = " ".join(
            str(call.args[0]) for call in conn.cursor.return_value.execute.call_args_list
        )
        assert "GIN" not in all_sql

    def test_commit_swap_load_renames_in_order(self):
        from feast.infra.online_stores.postgres_online_store.postgres_swap_load import (
            commit_swap_load,
        )
        conn = self._make_conn()
        commit_swap_load(conn, "proj_driver_stats", has_string_features=False)

        all_sql = [str(call.args[0]) for call in conn.cursor.return_value.execute.call_args_list]
        rename_calls = [s for s in all_sql if "RENAME" in s.upper()]
        assert len(rename_calls) >= 2
        # staging → active rename must happen after active → old
        assert "proj_driver_stats_staging" in rename_calls[1]

    def test_drop_staging_drops_if_exists(self):
        from feast.infra.online_stores.postgres_online_store.postgres_swap_load import (
            drop_staging,
        )
        conn = self._make_conn()
        drop_staging(conn, "proj_driver_stats")

        executed_sql = " ".join(
            str(call.args[0]) for call in conn.cursor.return_value.execute.call_args_list
        )
        assert "DROP TABLE IF EXISTS" in executed_sql
        assert "proj_driver_stats_staging" in executed_sql
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run python -m pytest sdk/python/tests/unit/infra/online_store/test_postgres_swap_load.py::TestSwapLoadModule -v
```

Expected: `FAILED` — `ModuleNotFoundError: postgres_swap_load`

- [ ] **Step 3: Create the module**

Create `sdk/python/feast/infra/online_stores/postgres_online_store/postgres_swap_load.py`:

```python
import logging
from typing import List, Tuple

from psycopg import sql
from psycopg.connection import Connection

logger = logging.getLogger(__name__)


def staging_table_name(table_name: str) -> str:
    return f"{table_name}_staging"


def begin_swap_load(conn: Connection, table_name: str) -> None:
    staging = staging_table_name(table_name)
    logger.info("swap_load: creating staging table %s", staging)
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(staging))
        )
        # Copy schema and primary key constraint only — extra indexes (_ek, _fts_idx)
        # are built after bulk load in commit_swap_load for better insert performance.
        cur.execute(
            sql.SQL(
                "CREATE TABLE {} (LIKE {} INCLUDING DEFAULTS INCLUDING CONSTRAINTS)"
            ).format(
                sql.Identifier(staging),
                sql.Identifier(table_name),
            )
        )
    conn.commit()
    logger.info("swap_load: staging table %s created", staging)


def write_batch(
    conn: Connection,
    table_name: str,
    rows: List[Tuple],
) -> None:
    staging = staging_table_name(table_name)
    insert_query = sql.SQL(
        """
        INSERT INTO {}
        (entity_key, feature_name, value, value_text, vector_value, event_ts, created_ts)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
    ).format(sql.Identifier(staging))
    with conn.cursor() as cur:
        cur.executemany(insert_query, rows)
    conn.commit()


def commit_swap_load(
    conn: Connection, table_name: str, has_string_features: bool = False
) -> None:
    staging = staging_table_name(table_name)
    old = f"{table_name}_old"
    logger.info("swap_load: building indexes on %s", staging)
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("CREATE INDEX {} ON {} (entity_key)").format(
                sql.Identifier(f"{staging}_ek"),
                sql.Identifier(staging),
            )
        )
        if has_string_features:
            cur.execute(
                sql.SQL(
                    "CREATE INDEX {} ON {} USING GIN (to_tsvector('english', value_text))"
                ).format(
                    sql.Identifier(f"{staging}_fts_idx"),
                    sql.Identifier(staging),
                )
            )
        logger.info("swap_load: swapping %s -> %s", staging, table_name)
        cur.execute(
            sql.SQL("ALTER TABLE {} RENAME TO {}").format(
                sql.Identifier(table_name),
                sql.Identifier(old),
            )
        )
        cur.execute(
            sql.SQL("ALTER TABLE {} RENAME TO {}").format(
                sql.Identifier(staging),
                sql.Identifier(table_name),
            )
        )
        cur.execute(sql.SQL("DROP TABLE {}").format(sql.Identifier(old)))
    conn.commit()
    logger.info("swap_load: swap complete for %s", table_name)


def drop_staging(conn: Connection, table_name: str) -> None:
    staging = staging_table_name(table_name)
    logger.warning("swap_load: cleaning up staging table %s", staging)
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(staging))
        )
    conn.commit()
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run python -m pytest sdk/python/tests/unit/infra/online_store/test_postgres_swap_load.py::TestSwapLoadModule -v
```

Expected: all `PASSED`

- [ ] **Step 5: Commit**

```bash
git add sdk/python/feast/infra/online_stores/postgres_online_store/postgres_swap_load.py \
        sdk/python/tests/unit/infra/online_store/test_postgres_swap_load.py
git commit -s -m "feat: add postgres_swap_load module with begin/write/commit/drop functions"
```

---

## Task 4: Branch `online_write_batch()` and override `finalize_online_write()`

**Files:**
- Modify: `sdk/python/feast/infra/online_stores/postgres_online_store/postgres.py:99`
- Test: `sdk/python/tests/unit/infra/online_store/test_postgres_swap_load.py`

- [ ] **Step 1: Write failing tests**

Append to `test_postgres_swap_load.py`:

```python
class TestOnlineWriteBatchBranching:
    def _make_repo_config(self, swap_load: bool):
        from feast.infra.online_stores.postgres_online_store.postgres import (
            PostgreSQLOnlineStoreConfig,
        )
        from feast.repo_config import RegistryConfig, RepoConfig
        return RepoConfig(
            project="test_project",
            provider="local",
            online_store=PostgreSQLOnlineStoreConfig(
                type="postgres",
                host="localhost",
                port=5432,
                database="test",
                user="root",
                password="test",  # pragma: allowlist secret
                swap_load=swap_load,
            ),
            registry=RegistryConfig(path="/tmp/test.pb"),
            entity_key_serialization_version=3,
        )

    def test_swap_load_false_calls_upsert(self):
        from feast.infra.online_stores.postgres_online_store.postgres import (
            PostgreSQLOnlineStore,
        )
        store = PostgreSQLOnlineStore()
        config = self._make_repo_config(swap_load=False)
        table = MagicMock()
        table.features = []

        with patch.object(store, "_get_conn") as mock_conn_ctx:
            mock_conn = MagicMock()
            mock_conn_ctx.return_value.__enter__ = lambda s: mock_conn
            mock_conn_ctx.return_value.__exit__ = MagicMock(return_value=False)
            mock_cursor = MagicMock()
            mock_conn.cursor.return_value.__enter__ = lambda s: mock_cursor
            mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

            store.online_write_batch(config, table, [], None)

            # upsert path: executemany called on the cursor
            mock_cursor.executemany.assert_called_once()
            executed = str(mock_cursor.executemany.call_args[0][0])
            assert "ON CONFLICT" in executed

    def test_swap_load_true_calls_write_batch(self):
        from feast.infra.online_stores.postgres_online_store.postgres import (
            PostgreSQLOnlineStore,
        )
        store = PostgreSQLOnlineStore()
        config = self._make_repo_config(swap_load=True)
        table = MagicMock()
        table.features = []

        with patch.object(store, "_get_conn") as mock_conn_ctx, \
             patch("feast.infra.online_stores.postgres_online_store.postgres_swap_load.write_batch") as mock_write, \
             patch("feast.infra.online_stores.postgres_online_store.postgres_swap_load.staging_exists", return_value=True):
            mock_conn = MagicMock()
            mock_conn_ctx.return_value.__enter__ = lambda s: mock_conn
            mock_conn_ctx.return_value.__exit__ = MagicMock(return_value=False)

            store.online_write_batch(config, table, [], None)

            mock_write.assert_called_once()

    def test_finalize_calls_commit_swap_load_when_swap_load_true(self):
        from feast.infra.online_stores.postgres_online_store.postgres import (
            PostgreSQLOnlineStore,
        )
        store = PostgreSQLOnlineStore()
        config = self._make_repo_config(swap_load=True)
        table = MagicMock()
        table.name = "driver_stats"
        table.projection.name_to_use.return_value = "driver_stats"
        table.projection.version_tag = None
        table.current_version_number = None

        with patch.object(store, "_get_conn") as mock_conn_ctx, \
             patch("feast.infra.online_stores.postgres_online_store.postgres.commit_swap_load") as mock_commit:
            mock_conn = MagicMock()
            mock_conn_ctx.return_value.__enter__ = lambda s: mock_conn
            mock_conn_ctx.return_value.__exit__ = MagicMock(return_value=False)

            store.finalize_online_write(config, table)

            mock_commit.assert_called_once()

    def test_finalize_is_noop_when_swap_load_false(self):
        from feast.infra.online_stores.postgres_online_store.postgres import (
            PostgreSQLOnlineStore,
        )
        store = PostgreSQLOnlineStore()
        config = self._make_repo_config(swap_load=False)
        table = MagicMock()

        with patch("feast.infra.online_stores.postgres_online_store.postgres.commit_swap_load") as mock_commit:
            store.finalize_online_write(config, table)
            mock_commit.assert_not_called()
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run python -m pytest sdk/python/tests/unit/infra/online_store/test_postgres_swap_load.py::TestOnlineWriteBatchBranching -v
```

Expected: `FAILED`

- [ ] **Step 3: Add imports and branch `online_write_batch()` in `postgres.py`**

At the top of `postgres.py`, add to the imports after the existing `from feast.infra.online_stores` imports:

```python
from feast.infra.online_stores.postgres_online_store.postgres_swap_load import (
    begin_swap_load,
    commit_swap_load,
    drop_staging,
    staging_exists,
    write_batch as swap_write_batch,
)
```

Replace `online_write_batch()` (lines 99–171) with:

```python
def online_write_batch(
    self,
    config: RepoConfig,
    table: FeatureView,
    data: List[
        Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
    ],
    progress: Optional[Callable[[int], Any]],
) -> None:
    insert_values = []
    for entity_key, values, timestamp, created_ts in data:
        entity_key_bin = serialize_entity_key(
            entity_key,
            entity_key_serialization_version=config.entity_key_serialization_version,
        )
        timestamp = _to_naive_utc(timestamp)
        if created_ts is not None:
            created_ts = _to_naive_utc(created_ts)

        for feature_name, val in values.items():
            vector_val = None
            value_text = None

            if val.WhichOneof("val") == "string_val":
                value_text = val.string_val

            if config.online_store.vector_enabled:
                vector_val = get_list_val_str(val)
            insert_values.append(
                (
                    entity_key_bin,
                    feature_name,
                    val.SerializeToString(),
                    value_text,
                    vector_val,
                    timestamp,
                    created_ts,
                )
            )

    if config.online_store.swap_load:
        table_name = _table_id(
            config.project,
            table,
            config.registry.enable_online_feature_view_versioning,
        )
        with self._get_conn(config) as conn:
            if not staging_exists(conn, table_name):
                begin_swap_load(conn, table_name)
            swap_write_batch(conn, table_name, insert_values)
    else:
        sql_query = sql.SQL(
            """
            INSERT INTO {}
            (entity_key, feature_name, value, value_text, vector_value, event_ts, created_ts)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (entity_key, feature_name) DO
            UPDATE SET
                value = EXCLUDED.value,
                value_text = EXCLUDED.value_text,
                vector_value = EXCLUDED.vector_value,
                event_ts = EXCLUDED.event_ts,
                created_ts = EXCLUDED.created_ts;
        """
        ).format(
            sql.Identifier(
                _table_id(
                    config.project,
                    table,
                    config.registry.enable_online_feature_view_versioning,
                )
            )
        )
        with self._get_conn(config) as conn, conn.cursor() as cur:
            cur.executemany(sql_query, insert_values)
            conn.commit()

    if progress:
        progress(len(data))
```

Add `staging_exists` to `postgres_swap_load.py` (append to the file):

```python
def staging_exists(conn: Connection, table_name: str) -> bool:
    staging = staging_table_name(table_name)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT EXISTS (
                SELECT FROM pg_tables WHERE tablename = %s
            )
            """,
            (staging,),
        )
        row = cur.fetchone()
        return bool(row[0]) if row else False
```

Add `finalize_online_write()` override to `PostgreSQLOnlineStore` class in `postgres.py`, after `online_write_batch`:

```python
def finalize_online_write(
    self,
    config: RepoConfig,
    table: FeatureView,
) -> None:
    if not config.online_store.swap_load:
        return
    table_name = _table_id(
        config.project,
        table,
        config.registry.enable_online_feature_view_versioning,
    )
    has_string_features = any(
        f.dtype.to_value_type() == ValueType.STRING for f in table.features
    )
    try:
        with self._get_conn(config) as conn:
            commit_swap_load(conn, table_name, has_string_features)
    except Exception:
        with self._get_conn(config) as conn:
            drop_staging(conn, table_name)
        raise
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run python -m pytest sdk/python/tests/unit/infra/online_store/test_postgres_swap_load.py::TestOnlineWriteBatchBranching -v
```

Expected: all `PASSED`

- [ ] **Step 5: Run full unit test suite to check for regressions**

```bash
uv run python -m pytest sdk/python/tests/unit/infra/online_store/ -v
```

Expected: all previously passing tests still pass

- [ ] **Step 6: Commit**

```bash
git add sdk/python/feast/infra/online_stores/postgres_online_store/postgres.py \
        sdk/python/feast/infra/online_stores/postgres_online_store/postgres_swap_load.py \
        sdk/python/tests/unit/infra/online_store/test_postgres_swap_load.py
git commit -s -m "feat: branch online_write_batch and add finalize_online_write for swap_load"
```

---

## Task 5: Call `finalize_online_write()` in `nodes.py`

**Files:**
- Modify: `sdk/python/feast/infra/compute_engines/local/nodes.py:387`

- [ ] **Step 1: Add call after the batch loop**

In `sdk/python/feast/infra/compute_engines/local/nodes.py`, replace lines 387–396:

```python
            for batch in batches:
                rows_to_write = _convert_arrow_to_proto(
                    batch, self.feature_view, join_key_to_value_type
                )
                online_store.online_write_batch(
                    config=context.repo_config,
                    table=self.feature_view,
                    data=rows_to_write,
                    progress=lambda x: None,
                )
```

with:

```python
            for batch in batches:
                rows_to_write = _convert_arrow_to_proto(
                    batch, self.feature_view, join_key_to_value_type
                )
                online_store.online_write_batch(
                    config=context.repo_config,
                    table=self.feature_view,
                    data=rows_to_write,
                    progress=lambda x: None,
                )
            online_store.finalize_online_write(
                config=context.repo_config,
                table=self.feature_view,
            )
```

- [ ] **Step 2: Run unit tests to check nothing regressed**

```bash
uv run python -m pytest sdk/python/tests/unit/ -v -k "not image_utils and not rag_retriever"
```

Expected: all previously passing tests still pass

- [ ] **Step 3: Commit**

```bash
git add sdk/python/feast/infra/compute_engines/local/nodes.py
git commit -s -m "feat: call finalize_online_write after batch loop in LocalOutputNode"
```

---

## Task 6: Integration tests

**Files:**
- Modify: `sdk/python/tests/integration/online_store/test_postgres_versioning.py`

- [ ] **Step 1: Add integration tests**

Append to `sdk/python/tests/integration/online_store/test_postgres_versioning.py`:

```python
@pytest.mark.integration
@pytest.mark.skipif(
    not shutil.which("docker"),
    reason="Docker not available",
)
class TestPostgresSwapLoadIntegration:
    """Integration tests for swap_load mode with a real PostgreSQL database."""

    @pytest.fixture(autouse=True)
    def setup_postgres(self):
        try:
            from testcontainers.postgres import PostgresContainer
        except ImportError:
            pytest.skip("testcontainers[postgres] not installed")

        self.container = PostgresContainer(
            "postgres:16",
            username="root",
            password="testpass",  # pragma: allowlist secret
            dbname="test",
        ).with_exposed_ports(5432)
        self.container.start()
        self.port = self.container.get_exposed_port(5432)
        yield
        self.container.stop()

    def _make_config(self, swap_load: bool = True):
        from feast.infra.online_stores.postgres_online_store.postgres import (
            PostgreSQLOnlineStoreConfig,
        )
        return RepoConfig(
            project="test_project",
            provider="local",
            online_store=PostgreSQLOnlineStoreConfig(
                type="postgres",
                host="localhost",
                port=int(self.port),
                user="root",
                password="testpass",  # pragma: allowlist secret
                database="test",
                sslmode="disable",
                swap_load=swap_load,
            ),
            registry=RegistryConfig(
                path="/tmp/test_pg_swap_registry.pb",
                enable_online_feature_view_versioning=False,
            ),
            entity_key_serialization_version=3,
        )

    def _staging_exists(self, config) -> bool:
        from feast.infra.online_stores.postgres_online_store.postgres import (
            PostgreSQLOnlineStore,
        )
        store = PostgreSQLOnlineStore()
        with store._get_conn(config) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT EXISTS (SELECT FROM pg_tables WHERE tablename = %s)",
                    ("test_project_driver_stats_staging",),
                )
                row = cur.fetchone()
                return bool(row[0]) if row else False

    def test_swap_load_writes_and_reads_correctly(self):
        """swap_load=True materialises features readable via online_read."""
        config = self._make_config(swap_load=True)
        store = PostgreSQLOnlineStore()
        fv = _make_feature_view()
        store.update(config, [], [fv], [], [], False)

        entity_key = _make_entity_key(1001)
        val = ValueProto()
        val.int64_val = 99
        now = datetime.now(tz=timezone.utc)

        store.online_write_batch(config, fv, [(entity_key, {"trips_today": val}, now, now)], None)
        store.finalize_online_write(config, fv)

        result = store.online_read(config, fv, [entity_key], ["trips_today"])
        assert result[0][1] is not None
        assert result[0][1]["trips_today"].int64_val == 99

    def test_swap_load_no_staging_table_after_success(self):
        """Staging table is cleaned up after a successful swap."""
        config = self._make_config(swap_load=True)
        store = PostgreSQLOnlineStore()
        fv = _make_feature_view()
        store.update(config, [], [fv], [], [], False)

        entity_key = _make_entity_key(1002)
        val = ValueProto()
        val.int64_val = 55
        now = datetime.now(tz=timezone.utc)

        store.online_write_batch(config, fv, [(entity_key, {"trips_today": val}, now, now)], None)
        store.finalize_online_write(config, fv)

        assert not self._staging_exists(config)

    def test_swap_load_second_run_replaces_data(self):
        """A second swap_load run replaces previous data atomically."""
        config = self._make_config(swap_load=True)
        store = PostgreSQLOnlineStore()
        fv = _make_feature_view()
        store.update(config, [], [fv], [], [], False)

        entity_key = _make_entity_key(1003)
        now = datetime.now(tz=timezone.utc)

        # First run
        val1 = ValueProto()
        val1.int64_val = 10
        store.online_write_batch(config, fv, [(entity_key, {"trips_today": val1}, now, now)], None)
        store.finalize_online_write(config, fv)

        # Second run — new data, also entity 1004 which didn't exist before
        entity_key2 = _make_entity_key(1004)
        val2 = ValueProto()
        val2.int64_val = 20
        val3 = ValueProto()
        val3.int64_val = 30

        store.online_write_batch(
            config, fv,
            [(entity_key2, {"trips_today": val2}, now, now)],
            None,
        )
        store.finalize_online_write(config, fv)

        # Entity 1003 from first run should be gone (swap replaces entire table)
        result = store.online_read(config, fv, [entity_key], ["trips_today"])
        assert result[0] == (None, None)

        result2 = store.online_read(config, fv, [entity_key2], ["trips_today"])
        assert result2[0][1]["trips_today"].int64_val == 20

    def test_swap_load_false_uses_upsert(self):
        """swap_load=False retains existing upsert behaviour."""
        config = self._make_config(swap_load=False)
        store = PostgreSQLOnlineStore()
        fv = _make_feature_view()
        store.update(config, [], [fv], [], [], False)

        entity_key = _make_entity_key(2001)
        now = datetime.now(tz=timezone.utc)

        val1 = ValueProto()
        val1.int64_val = 100
        store.online_write_batch(config, fv, [(entity_key, {"trips_today": val1}, now, now)], None)
        store.finalize_online_write(config, fv)

        val2 = ValueProto()
        val2.int64_val = 200
        store.online_write_batch(config, fv, [(entity_key, {"trips_today": val2}, now, now)], None)
        store.finalize_online_write(config, fv)

        result = store.online_read(config, fv, [entity_key], ["trips_today"])
        assert result[0][1]["trips_today"].int64_val == 200
```

- [ ] **Step 2: Run integration tests to verify they pass**

```bash
uv run python -m pytest --integration sdk/python/tests/integration/online_store/test_postgres_versioning.py -v -k "postgres"
```

Expected: all new `TestPostgresSwapLoadIntegration` tests pass, existing `TestPostgresVersioningIntegration` tests still pass.

- [ ] **Step 3: Commit**

```bash
git add sdk/python/tests/integration/online_store/test_postgres_versioning.py
git commit -s -m "test: add integration tests for postgres swap_load"
```
