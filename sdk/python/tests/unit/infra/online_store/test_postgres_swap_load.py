"""Unit tests for postgres swap_load feature."""

from unittest.mock import MagicMock, patch

from feast.infra.online_stores.online_store import OnlineStore


class ConcreteStore(OnlineStore):
    """Minimal concrete subclass to test base class methods."""

    def online_write_batch(self, config, table, data, progress):
        pass

    def online_read(self, config, table, entity_keys, requested_features=None):
        return []

    def update(
        self,
        config,
        tables_to_delete,
        tables_to_keep,
        entities_to_delete,
        entities_to_keep,
        partial,
    ):
        pass

    def teardown(self, config, tables, entities):
        pass


class TestFinalizeOnlineWrite:
    def test_base_class_finalize_is_noop(self):
        store = ConcreteStore()
        config = MagicMock()
        table = MagicMock()
        # Should not raise
        store.finalize_online_write(config=config, table=table)


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

        assert (
            staging_table_name("myproject_driver_stats")
            == "myproject_driver_stats_staging"
        )

    def test_begin_swap_load_creates_staging_without_extra_indexes(self):
        from feast.infra.online_stores.postgres_online_store.postgres_swap_load import (
            begin_swap_load,
        )

        conn = self._make_conn()
        begin_swap_load(conn, "proj_driver_stats")

        executed_sql = " ".join(
            str(call.args[0])
            for call in conn.cursor.return_value.execute.call_args_list
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
            str(call.args[0])
            for call in conn.cursor.return_value.execute.call_args_list
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

        all_sql = [
            str(call.args[0])
            for call in conn.cursor.return_value.execute.call_args_list
        ]
        index_positions = [
            i for i, s in enumerate(all_sql) if "CREATE INDEX" in s.upper()
        ]
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
            str(call.args[0])
            for call in conn.cursor.return_value.execute.call_args_list
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
            str(call.args[0])
            for call in conn.cursor.return_value.execute.call_args_list
        )
        assert "GIN" not in all_sql

    def test_commit_swap_load_renames_in_order(self):
        from feast.infra.online_stores.postgres_online_store.postgres_swap_load import (
            commit_swap_load,
        )

        conn = self._make_conn()
        commit_swap_load(conn, "proj_driver_stats", has_string_features=False)

        all_sql = [
            str(call.args[0])
            for call in conn.cursor.return_value.execute.call_args_list
        ]
        # Find the table renames (not index renames)
        table_rename_calls = [
            s for s in all_sql if "ALTER TABLE" in s.upper() and "RENAME" in s.upper()
        ]
        assert len(table_rename_calls) >= 2
        # First: active → prev, Second: staging → active
        assert "proj_driver_stats_prev" in table_rename_calls[0]
        assert "proj_driver_stats_staging" in table_rename_calls[1]

    def test_drop_staging_drops_if_exists(self):
        from feast.infra.online_stores.postgres_online_store.postgres_swap_load import (
            drop_staging,
        )

        conn = self._make_conn()
        drop_staging(conn, "proj_driver_stats")

        executed_sql = " ".join(
            str(call.args[0])
            for call in conn.cursor.return_value.execute.call_args_list
        )
        assert "DROP TABLE IF EXISTS" in executed_sql
        assert "proj_driver_stats_staging" in executed_sql


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

    def test_swap_load_true_calls_begin_and_write_batch(self):
        from feast.infra.online_stores.postgres_online_store.postgres import (
            PostgreSQLOnlineStore,
        )

        store = PostgreSQLOnlineStore()
        config = self._make_repo_config(swap_load=True)
        table = MagicMock()
        table.features = []

        with (
            patch.object(store, "_get_conn") as mock_conn_ctx,
            patch(
                "feast.infra.online_stores.postgres_online_store.postgres.begin_swap_load"
            ) as mock_begin,
            patch(
                "feast.infra.online_stores.postgres_online_store.postgres.swap_write_batch"
            ) as mock_write,
        ):
            mock_conn = MagicMock()
            mock_conn_ctx.return_value.__enter__ = lambda s: mock_conn
            mock_conn_ctx.return_value.__exit__ = MagicMock(return_value=False)

            store.online_write_batch(config, table, [], None)

            mock_begin.assert_called_once()
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
        table.features = []

        with (
            patch.object(store, "_get_conn") as mock_conn_ctx,
            patch(
                "feast.infra.online_stores.postgres_online_store.postgres.commit_swap_load"
            ) as mock_commit,
        ):
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

        with patch(
            "feast.infra.online_stores.postgres_online_store.postgres.commit_swap_load"
        ) as mock_commit:
            store.finalize_online_write(config, table)
            mock_commit.assert_not_called()


class TestPrevRetention:
    def _run_commit(self, has_string_features=False):
        from feast.infra.online_stores.postgres_online_store import (
            postgres_swap_load as psl,
        )

        conn = MagicMock()
        cur = conn.cursor.return_value.__enter__.return_value
        psl.commit_swap_load(conn, "oc_fv", has_string_features=has_string_features)
        # Render each Composed query to real SQL text so assertions can pin
        # exact statement-level behavior (identifiers come out double-quoted).
        return [c.args[0].as_string() for c in cur.execute.call_args_list]

    def test_prev_generation_is_retained_not_dropped(self):
        statements = self._run_commit()
        joined = " ".join(statements)
        assert 'DROP TABLE IF EXISTS "oc_fv_prev"' in joined
        assert 'ALTER TABLE "oc_fv" RENAME TO "oc_fv_prev"' in joined
        # the live generation must never be dropped
        assert 'DROP TABLE "oc_fv_old"' not in joined
        assert 'RENAME TO "oc_fv_old"' not in joined

    def test_live_indexes_renamed_to_prev_before_staging_swap(self):
        statements = self._run_commit()
        joined = " ".join(statements)
        assert 'ALTER INDEX IF EXISTS "oc_fv_ek" RENAME TO "oc_fv_prev_ek"' in joined
        # staging index still takes the live name afterwards
        assert 'ALTER INDEX "oc_fv_staging_ek" RENAME TO "oc_fv_ek"' in joined
        # ordering: prev index rename must happen before staging index rename
        prev_idx = next(i for i, s in enumerate(statements) if "oc_fv_prev_ek" in s)
        stg_idx = next(
            i for i, s in enumerate(statements) if 'ALTER INDEX "oc_fv_staging_ek"' in s
        )
        assert prev_idx < stg_idx

    def test_fts_index_renamed_when_string_features(self):
        statements = self._run_commit(has_string_features=True)
        joined = " ".join(statements)
        assert (
            'ALTER INDEX IF EXISTS "oc_fv_fts_idx" RENAME TO "oc_fv_prev_fts_idx"'
            in joined
        )

    def test_prev_table_name_helper(self):
        from feast.infra.online_stores.postgres_online_store.postgres_swap_load import (
            prev_table_name,
        )

        assert prev_table_name("oc_fv") == "oc_fv_prev"
