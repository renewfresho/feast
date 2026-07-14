"""Unit tests for postgres swap_load feature."""

import re
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

    def test_stg_table_name(self):
        from feast.infra.online_stores.postgres_online_store.postgres_swap_load import (
            staging_table_name,
        )

        assert (
            staging_table_name("myproject_driver_stats")
            == "myproject_driver_stats_stg"
        )

    def test_begin_swap_load_creates_stg_without_extra_indexes(self):
        from feast.infra.online_stores.postgres_online_store.postgres_swap_load import (
            begin_swap_load,
        )

        conn = self._make_conn()
        begin_swap_load(conn, "proj_driver_stats")

        executed_sql = " ".join(
            str(call.args[0])
            for call in conn.cursor.return_value.execute.call_args_list
        )
        assert "proj_driver_stats_stg" in executed_sql
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
        assert "proj_driver_stats_stg" in executed_sql
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
        assert "proj_driver_stats_prv" in table_rename_calls[0]
        assert "proj_driver_stats_stg" in table_rename_calls[1]

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
        assert "proj_driver_stats_stg" in executed_sql


class TestLongIdentifierNaming:
    """Postgres silently truncates identifiers over 63 bytes (NAMEDATALEN -
    1) instead of rejecting them. A previous version of this module fell
    into exactly that trap in production: a table_name long enough that
    naively appending "_staging_ek" truncated back down to exactly the bare
    "_staging" name, so `CREATE INDEX "{staging}_ek" ON "{staging}"`
    collided with its own table -- surfaced as
    `psycopg.errors.DuplicateTable: relation "..._staging" already exists`,
    with the "_ek" silently gone before Postgres ever reported it.

    _identifier fixes this deliberately instead of relying on truncation:
    names that already fit are left untouched; names that don't get a short
    hash of the *original* table_name appended ahead of the suffix, with
    table_name itself shortened by removing interior vowels (then, if still
    too long, interior consonants) -- always protecting each "_"-separated
    word's own first/last character -- working from the end of the string
    backward.
    """

    # Exactly reproduces the production incident (its "_staging" form was
    # precisely 63 bytes under the old suffix words). The shorter "_stg"/
    # "_ek" suffixes used today no longer overflow for this table's actual
    # feature views (all-numeric dtypes -> has_string_features=False), but
    # a hypothetical string-featured view on this same table_name still
    # needs the full shortening mechanism -- see
    # test_production_name_only_needs_shortening_for_fts_idx below.
    LONG_TABLE_NAME = "order_confidence_order_confidence_windowed_agg_by_sc_bc"

    # A much longer synthetic name to exercise phase 2 (interior consonant
    # stripping), which LONG_TABLE_NAME alone never needs.
    VERY_LONG_TABLE_NAME = (
        "order_confidence_order_confidence_windowed_aggregate_metrics"
        "_by_selling_buying_company"
    )

    def _run_commit(self, table_name, has_string_features=False):
        from feast.infra.online_stores.postgres_online_store import (
            postgres_swap_load as psl,
        )

        conn = MagicMock()
        cur = conn.cursor.return_value.__enter__.return_value
        psl.commit_swap_load(
            conn, table_name, has_string_features=has_string_features
        )
        return [c.args[0].as_string() for c in cur.execute.call_args_list]

    def test_short_names_are_left_untouched(self):
        from feast.infra.online_stores.postgres_online_store.postgres_swap_load import (
            _identifier,
        )

        assert _identifier("proj_driver_stats", "_ek") == "proj_driver_stats_ek"

    def test_production_name_only_needs_shortening_for_fts_idx(self):
        # The shorter "_stg"/"_ek" suffixes alone already fit this exact
        # table_name -- confirms the fix doesn't over-trigger for the
        # actual production case, which has no string features.
        from feast.infra.online_stores.postgres_online_store.postgres_swap_load import (
            _identifier,
        )

        assert (
            _identifier(self.LONG_TABLE_NAME, "_ek")
            == f"{self.LONG_TABLE_NAME}_ek"
        )
        assert (
            _identifier(self.LONG_TABLE_NAME, "_stg")
            == f"{self.LONG_TABLE_NAME}_stg"
        )
        # but the GIN index suffix is long enough to still need shortening
        fts = _identifier(self.LONG_TABLE_NAME, "_stg_fts_idx")
        assert fts != f"{self.LONG_TABLE_NAME}_stg_fts_idx"
        assert len(fts) <= 63

    def test_over_limit_names_stay_within_the_limit(self):
        from feast.infra.online_stores.postgres_online_store.postgres_swap_load import (
            _MAX_IDENTIFIER_LEN,
            _identifier,
        )

        assert len(self.VERY_LONG_TABLE_NAME) + len("_stg") > _MAX_IDENTIFIER_LEN
        result = _identifier(self.VERY_LONG_TABLE_NAME, "_stg")
        assert len(result) <= _MAX_IDENTIFIER_LEN

    def test_identifier_is_deterministic(self):
        from feast.infra.online_stores.postgres_online_store.postgres_swap_load import (
            _identifier,
        )

        assert _identifier(self.VERY_LONG_TABLE_NAME, "_stg") == _identifier(
            self.VERY_LONG_TABLE_NAME, "_stg"
        )

    def test_first_and_last_char_of_each_word_are_protected(self):
        from feast.infra.online_stores.postgres_online_store.postgres_swap_load import (
            _strip_chars,
        )

        # "order": o-r-d-e-r. Protecting the word's first/last character
        # keeps the leading "o" and trailing "r", so only the interior "e"
        # is a removable vowel.
        assert _strip_chars("order", 1) == "ordr"
        # Even heavy stripping must never remove either boundary character.
        result = _strip_chars("order", 10)
        assert result[0] == "o"
        assert result[-1] == "r"

    def test_vowels_are_stripped_before_consonants(self):
        from feast.infra.online_stores.postgres_online_store.postgres_swap_load import (
            _strip_chars,
        )

        # "confidence": c-o-n-f-i-d-e-n-c-e. Exactly 3 interior vowels
        # (o, i, and the "e" at position 6 -- the trailing "e" is the
        # word's last character, protected) are removable before any
        # consonant is touched.
        assert _strip_chars("confidence", 3) == "cnfdnce"
        # the 4th removal, beyond available interior vowels, must be a
        # consonant (phase 2) -- one character shorter than the vowel-only
        # result, not a no-op.
        assert len(_strip_chars("confidence", 4)) == len("cnfdnce") - 1

    def test_stripping_works_from_the_end_backward(self):
        from feast.infra.online_stores.postgres_online_store.postgres_swap_load import (
            _strip_chars,
        )

        # Two identical words back-to-back: removing exactly as many
        # interior vowels as one word has must exhaust the *second*
        # occurrence first, leaving the first untouched.
        first, second = _strip_chars("confidence_confidence", 3).split("_")
        assert first == "confidence"
        assert second == "cnfdnce"

    def test_hash_prevents_collision_between_differently_shortened_names(self):
        # Two different original names whose _strip_chars output alone
        # would coincide (both collapse to "zz" once every interior
        # character is stripped) -- confirms the hash suffix is what keeps
        # their final identifiers apart, not the shortening itself.
        from feast.infra.online_stores.postgres_online_store.postgres_swap_load import (
            _identifier,
            _strip_chars,
        )

        name_a = "z" * 100
        name_b = "z" * 150
        assert _strip_chars(name_a, len(name_a) - 2) == _strip_chars(
            name_b, len(name_b) - 2
        )
        assert _identifier(name_a, "_ek") != _identifier(name_b, "_ek")

    def test_index_name_never_collides_with_its_own_table(self):
        from feast.infra.online_stores.postgres_online_store.postgres_swap_load import (
            staging_table_name,
        )

        staging = staging_table_name(self.VERY_LONG_TABLE_NAME)
        statements = self._run_commit(self.VERY_LONG_TABLE_NAME)
        create_index = next(s for s in statements if "CREATE INDEX" in s.upper())
        assert f'ON "{staging}"' in create_index
        assert f'INDEX "{staging}"' not in create_index

    def test_all_generated_identifiers_within_postgres_limit(self):
        # table_name itself is excluded: it's fixed by upstream feast's own
        # naming (out of this module's control) and legitimately appears
        # unmodified as the "rename FROM" side of one statement -- only the
        # identifiers *this module derives* (staging/prev/_ek/_fts_idx) are
        # what _identifier is responsible for keeping within budget.
        statements = self._run_commit(
            self.VERY_LONG_TABLE_NAME, has_string_features=True
        )
        for stmt in statements:
            for identifier in re.findall(r'"([^"]+)"', stmt):
                if identifier == self.VERY_LONG_TABLE_NAME:
                    continue
                assert len(identifier) <= 63, f"{identifier!r} exceeds 63 bytes"

    def test_rename_pair_uses_matching_shortened_index_name(self):
        # The "_ek" index renamed away from the live table must be the
        # exact same shortened name a later swap independently recomputes
        # -- otherwise a stale, orphaned index is left behind on every swap
        # of a long-named feature view.
        from feast.infra.online_stores.postgres_online_store.postgres_swap_load import (
            _identifier,
        )

        statements = self._run_commit(self.VERY_LONG_TABLE_NAME)
        joined = " ".join(statements)
        table_ek = _identifier(self.VERY_LONG_TABLE_NAME, "_ek")
        assert f'RENAME TO "{table_ek}"' in joined
        assert f'ALTER INDEX IF EXISTS "{table_ek}" RENAME TO' in joined


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
        assert 'DROP TABLE IF EXISTS "oc_fv_prv"' in joined
        assert 'ALTER TABLE "oc_fv" RENAME TO "oc_fv_prv"' in joined
        # the live generation must never be dropped
        assert 'DROP TABLE "oc_fv_old"' not in joined
        assert 'RENAME TO "oc_fv_old"' not in joined

    def test_live_indexes_renamed_to_prev_before_stg_swap(self):
        statements = self._run_commit()
        joined = " ".join(statements)
        assert 'ALTER INDEX IF EXISTS "oc_fv_ek" RENAME TO "oc_fv_prv_ek"' in joined
        # staging index still takes the live name afterwards
        assert 'ALTER INDEX "oc_fv_stg_ek" RENAME TO "oc_fv_ek"' in joined
        # ordering: prev index rename must happen before staging index rename
        prev_idx = next(i for i, s in enumerate(statements) if "oc_fv_prv_ek" in s)
        stg_idx = next(
            i for i, s in enumerate(statements) if 'ALTER INDEX "oc_fv_stg_ek"' in s
        )
        assert prev_idx < stg_idx

    def test_fts_index_renamed_when_string_features(self):
        statements = self._run_commit(has_string_features=True)
        joined = " ".join(statements)
        assert (
            'ALTER INDEX IF EXISTS "oc_fv_fts_idx" RENAME TO "oc_fv_prv_fts_idx"'
            in joined
        )

    def test_prev_table_name_helper(self):
        from feast.infra.online_stores.postgres_online_store.postgres_swap_load import (
            prev_table_name,
        )

        assert prev_table_name("oc_fv") == "oc_fv_prv"


class TestFinalizeCleanupAfterFailedCommit:
    """A failed commit_swap_load leaves the connection mid-transaction in an
    error state. Reproduces a real production failure: the original cleanup
    code reused that same connection via _get_conn (which unconditionally
    calls set_autocommit before yielding) without rolling back first, so the
    cleanup attempt itself raised `psycopg.errors.ProgrammingError: can't
    change 'autocommit' now: connection in transaction status INERROR` --
    masking the original exception and skipping the intended drop_staging.
    """

    def _make_repo_config(self, conn_type=None):
        from feast.infra.online_stores.postgres_online_store.postgres import (
            PostgreSQLOnlineStoreConfig,
        )
        from feast.repo_config import RegistryConfig, RepoConfig

        kwargs = {}
        if conn_type is not None:
            kwargs["conn_type"] = conn_type
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
                swap_load=True,
                **kwargs,
            ),
            registry=RegistryConfig(path="/tmp/test.pb"),
            entity_key_serialization_version=3,
        )

    def _make_table(self):
        table = MagicMock()
        table.name = "driver_stats"
        table.projection.name_to_use.return_value = "driver_stats"
        table.projection.version_tag = None
        table.current_version_number = None
        table.features = []
        return table

    def test_connection_is_rolled_back_before_cleanup_reuses_it(self):
        from feast.infra.online_stores.postgres_online_store.postgres import (
            PostgreSQLOnlineStore,
        )

        store = PostgreSQLOnlineStore()
        store._conn = MagicMock()  # _get_conn runs for real against this mock
        config = self._make_repo_config()
        table = self._make_table()

        with (
            patch(
                "feast.infra.online_stores.postgres_online_store.postgres.commit_swap_load",
                side_effect=RuntimeError("boom"),
            ),
            patch(
                "feast.infra.online_stores.postgres_online_store.postgres.drop_staging"
            ) as mock_drop,
        ):
            try:
                store.finalize_online_write(config, table)
            except RuntimeError:
                pass

        mock_drop.assert_called_once()
        call_names = [c[0] for c in store._conn.method_calls]
        # set_autocommit is called once before commit_swap_load runs (and
        # fails), then again when the cleanup path reuses the connection.
        # rollback must land strictly between those two -- calling
        # set_autocommit on a connection still mid-failed-transaction is
        # exactly what raised the masking "can't change autocommit now"
        # error in production.
        set_autocommit_indices = [
            i for i, name in enumerate(call_names) if name == "set_autocommit"
        ]
        assert len(set_autocommit_indices) == 2
        assert call_names.index("rollback") < set_autocommit_indices[1]

    def test_original_exception_propagates_not_a_cleanup_failure(self):
        from feast.infra.online_stores.postgres_online_store.postgres import (
            PostgreSQLOnlineStore,
        )

        store = PostgreSQLOnlineStore()
        store._conn = MagicMock()
        config = self._make_repo_config()
        table = self._make_table()

        with (
            patch(
                "feast.infra.online_stores.postgres_online_store.postgres.commit_swap_load",
                side_effect=RuntimeError("original failure"),
            ),
            patch(
                "feast.infra.online_stores.postgres_online_store.postgres.drop_staging",
                side_effect=RuntimeError("cleanup also failed"),
            ),
        ):
            try:
                store.finalize_online_write(config, table)
                raised = None
            except RuntimeError as e:
                raised = e

        assert raised is not None
        assert str(raised) == "original failure"

    def test_pool_connections_skip_cleanup_without_raising(self):
        from feast.infra.online_stores.postgres_online_store.postgres import (
            PostgreSQLOnlineStore,
        )
        from feast.infra.utils.postgres.postgres_config import ConnectionType

        store = PostgreSQLOnlineStore()
        config = self._make_repo_config(conn_type=ConnectionType.pool)
        table = self._make_table()

        with (
            patch.object(store, "_get_conn") as mock_get_conn,
            patch(
                "feast.infra.online_stores.postgres_online_store.postgres.commit_swap_load",
                side_effect=RuntimeError("original failure"),
            ),
            patch(
                "feast.infra.online_stores.postgres_online_store.postgres.drop_staging"
            ) as mock_drop,
        ):
            mock_conn = MagicMock()
            mock_get_conn.return_value.__enter__ = lambda s: mock_conn
            mock_get_conn.return_value.__exit__ = MagicMock(return_value=False)

            try:
                store.finalize_online_write(config, table)
                raised = None
            except RuntimeError as e:
                raised = e

        assert raised is not None
        assert str(raised) == "original failure"
        mock_drop.assert_not_called()
