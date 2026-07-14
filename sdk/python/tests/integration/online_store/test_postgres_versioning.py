"""Integration tests for PostgreSQL online store feature view versioning.

Run with: pytest --integration sdk/python/tests/integration/online_store/test_postgres_versioning.py
"""

import shutil
from datetime import datetime, timedelta, timezone

import pytest

from feast import Entity, FeatureView
from feast.field import Field
from feast.infra.online_stores.postgres_online_store.postgres import (
    PostgreSQLOnlineStore,
)
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import RegistryConfig, RepoConfig
from feast.types import Float32, Int64
from feast.value_type import ValueType


def _make_feature_view(name="driver_stats", version="latest"):
    entity = Entity(
        name="driver_id",
        join_keys=["driver_id"],
        value_type=ValueType.INT64,
    )
    return FeatureView(
        name=name,
        entities=[entity],
        ttl=timedelta(days=1),
        schema=[
            Field(name="driver_id", dtype=Int64),
            Field(name="trips_today", dtype=Int64),
            Field(name="avg_rating", dtype=Float32),
        ],
        version=version,
    )


def _make_entity_key(driver_id: int) -> EntityKeyProto:
    entity_key = EntityKeyProto()
    entity_key.join_keys.append("driver_id")
    val = ValueProto()
    val.int64_val = driver_id
    entity_key.entity_values.append(val)
    return entity_key


def _write_and_read(store, config, fv, driver_id=1001, trips=42):
    entity_key = _make_entity_key(driver_id)
    val = ValueProto()
    val.int64_val = trips
    now = datetime.now(tz=timezone.utc)
    store.online_write_batch(
        config, fv, [(entity_key, {"trips_today": val}, now, now)], None
    )
    return store.online_read(config, fv, [entity_key], ["trips_today"])


@pytest.mark.integration
@pytest.mark.skipif(
    not shutil.which("docker"),
    reason="Docker not available",
)
class TestPostgresVersioningIntegration:
    """Integration tests for PostgreSQL versioning with a real database."""

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

    def _make_config(self, enable_versioning=False):
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
            ),
            registry=RegistryConfig(
                path="/tmp/test_pg_registry.pb",
                enable_online_feature_view_versioning=enable_versioning,
            ),
            entity_key_serialization_version=3,
        )

    def test_write_read_without_versioning(self):
        config = self._make_config(enable_versioning=False)
        store = PostgreSQLOnlineStore()
        fv = _make_feature_view()
        store.update(config, [], [fv], [], [], False)

        result = _write_and_read(store, config, fv)
        assert result[0][1] is not None
        assert result[0][1]["trips_today"].int64_val == 42

    def test_write_read_with_versioning_v1(self):
        config = self._make_config(enable_versioning=True)
        store = PostgreSQLOnlineStore()
        fv = _make_feature_view()
        fv.current_version_number = 1
        store.update(config, [], [fv], [], [], False)

        result = _write_and_read(store, config, fv)
        assert result[0][1] is not None
        assert result[0][1]["trips_today"].int64_val == 42

    def test_version_isolation(self):
        """Data written to v1 is not visible from v2."""
        config = self._make_config(enable_versioning=True)
        store = PostgreSQLOnlineStore()

        fv_v1 = _make_feature_view()
        fv_v1.current_version_number = 1
        store.update(config, [], [fv_v1], [], [], False)
        _write_and_read(store, config, fv_v1, driver_id=1001, trips=10)

        fv_v2 = _make_feature_view()
        fv_v2.current_version_number = 2
        store.update(config, [], [fv_v2], [], [], False)

        entity_key = _make_entity_key(1001)
        result = store.online_read(config, fv_v2, [entity_key], ["trips_today"])
        assert result[0] == (None, None)

        result = store.online_read(config, fv_v1, [entity_key], ["trips_today"])
        assert result[0][1] is not None
        assert result[0][1]["trips_today"].int64_val == 10

    def test_projection_version_tag_routes_to_correct_table(self):
        """projection.version_tag routes reads to the correct versioned table."""
        config = self._make_config(enable_versioning=True)
        store = PostgreSQLOnlineStore()

        fv_v1 = _make_feature_view()
        fv_v1.current_version_number = 1
        store.update(config, [], [fv_v1], [], [], False)
        _write_and_read(store, config, fv_v1, driver_id=1001, trips=100)

        fv_v2 = _make_feature_view()
        fv_v2.current_version_number = 2
        store.update(config, [], [fv_v2], [], [], False)
        _write_and_read(store, config, fv_v2, driver_id=1001, trips=200)

        fv_read = _make_feature_view()
        fv_read.projection.version_tag = 1
        entity_key = _make_entity_key(1001)
        result = store.online_read(config, fv_read, [entity_key], ["trips_today"])
        assert result[0][1]["trips_today"].int64_val == 100

        fv_read2 = _make_feature_view()
        fv_read2.projection.version_tag = 2
        result = store.online_read(config, fv_read2, [entity_key], ["trips_today"])
        assert result[0][1]["trips_today"].int64_val == 200

    def test_teardown_versioned_table(self):
        """teardown() drops the versioned table without error."""
        config = self._make_config(enable_versioning=True)
        store = PostgreSQLOnlineStore()

        fv = _make_feature_view()
        fv.current_version_number = 1
        store.update(config, [], [fv], [], [], False)
        _write_and_read(store, config, fv)

        # Should not raise
        store.teardown(config, [fv], [])

    def test_update_deletes_versioned_table(self):
        """update() with tables_to_delete correctly drops versioned tables."""
        config = self._make_config(enable_versioning=True)
        store = PostgreSQLOnlineStore()

        fv = _make_feature_view()
        fv.current_version_number = 1
        store.update(config, [], [fv], [], [], False)
        _write_and_read(store, config, fv, driver_id=1001, trips=50)

        # Delete the versioned table
        store.update(config, [fv], [], [], [], False)


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
                    ("feast_test_project_driver_stats_staging",),
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

        store.online_write_batch(
            config, fv, [(entity_key, {"trips_today": val}, now, now)], None
        )
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

        store.online_write_batch(
            config, fv, [(entity_key, {"trips_today": val}, now, now)], None
        )
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
        store.online_write_batch(
            config, fv, [(entity_key, {"trips_today": val1}, now, now)], None
        )
        store.finalize_online_write(config, fv)

        # Second run with different entity — entity 1003 should be gone after swap
        entity_key2 = _make_entity_key(1004)
        val2 = ValueProto()
        val2.int64_val = 20

        store.online_write_batch(
            config,
            fv,
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
        store.online_write_batch(
            config, fv, [(entity_key, {"trips_today": val1}, now, now)], None
        )
        store.finalize_online_write(config, fv)

        val2 = ValueProto()
        val2.int64_val = 200
        store.online_write_batch(
            config, fv, [(entity_key, {"trips_today": val2}, now, now)], None
        )
        store.finalize_online_write(config, fv)

        result = store.online_read(config, fv, [entity_key], ["trips_today"])
        assert result[0][1]["trips_today"].int64_val == 200
