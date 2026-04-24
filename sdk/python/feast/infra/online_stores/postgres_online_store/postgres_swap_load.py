import logging
from typing import List, Tuple

from psycopg import sql
from psycopg.connection import Connection

logger = logging.getLogger(__name__)


def staging_table_name(table_name: str) -> str:
    return f"{table_name}_staging"


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


def begin_swap_load(conn: Connection, table_name: str) -> None:
    staging = staging_table_name(table_name)
    logger.info("swap_load: creating staging table %s", staging)
    with conn.cursor() as cur:
        cur.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(staging)))
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
        # Rename the indexes from their staging-based names to match the live table
        # name.  This is required so that subsequent swap-load runs can create
        # fresh indexes under the staging-based names without hitting a
        # DuplicateTable error (Postgres keeps index names when a table is renamed).
        cur.execute(
            sql.SQL("ALTER INDEX {} RENAME TO {}").format(
                sql.Identifier(f"{staging}_ek"),
                sql.Identifier(f"{table_name}_ek"),
            )
        )
        if has_string_features:
            cur.execute(
                sql.SQL("ALTER INDEX {} RENAME TO {}").format(
                    sql.Identifier(f"{staging}_fts_idx"),
                    sql.Identifier(f"{table_name}_fts_idx"),
                )
            )
    conn.commit()
    logger.info("swap_load: swap complete for %s", table_name)


def drop_staging(conn: Connection, table_name: str) -> None:
    staging = staging_table_name(table_name)
    logger.warning("swap_load: cleaning up staging table %s", staging)
    with conn.cursor() as cur:
        cur.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(staging)))
    conn.commit()
