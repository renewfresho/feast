"""Swap-load helpers for the PostgreSQL online store.

Manual rollback after a bad swap (run in psql, single transaction):
    -- Tables live in the store's db_schema (or the connecting user's own
    -- schema if db_schema is unset), so put it on the search path first:
    SET search_path TO <db_schema or feast/postgres user>;
    BEGIN;
    ALTER TABLE {table} RENAME TO {table}_bad;
    ALTER TABLE {table}_prev RENAME TO {table};
    ALTER INDEX IF EXISTS {table}_prev_ek RENAME TO {table}_ek;
    COMMIT;
The next successful swap replaces {table}_prev and cleans this up.
{table}_bad is left behind for post-mortem; drop it manually once
investigated (the next successful swap only cleans up {table}_prev, not
{table}_bad).
"""

import logging
from typing import List, Tuple

from psycopg import sql
from psycopg.connection import Connection

logger = logging.getLogger(__name__)


def staging_table_name(table_name: str) -> str:
    return f"{table_name}_staging"


def prev_table_name(table_name: str) -> str:
    return f"{table_name}_prev"


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
    prev = prev_table_name(table_name)
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
        # Retain exactly one previous generation as {table}_prev for manual
        # rollback (rename {table} to {table}_bad, then {table}_prev to {table}).
        cur.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(prev)))
        cur.execute(
            sql.SQL("ALTER TABLE {} RENAME TO {}").format(
                sql.Identifier(table_name),
                sql.Identifier(prev),
            )
        )
        # Move the outgoing generation's indexes out of the way so the staging
        # indexes can take the live names. IF EXISTS: on the first-ever swap the
        # live table was created by the online store's update() and has no _ek.
        cur.execute(
            sql.SQL("ALTER INDEX IF EXISTS {} RENAME TO {}").format(
                sql.Identifier(f"{table_name}_ek"),
                sql.Identifier(f"{prev}_ek"),
            )
        )
        if has_string_features:
            cur.execute(
                sql.SQL("ALTER INDEX IF EXISTS {} RENAME TO {}").format(
                    sql.Identifier(f"{table_name}_fts_idx"),
                    sql.Identifier(f"{prev}_fts_idx"),
                )
            )
        cur.execute(
            sql.SQL("ALTER TABLE {} RENAME TO {}").format(
                sql.Identifier(staging),
                sql.Identifier(table_name),
            )
        )
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
    logger.info(
        "swap_load: swap complete for %s (previous kept as %s)", table_name, prev
    )


def drop_staging(conn: Connection, table_name: str) -> None:
    staging = staging_table_name(table_name)
    logger.warning("swap_load: cleaning up staging table %s", staging)
    with conn.cursor() as cur:
        cur.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(staging)))
    conn.commit()
