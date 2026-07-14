"""Swap-load helpers for the PostgreSQL online store.

Manual rollback after a bad swap (run in psql, single transaction):
    -- Tables live in the store's db_schema (or the connecting user's own
    -- schema if db_schema is unset), so put it on the search path first:
    SET search_path TO <db_schema or feast/postgres user>;
    BEGIN;
    ALTER TABLE {table} RENAME TO {table}_bad;
    ALTER TABLE {table}_prv RENAME TO {table};
    ALTER INDEX IF EXISTS {table}_prv_ek RENAME TO {table}_ek;
    COMMIT;
The next successful swap replaces {table}_prv and cleans this up.
{table}_bad is left behind for post-mortem; drop it manually once
investigated (the next successful swap only cleans up {table}_prv, not
{table}_bad).

For a long feature-view name, {table}_ek may not literally be that string:
identifiers over 63 bytes are shortened by _identifier below (interior
vowels/consonants stripped, then a short hash of the full name appended)
rather than relying on Postgres's silent truncation. Check the real name
first with \\d {table} rather than assuming the literal "_ek" suffix.
"""

import hashlib
import logging
from typing import List, Tuple

from psycopg import sql
from psycopg.connection import Connection

logger = logging.getLogger(__name__)

# Postgres identifiers over 63 bytes (NAMEDATALEN - 1) are silently
# truncated, not rejected. For a sufficiently long `table_name`, naively
# appending a suffix (e.g. "_staging_ek") can truncate back down to exactly
# `table_name`'s own "_staging" form, making the index collide with its own
# table -- observed in production as "relation ...staging already exists"
# raised from a CREATE INDEX statement, with the "_ek" silently dropped
# before Postgres ever reported the error. _identifier below shortens
# deliberately instead of relying on that silent behavior.
_MAX_IDENTIFIER_LEN = 63
_HASH_LEN = 4
_VOWELS = frozenset("aeiou")


def _strip_chars(base: str, excess: int) -> str:
    """Shorten `base` by removing exactly `excess` characters, protecting
    each "_"-separated word's own first and last character (so "order"
    can shorten to "ordr" but never loses its boundary letters). Phase 1
    removes interior vowels; once those are exhausted, phase 2 removes
    interior consonants too. Both phases work from the end of the string
    backward, so words toward the end shorten first.
    """
    words = base.split("_")
    # (word_index, char_index, char, protected)
    chars = [
        (wi, ci, c, len(w) == 1 or ci in (0, len(w) - 1))
        for wi, w in enumerate(words)
        for ci, c in enumerate(w)
    ]
    removed: set = set()
    remaining = excess

    def remove_pass(is_target):
        nonlocal remaining
        if remaining <= 0:
            return
        for wi, ci, c, protected in reversed(chars):
            if remaining <= 0:
                return
            if protected or (wi, ci) in removed:
                continue
            if is_target(c):
                removed.add((wi, ci))
                remaining -= 1

    remove_pass(lambda c: c in _VOWELS)
    remove_pass(lambda c: c not in _VOWELS)

    return "_".join(
        "".join(c for ci, c in enumerate(w) if (wi, ci) not in removed)
        for wi, w in enumerate(words)
    )


def _identifier(table_name: str, suffix: str) -> str:
    """Derive a Postgres identifier from `table_name` and a logical suffix
    (e.g. "_stg", "_stg_ek"), guaranteed to fit the 63-byte identifier
    limit.

    Names that already fit are returned untouched (`table_name + suffix`).
    Only once that would exceed the limit does a short hash of the *full,
    original* `table_name` get appended ahead of `suffix`, and `table_name`
    itself get shortened via _strip_chars to make room. The hash is a pure
    function of `table_name` -- identical across processes and over time --
    so two different table names can never collide after shortening, and
    swap_load's rename choreography can independently recompute, on a later
    swap, the exact same names an earlier swap created.
    """
    plain = f"{table_name}{suffix}"
    if len(plain) <= _MAX_IDENTIFIER_LEN:
        return plain
    digest = hashlib.sha1(table_name.encode()).hexdigest()[:_HASH_LEN]
    tail = f"_{digest}{suffix}"
    budget = _MAX_IDENTIFIER_LEN - len(tail)
    base = _strip_chars(table_name, len(table_name) - budget)
    return f"{base}{tail}"


def staging_table_name(table_name: str) -> str:
    return _identifier(table_name, "_stg")


def prev_table_name(table_name: str) -> str:
    return _identifier(table_name, "_prv")


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
    # Every suffixed identifier below is derived directly from table_name
    # (never from the already-shortened `staging`/`prev` strings) so each
    # one is independently guaranteed to fit the 63-byte limit -- deriving
    # "_ek" from an already-shortened `staging` could reintroduce the exact
    # overflow _identifier exists to prevent.
    staging_ek = _identifier(table_name, "_stg_ek")
    table_ek = _identifier(table_name, "_ek")
    prev_ek = _identifier(table_name, "_prv_ek")
    logger.info("swap_load: building indexes on %s", staging)
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("CREATE INDEX {} ON {} (entity_key)").format(
                sql.Identifier(staging_ek),
                sql.Identifier(staging),
            )
        )
        if has_string_features:
            staging_fts_idx = _identifier(table_name, "_stg_fts_idx")
            cur.execute(
                sql.SQL(
                    "CREATE INDEX {} ON {} USING GIN (to_tsvector('english', value_text))"
                ).format(
                    sql.Identifier(staging_fts_idx),
                    sql.Identifier(staging),
                )
            )
        logger.info("swap_load: swapping %s -> %s", staging, table_name)
        # Retain exactly one previous generation as {table}_prv for manual
        # rollback (rename {table} to {table}_bad, then {table}_prv to {table}).
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
                sql.Identifier(table_ek),
                sql.Identifier(prev_ek),
            )
        )
        if has_string_features:
            table_fts_idx = _identifier(table_name, "_fts_idx")
            prev_fts_idx = _identifier(table_name, "_prv_fts_idx")
            cur.execute(
                sql.SQL("ALTER INDEX IF EXISTS {} RENAME TO {}").format(
                    sql.Identifier(table_fts_idx),
                    sql.Identifier(prev_fts_idx),
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
                sql.Identifier(staging_ek),
                sql.Identifier(table_ek),
            )
        )
        if has_string_features:
            cur.execute(
                sql.SQL("ALTER INDEX {} RENAME TO {}").format(
                    sql.Identifier(staging_fts_idx),
                    sql.Identifier(table_fts_idx),
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
