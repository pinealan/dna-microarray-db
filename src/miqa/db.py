"""
Database helpers — raw SQL via psycopg3, no ORM.
"""

from typing import Any

import psycopg



class DBError(Exception):
    pass


def upsert_sample(
    conn: psycopg.Connection,
    *,
    repository_id: str,
    repository_sample_id: str,
    repository_series_id: str | None = None,
    platform_id: str | None = None,
    gender: str | None = None,
    age: str | None = None,
    tissue: str | None = None,
    disease: str | None = None,
    extraction_protocol: str | None = None,
    extras: dict[str, Any] | None = None,
) -> int:
    """
    Insert a sample row, ignoring conflicts on (repository_id, repository_sample_id).
    Returns the sample id (new or existing).
    """
    import json

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO sample (
                repository_id, repository_sample_id, repository_series_id,
                platform_id, gender, age, tissue, disease,
                extraction_protocol, extras
            ) VALUES (
                %s, %s, %s, %s, %s::gender, %s, %s, %s, %s, %s
            )
            ON CONFLICT (repository_id, repository_sample_id) DO NOTHING
            RETURNING id
            """,
            (
                repository_id,
                repository_sample_id,
                repository_series_id,
                platform_id,
                gender,
                age,
                tissue,
                disease,
                extraction_protocol,
                json.dumps(extras) if extras else None,
            ),
        )
        row = cur.fetchone()
        if row is not None:
            return row[0]

        # Row already existed — fetch its id
        cur.execute(
            "SELECT id FROM sample WHERE repository_id = %s AND repository_sample_id = %s",
            (repository_id, repository_sample_id),
        )
        row = cur.fetchone()
        if row is not None:
            return row[0]

        raise DBError('Could not find row that was expected to exist.')


def insert_idat_file(
    conn: psycopg.Connection,
    *,
    sample_id: int,
    source_url: str,
    s3_key: str | None = None,
    channel: str | None = None,
) -> int:
    """Insert an idat_file row. Returns the new id."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO idat_file (sample_id, source_url, s3_key, channel)
            VALUES (%s, %s, %s, %s)
            RETURNING id
            """,
            (sample_id, source_url, s3_key, channel),
        )
        return cur.fetchone()[0]


def mark_idat_uploaded(conn: psycopg.Connection, idat_id: int, s3_key: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE idat_file SET s3_key = %s, uploaded_at = now() WHERE id = %s",
            (s3_key, idat_id),
        )


def mark_idat_processed(conn: psycopg.Connection, idat_id: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE idat_file SET processed_at = now() WHERE id = %s",
            (idat_id,),
        )


def mark_idat_deleted(conn: psycopg.Connection, idat_id: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE idat_file SET deleted_at = now() WHERE id = %s",
            (idat_id,),
        )
