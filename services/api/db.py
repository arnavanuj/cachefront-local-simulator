import logging
import os
import time
from contextlib import contextmanager
from typing import Any

import mysql.connector
from mysql.connector import pooling

from metrics import DB_READS


LOGGER = logging.getLogger("cachefront.api.db")
REPLICA_LAG_SECONDS = float(os.getenv("REPLICA_LAG_SECONDS", "0"))


def _build_pool() -> pooling.MySQLConnectionPool:
    host = os.getenv("MYSQL_HOST", "mysql")
    port = int(os.getenv("MYSQL_PORT", "3306"))
    user = os.getenv("MYSQL_USER", "app_user")
    password = os.getenv("MYSQL_PASSWORD", "app_password")
    database = os.getenv("MYSQL_DATABASE", "appdb")

    last_error: Exception | None = None
    for attempt in range(1, 31):
        try:
            return pooling.MySQLConnectionPool(
                pool_name="cachefront_pool",
                pool_size=10,
                host=host,
                port=port,
                user=user,
                password=password,
                database=database,
            )
        except mysql.connector.Error as exc:
            last_error = exc
            LOGGER.warning("db_pool_retry attempt=%s error=%s", attempt, exc)
            time.sleep(2)

    raise RuntimeError(f"failed to create MySQL pool: {last_error}") from last_error


POOL = _build_pool()


@contextmanager
def get_cursor(dictionary: bool = True):
    connection = POOL.get_connection()
    cursor = connection.cursor(dictionary=dictionary)
    try:
        yield connection, cursor
    finally:
        cursor.close()
        connection.close()


def _apply_replica_lag_if_needed(user_id: int, apply_replica_lag: bool) -> None:
    if apply_replica_lag and REPLICA_LAG_SECONDS > 0:
        LOGGER.info("replica_lag_simulated user_id=%s lag_seconds=%s", user_id, REPLICA_LAG_SECONDS)
        time.sleep(REPLICA_LAG_SECONDS)


def fetch_user(user_id: int, apply_replica_lag: bool = True) -> dict[str, Any] | None:
    DB_READS.inc()
    LOGGER.info("db_read table=users user_id=%s apply_replica_lag=%s", user_id, apply_replica_lag)
    with get_cursor(dictionary=True) as (_, cursor):
        cursor.execute(
            """
            SELECT id, name, email, status, updated_at
            FROM users
            WHERE id = %s
            """,
            (user_id,),
        )
        row = cursor.fetchone()

    _apply_replica_lag_if_needed(user_id, apply_replica_lag)
    return row


def fetch_user_freshness(user_id: int) -> dict[str, Any] | None:
    with get_cursor(dictionary=True) as (_, cursor):
        cursor.execute(
            """
            SELECT id, updated_at
            FROM users
            WHERE id = %s
            """,
            (user_id,),
        )
        return cursor.fetchone()


def insert_user(user_id: int, fields: dict[str, Any]) -> dict[str, Any]:
    status = fields.get("status") or "active"
    with get_cursor(dictionary=True) as (connection, cursor):
        cursor.execute(
            """
            INSERT INTO users (id, name, email, status, updated_at)
            VALUES (%s, %s, %s, %s, NOW())
            """,
            (user_id, fields["name"], fields["email"], status),
        )
        connection.commit()

    return fetch_user(user_id, apply_replica_lag=False)


def update_user(user_id: int, fields: dict[str, Any]) -> dict[str, Any] | None:
    allowed_fields = {"name", "email", "status"}
    update_fields = {key: value for key, value in fields.items() if key in allowed_fields and value is not None}
    if not update_fields:
        return fetch_user(user_id, apply_replica_lag=False)

    assignments = ", ".join(f"{column} = %s" for column in update_fields)
    values = list(update_fields.values())
    values.append(user_id)

    with get_cursor(dictionary=True) as (connection, cursor):
        cursor.execute(
            f"""
            UPDATE users
            SET {assignments}, updated_at = NOW()
            WHERE id = %s
            """,
            values,
        )
        if cursor.rowcount == 0:
            connection.rollback()
            return None
        connection.commit()

    return fetch_user(user_id, apply_replica_lag=False)
