import json
import math
from typing import Any

from asyncpg import Connection
from fastapi import Depends
from fastapi import HTTPException
from loguru import logger  # noqa

import validator.utils.database.database_constants as dcst
from validator.db.src.sql.contenders import fetch_contender_by_hotkey, fetch_contender_by_hotkey_and_task
from validator.models import Task, TasksWithHotkeyDetails, TaskWithHotkeyDetails
from validator.db.src.database import PSQLDB


def normalise_float(float: float | None) -> float | None:
    if float is None:
        return 0.0

    if math.isnan(float):
        return None

    if math.isinf(float):
        float = 1e100 if float > 0 else -1e100
    return float


async def get_recent_tasks(
    psql_db: PSQLDB, hotkeys: list[str] | None = None, limit: int = 100, page: int = 1
) -> list[Task]:
    async with await psql_db.connection() as connection:
        if hotkeys is not None:
            rows = await connection.fetch(
                f"""
                SELECT * 
                FROM {dcst.TABLE_TASKS} 
                WHERE {dcst.NODE_HOTKEY} = ANY($1) 
                ORDER BY {dcst.CREATED_AT} DESC
                LIMIT $2
                OFFEST $3
                """,
                hotkeys, 
                limit,
                (page - 1) * limit
            )
        else:
            rows = await connection.fetch(
                f"""
                SELECT * 
                FROM {dcst.TABLE_TASKS}
                ORDER BY {dcst.CREATED_AT} DESC 
                LIMIT $1
                OFFSET $2
                """,
                limit,
                (page - 1) * limit
            )
    return [Task(**task) for task in rows]


async def get_recent_tasks_for_hotkey(
    psql_db: PSQLDB, hotkey: str, limit: int = 100, page: int = 1
) -> TasksWithHotkeyDetails:
    async with await psql_db.connection() as connection:
        tasks = await connection.fetch(
                f"""
                SELECT * 
                FROM {dcst.TABLE_TASKS} 
                WHERE {dcst.NODE_HOTKEY} = $1 
                ORDER BY {dcst.CREATED_AT} DESC
                LIMIT $2
                OFFSET $3
                """,
                hotkey, 
                limit,
                (page - 1) * limit
            )
        tasks = [Task(**task) for task in tasks]
        contender = await fetch_contender_by_hotkey(connection, hotkey)

    return TasksWithHotkeyDetails(tasks=tasks, contender=contender)
    

async def get_task_with_hotkey_details(
        psql_db: PSQLDB, task_id: int
        ) -> TaskWithHotkeyDetails:
    async with await psql_db.connection() as connection:
        task = await connection.fetchrow(
                f"""
                SELECT * 
                FROM {dcst.TABLE_TASKS} 
                WHERE {dcst.COLUMN_ID} = $1 
                ORDER BY {dcst.CREATED_AT} DESC
                """,
                task_id
            )
        task = Task(**task)
        contender = await fetch_contender_by_hotkey_and_task(connection, task.node_hotkey, task.task_name)
    return TaskWithHotkeyDetails(contender=contender, task=task)


async def store_latest_scores_url(url: str, psql_db: PSQLDB) -> None:
    async with await psql_db.connection() as connection:
        connection: Connection

        # First expire all existing URLs
        expire_query = f"""
            UPDATE {dcst.LATEST_SCORES_URL_TABLE}
            SET expired_at = NOW()
            WHERE expired_at IS NULL
        """
        await connection.execute(expire_query)

        # Then insert the new URL
        insert_query = f"""
            INSERT INTO {dcst.LATEST_SCORES_URL_TABLE} (url)
            VALUES ($1)
        """
        await connection.execute(insert_query, url)



async def get_latest_scores_url(psql_db: PSQLDB) -> str | None:
    async with await psql_db.connection() as connection:
        connection: Connection

        query = f"""
            SELECT url FROM {dcst.LATEST_SCORES_URL_TABLE} WHERE expired_at IS NULL ORDER BY created_at DESC LIMIT 1
        """
        return await connection.fetchval(query)