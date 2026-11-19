"""Async access to the one SQLite file.

sqlite3 is a blocking library. Every call to it runs on one worker thread that owns the connection, and
callers await the result, so the event loop never waits on the disk. One thread also gives one connection
and a strict order of statements, which the per-step transactions rely on.

The store counts every INSERT, UPDATE and DELETE it runs. When CRASH_AFTER=<n> is set (only undo passes it
in), the process stops at once right after the n-th write, before the step it belongs to is committed.
SQLite discards that half-done step the next time the file is opened.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import sqlite3
from collections.abc import AsyncIterator, Callable, Iterable, Sequence
from concurrent.futures import ThreadPoolExecutor
from typing import Any, TypeVar

T = TypeVar("T")
Params = Sequence[Any]

CRASH_EXIT_CODE = 3


def _connect(path: str) -> sqlite3.Connection:
    # isolation_level=None: sqlite3 never opens a transaction on its own; every BEGIN/COMMIT is ours.
    conn = sqlite3.connect(path, isolation_level=None)
    conn.execute("PRAGMA foreign_keys = ON")  # an arrow can never point at a node that is not there
    # SQLite's default rollback journal: every committed step is in the one database file. A transaction
    # cut short by a crash leaves a journal next to it, and SQLite rolls the step back the next time the
    # file is opened.
    conn.execute("PRAGMA journal_mode = DELETE")
    conn.execute("PRAGMA synchronous = FULL")
    conn.execute("PRAGMA busy_timeout = 5000")
    return conn


def _fetchall(conn: sqlite3.Connection, sql: str, params: Params) -> list[tuple[Any, ...]]:
    return conn.execute(sql, params).fetchall()


def _fetchone(conn: sqlite3.Connection, sql: str, params: Params) -> tuple[Any, ...] | None:
    return conn.execute(sql, params).fetchone()


class Store:
    """One open database file."""

    def __init__(
        self, path: str, executor: ThreadPoolExecutor, conn: sqlite3.Connection, crash_after: int | None
    ) -> None:
        self.path = path
        self._executor = executor
        self._conn = conn
        self._crash_after = crash_after
        self.writes = 0

    @classmethod
    async def open(cls, path: str, *, crash_after: int | None = None) -> Store:
        executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="sqlite")
        loop = asyncio.get_running_loop()
        try:
            conn = await loop.run_in_executor(executor, _connect, path)
        except BaseException:
            executor.shutdown(wait=False)
            raise
        return cls(path, executor, conn, crash_after)

    async def close(self) -> None:
        try:
            await self._call(self._conn.close)
        finally:
            self._executor.shutdown(wait=True)

    async def _call(self, fn: Callable[..., T], *args: Any) -> T:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._executor, fn, *args)

    # ---- reads -------------------------------------------------------------------------------------

    async def fetchall(self, sql: str, params: Params = ()) -> list[tuple[Any, ...]]:
        return await self._call(_fetchall, self._conn, sql, params)

    async def fetchone(self, sql: str, params: Params = ()) -> tuple[Any, ...] | None:
        return await self._call(_fetchone, self._conn, sql, params)

    async def fetchval(self, sql: str, params: Params = ()) -> Any:
        row = await self.fetchone(sql, params)
        return None if row is None else row[0]

    # ---- writes (counted) --------------------------------------------------------------------------

    async def write(self, sql: str, params: Params = ()) -> int:
        """Run one INSERT, UPDATE or DELETE. Returns the number of rows it changed."""
        return await self._call(self._write, sql, params)

    async def write_many(self, sql: str, rows: Iterable[Params]) -> None:
        await self._call(self._write_many, sql, list(rows))

    # ---- statements that are not writes to the data -------------------------------------------------

    async def control(self, sql: str) -> None:
        """SAVEPOINT, RELEASE, ROLLBACK TO, temp-table DDL. Not counted."""
        await self._call(self._conn.execute, sql)

    async def script(self, sql: str) -> None:
        await self._call(self._conn.executescript, sql)

    @contextlib.asynccontextmanager
    async def transaction(self) -> AsyncIterator[None]:
        """One step: everything inside is saved together or not at all."""
        await self._call(self._conn.execute, "BEGIN IMMEDIATE")
        try:
            yield
        except BaseException:
            await self._call(self._conn.execute, "ROLLBACK")
            raise
        try:
            await self._call(self._conn.execute, "COMMIT")
        except BaseException:
            if self._conn.in_transaction:
                await self._call(self._conn.execute, "ROLLBACK")
            raise

    # ---- worker-thread side ------------------------------------------------------------------------

    def _write(self, sql: str, params: Params) -> int:
        if self._crash_after is not None and self.writes >= self._crash_after:
            self._crash()  # only reached with CRASH_AFTER=0
        cursor = self._conn.execute(sql, params)
        self.writes += 1
        if self._crash_after is not None and self.writes >= self._crash_after:
            self._crash()
        return cursor.rowcount

    def _write_many(self, sql: str, rows: list[Params]) -> None:
        if self._crash_after is None:
            self._conn.executemany(sql, rows)
            self.writes += len(rows)
            return
        for row in rows:  # count each row, so CRASH_AFTER can land inside a batch
            self._write(sql, row)

    def _crash(self) -> None:
        message = (
            f"graphundo: CRASH_AFTER={self._crash_after}: stopping after {self.writes} writes; "
            "the step in progress is not committed\n"
        )
        os.write(2, message.encode())
        os._exit(CRASH_EXIT_CODE)  # no cleanup, no commit, no rollback: like a killed process
