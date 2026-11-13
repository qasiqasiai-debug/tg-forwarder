from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path

import aiosqlite

from .utils import now

SCHEMA = """
CREATE TABLE IF NOT EXISTS settings(
    key TEXT PRIMARY KEY, value TEXT
);
CREATE TABLE IF NOT EXISTS channels(
    id INTEGER PRIMARY KEY,            -- marked id (-100...)
    ref TEXT,
    username TEXT,
    title TEXT,
    added_at INTEGER NOT NULL,
    cursor INTEGER NOT NULL DEFAULT 0, -- последний обработанный id поста
    status TEXT NOT NULL DEFAULT 'pending',
    error TEXT
);
CREATE TABLE IF NOT EXISTS forwarded(
    channel_id INTEGER NOT NULL,
    msg_id INTEGER NOT NULL,
    first_id INTEGER NOT NULL,         -- id первого сообщения публикации (альбом = 1 публикация)
    forwarded_at INTEGER NOT NULL,
    PRIMARY KEY(channel_id, msg_id)
);
CREATE INDEX IF NOT EXISTS idx_forwarded_time ON forwarded(forwarded_at);
CREATE TABLE IF NOT EXISTS queue(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    channel_id INTEGER NOT NULL,
    first_id INTEGER NOT NULL,
    msg_ids TEXT NOT NULL,             -- JSON-список id
    msg_date INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    UNIQUE(channel_id, first_id)
);
"""


@dataclass
class QueueItem:
    id: int
    channel_id: int
    msg_ids: list[int]
    attempts: int


class Database:
    def __init__(self, path: Path):
        self.path = path
        self.conn: aiosqlite.Connection | None = None

    async def init(self) -> None:
        self.conn = await aiosqlite.connect(self.path)
        self.conn.row_factory = sqlite3.Row
        await self.conn.execute("PRAGMA journal_mode=WAL")
        await self.conn.execute("PRAGMA synchronous=NORMAL")
        await self.conn.executescript(SCHEMA)
        await self.conn.commit()

    async def close(self) -> None:
        if self.conn:
            await self.conn.close()
            self.conn = None

    # ---------- settings ----------
    async def get_setting(self, key: str, default: str | None = None) -> str | None:
        cur = await self.conn.execute("SELECT value FROM settings WHERE key=?", (key,))
        row = await cur.fetchone()
        return row["value"] if row else default

    async def set_setting(self, key: str, value: str) -> None:
        await self.conn.execute(
            "INSERT INTO settings(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )
        await self.conn.commit()

    # ---------- channels ----------
    async def add_channel(self, cid: int, ref: str, username: str | None,
                          title: str, cursor: int) -> None:
        await self.conn.execute(
            "INSERT OR REPLACE INTO channels(id,ref,username,title,added_at,cursor,status) "
            "VALUES(?,?,?,?,?,?, 'ok')",
            (cid, ref, username, title, now(), cursor),
        )
        await self.conn.commit()

    async def remove_channel(self, cid: int) -> None:
        await self.conn.execute("DELETE FROM channels WHERE id=?", (cid,))
        await self.conn.execute("DELETE FROM queue WHERE channel_id=?", (cid,))
        await self.conn.commit()

    async def list_channels(self) -> list[sqlite3.Row]:
        cur = await self.conn.execute("SELECT * FROM channels ORDER BY added_at, id")
        return await cur.fetchall()

    async def get_channel(self, cid: int) -> sqlite3.Row | None:
        cur = await self.conn.execute("SELECT * FROM channels WHERE id=?", (cid,))
        return await cur.fetchone()

    async def set_channel_status(self, cid: int, status: str, error: str | None = None,
                                 title: str | None = None) -> None:
        if title:
            await self.conn.execute(
                "UPDATE channels SET status=?, error=?, title=? WHERE id=?",
                (status, error, title, cid))
        else:
            await self.conn.execute(
                "UPDATE channels SET status=?, error=? WHERE id=?", (status, error, cid))
        await self.conn.commit()

    async def set_cursor(self, cid: int, cursor: int) -> None:
        await self.conn.execute(
            "UPDATE channels SET cursor=? WHERE id=? AND cursor<?", (cursor, cid, cursor))
        await self.conn.commit()

    async def count_channels(self) -> tuple[int, int]:
        """(ok, всего)"""
        cur = await self.conn.execute(
            "SELECT COUNT(*) AS total, COALESCE(SUM(status='ok'),0) AS ok FROM channels")
        r = await cur.fetchone()
        return int(r["ok"]), int(r["total"])

    # ---------- queue / dedup ----------
    async def is_seen(self, cid: int, ids: list[int]) -> bool:
        """Уже переслано или уже стоит в очереди."""
        q = ",".join("?" * len(ids))
        cur = await self.conn.execute(
            f"SELECT 1 FROM forwarded WHERE channel_id=? AND msg_id IN ({q}) LIMIT 1",
            (cid, *ids))
        if await cur.fetchone():
            return True
        cur = await self.conn.execute(
            "SELECT 1 FROM queue WHERE channel_id=? AND first_id=?", (cid, min(ids)))
        return await cur.fetchone() is not None

    async def enqueue(self, cid: int, ids: list[int], msg_date: int) -> bool:
        ids = sorted(ids)
        if await self.is_seen(cid, ids):
            return False
        cur = await self.conn.execute(
            "INSERT OR IGNORE INTO queue(channel_id,first_id,msg_ids,msg_date,created_at) "
            "VALUES(?,?,?,?,?)",
            (cid, ids[0], json.dumps(ids), msg_date, now()))
        await self.conn.commit()
        return cur.rowcount > 0

    async def next_item(self) -> QueueItem | None:
        cur = await self.conn.execute(
            "SELECT * FROM queue ORDER BY msg_date, id LIMIT 1")
        r = await cur.fetchone()
        if not r:
            return None
        return QueueItem(r["id"], r["channel_id"], json.loads(r["msg_ids"]), r["attempts"])

    async def queue_size(self) -> int:
        cur = await self.conn.execute("SELECT COUNT(*) AS c FROM queue")
        return int((await cur.fetchone())["c"])

    async def bump_attempt(self, item_id: int, error: str, count: bool = True) -> None:
        await self.conn.execute(
            "UPDATE queue SET attempts=attempts+?, last_error=? WHERE id=?",
            (1 if count else 0, error[:500], item_id))
        await self.conn.commit()

    async def finish_item(self, item: QueueItem, forwarded: bool = True) -> None:
        """Удаляет из очереди; при forwarded=True фиксирует как пересланное."""
        ts = now()
        if forwarded:
            first = min(item.msg_ids)
            await self.conn.executemany(
                "INSERT OR IGNORE INTO forwarded(channel_id,msg_id,first_id,forwarded_at) "
                "VALUES(?,?,?,?)",
                [(item.channel_id, m, first, ts) for m in item.msg_ids])
            await self.conn.execute(
                "INSERT INTO settings(key,value) VALUES('last_success_ts',?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (str(ts),))
        await self.conn.execute("DELETE FROM queue WHERE id=?", (item.id,))
        await self.conn.commit()

    async def clear_queue(self) -> int:
        cur = await self.conn.execute("DELETE FROM queue")
        await self.conn.commit()
        return cur.rowcount

    # ---------- stats ----------
    async def count_forwarded(self, since: int = 0) -> int:
        cur = await self.conn.execute(
            "SELECT COUNT(*) AS c FROM forwarded WHERE msg_id=first_id AND forwarded_at>=?",
            (since,))
        return int((await cur.fetchone())["c"])

    async def top_channels(self, since: int, limit: int = 10) -> list[sqlite3.Row]:
        cur = await self.conn.execute(
            "SELECT f.channel_id AS id, COALESCE(c.title, CAST(f.channel_id AS TEXT)) AS title, "
            "COUNT(*) AS n FROM forwarded f LEFT JOIN channels c ON c.id=f.channel_id "
            "WHERE f.msg_id=f.first_id AND f.forwarded_at>=? "
            "GROUP BY f.channel_id ORDER BY n DESC LIMIT ?", (since, limit))
        return await cur.fetchall()
