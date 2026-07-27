from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Optional

import aiosqlite


DB_SCHEMA = """
CREATE TABLE IF NOT EXISTS plugins (
    id TEXT PRIMARY KEY,
    mode TEXT DEFAULT 'downstream',
    type TEXT NOT NULL,
    name TEXT NOT NULL,
    alias TEXT,
    enabled INTEGER DEFAULT 1,
    app_secret TEXT,
    app_version INTEGER DEFAULT 1,
    status INTEGER DEFAULT 0,
    last_heartbeat_at REAL,
    connected_at REAL,
    ws_session_id TEXT,
    capabilities TEXT DEFAULT '["text"]'
);

CREATE TABLE IF NOT EXISTS sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    upstream_user_id TEXT NOT NULL,
    plugin_id TEXT NOT NULL,
    plugin_session_id TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(upstream_user_id)
);

CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    upstream_user_id TEXT NOT NULL,
    plugin_id TEXT NOT NULL,
    direction TEXT NOT NULL,
    content_type TEXT NOT NULL,
    content TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""


class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._db: Optional[aiosqlite.Connection] = None

    async def init(self):
        os.makedirs(os.path.dirname(self.db_path) or ".", exist_ok=True)
        self._db = await aiosqlite.connect(self.db_path)
        self._db.row_factory = aiosqlite.Row
        await self._db.executescript(DB_SCHEMA)

        await self._migrate_v1()
        await self._db.commit()

    async def _migrate_v1(self):
        cur = await self._db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='downstreams'"
        )
        has_downstreams = await cur.fetchone()
        cur = await self._db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='plugins'"
        )
        has_plugins = await cur.fetchone()

        if has_downstreams and has_plugins:
            await self._db.execute(
                "INSERT OR IGNORE INTO plugins (id, mode, type, name, alias, enabled, app_secret, app_version, status, last_heartbeat_at, connected_at, ws_session_id) SELECT id, 'downstream', type, name, alias, enabled, app_secret, 1, status, last_heartbeat_at, connected_at, ws_session_id FROM downstreams"
            )
            await self._db.execute("DROP TABLE downstreams")
        elif has_downstreams:
            await self._db.execute("ALTER TABLE downstreams RENAME TO plugins")

        if has_downstreams or has_plugins:
            cols = await self._db.execute("PRAGMA table_info(plugins)")
            col_names = [row[1] for row in await cols.fetchall()]
            if "mode" not in col_names:
                await self._db.execute(
                    "ALTER TABLE plugins ADD COLUMN mode TEXT DEFAULT 'downstream'"
                )
            if "app_version" not in col_names:
                await self._db.execute(
                    "ALTER TABLE plugins ADD COLUMN app_version INTEGER DEFAULT 1"
                )
            if "capabilities" not in col_names:
                await self._db.execute(
                    "ALTER TABLE plugins ADD COLUMN capabilities TEXT DEFAULT '[\"text\"]'"
                )

    async def close(self):
        if self._db:
            await self._db.close()
            self._db = None

    async def upsert_plugin(
        self, plugin_id: str, mode: str, plugin_type: str, name: str,
        alias: str = "", enabled: bool = True, app_secret: str = "",
        app_version: int = 1, capabilities: list[str] | None = None,
    ):
        caps_json = json.dumps(capabilities or ["text"])
        await self._db.execute(
            """INSERT INTO plugins (id, mode, type, name, alias, enabled, app_secret, app_version, capabilities)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(id) DO UPDATE SET
                 mode=excluded.mode, type=excluded.type, name=excluded.name,
                 alias=excluded.alias, enabled=excluded.enabled,
                 app_secret=excluded.app_secret, app_version=excluded.app_version,
                 capabilities=excluded.capabilities""",
            (plugin_id, mode, plugin_type, name, alias, int(enabled), app_secret, app_version, caps_json),
        )
        await self._db.commit()

    async def update_plugin_status(
        self, plugin_id: str, status: int,
        ws_session_id: str = "", last_heartbeat_at: Optional[float] = None,
        connected_at: Optional[float] = None,
    ):
        sets = ["status = ?"]
        params: list[Any] = [status]
        if ws_session_id:
            sets.append("ws_session_id = ?")
            params.append(ws_session_id)
        if last_heartbeat_at is not None:
            sets.append("last_heartbeat_at = ?")
            params.append(last_heartbeat_at)
        if connected_at is not None:
            sets.append("connected_at = ?")
            params.append(connected_at)
        params.append(plugin_id)
        await self._db.execute(
            f"UPDATE plugins SET {', '.join(sets)} WHERE id = ?", params
        )
        await self._db.commit()

    async def get_plugin(self, plugin_id: str) -> Optional[dict]:
        cursor = await self._db.execute(
            "SELECT * FROM plugins WHERE id = ?", (plugin_id,)
        )
        row = await cursor.fetchone()
        if row:
            d = dict(row)
            d["capabilities"] = json.loads(d.get("capabilities") or '["text"]')
            return d
        return None

    async def get_all_plugins(self) -> list[dict]:
        cursor = await self._db.execute(
            "SELECT * FROM plugins ORDER BY name"
        )
        rows = await cursor.fetchall()
        result = []
        for r in rows:
            d = dict(r)
            d["capabilities"] = json.loads(d.get("capabilities") or '["text"]')
            result.append(d)
        return result

    async def get_plugin_by_alias(self, alias: str) -> Optional[dict]:
        cursor = await self._db.execute(
            "SELECT * FROM plugins WHERE alias = ?", (alias,)
        )
        row = await cursor.fetchone()
        return dict(row) if row else None

    async def get_downstreams(self) -> list[dict]:
        cursor = await self._db.execute(
            "SELECT * FROM plugins WHERE mode = 'downstream' ORDER BY name"
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    async def get_upstreams(self) -> list[dict]:
        cursor = await self._db.execute(
            "SELECT * FROM plugins WHERE mode = 'upstream' ORDER BY name"
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    async def get_session(self, user_id: str) -> Optional[dict]:
        cursor = await self._db.execute(
            "SELECT * FROM sessions WHERE upstream_user_id = ?", (user_id,)
        )
        row = await cursor.fetchone()
        return dict(row) if row else None

    async def upsert_session(
        self, user_id: str, plugin_id: str,
        plugin_session_id: str = ""
    ):
        await self._db.execute(
            """INSERT INTO sessions (upstream_user_id, plugin_id, plugin_session_id)
               VALUES (?, ?, ?)
               ON CONFLICT(upstream_user_id) DO UPDATE SET
                 plugin_id=excluded.plugin_id,
                 plugin_session_id=excluded.plugin_session_id,
                 updated_at=CURRENT_TIMESTAMP""",
            (user_id, plugin_id, plugin_session_id),
        )
        await self._db.commit()

    async def log_message(
        self, user_id: str, plugin_id: str,
        direction: str, content_type: str, content: str = ""
    ):
        await self._db.execute(
            """INSERT INTO messages (upstream_user_id, plugin_id, direction, content_type, content)
               VALUES (?, ?, ?, ?, ?)""",
            (user_id, plugin_id, direction, content_type, content),
        )
        await self._db.commit()
