"""会话管理（用户 ↔ 下游绑定）"""

from __future__ import annotations

from typing import Optional

from core.database import Database


class SessionManager:
    def __init__(self, db: Database):
        self.db = db

    async def get_binding(self, user_id: str) -> Optional[str]:
        session = await self.db.get_session(user_id)
        if session:
            return session["plugin_id"]
        return None

    async def set_binding(self, user_id: str, plugin_id: str):
        await self.db.upsert_session(user_id, plugin_id)

    async def clear_binding(self, user_id: str):
        await self.db.upsert_session(user_id, "")
