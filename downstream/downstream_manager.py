from __future__ import annotations

import asyncio
import time
import uuid
from typing import Any, Callable, Awaitable, Optional

from aiohttp import web

from core.database import Database
from core.models import PluginConnection, PluginStatus
from security.logger import SecurityLogger
from downstream.ws_protocol import (
    ConnectOK, ConnectError, MessageReceived, PingMessage,
    TYPE_MESSAGE_SEND, TYPE_PONG, TYPE_MESSAGE_TYPING,
)


class DownstreamManager:
    def __init__(
        self,
        db: Database,
        sec_logger: SecurityLogger,
        heartbeat_interval: int = 60,
        heartbeat_timeout: int = 300,
        log_fn: Callable[[str], None] = None,
        on_message: Callable[[str, dict], Awaitable[None]] = None,
    ):
        self.db = db
        self.sec_logger = sec_logger
        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_timeout = heartbeat_timeout
        self.log = log_fn or (lambda msg: print(f"[downstream] {msg}"))
        self.on_message = on_message

        self._connections: dict[str, list[PluginConnection]] = {}
        self._heartbeat_task: Optional[asyncio.Task] = None

    async def start(self):
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def stop(self):
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass

    async def connect(
        self, app_id: str, ws: web.WebSocketResponse, ip: str = ""
    ) -> ConnectOK | ConnectError:
        plugin = await self.db.get_plugin(app_id)
        if not plugin:
            self.sec_logger.auth_failed(ip, app_id, "unknown_app_id")
            return ConnectError(code=4002, message="unknown app_id")
        if not plugin["enabled"]:
            self.sec_logger.connection_refused(ip, reason="app_disabled")
            return ConnectError(code=4003, message="app disabled")

        session_id = f"s_{uuid.uuid4().hex[:12]}"
        now = time.time()
        conn = PluginConnection(app_id, "downstream", ws, session_id, ip, last_heartbeat=now, connected_at=now)

        if app_id not in self._connections:
            self._connections[app_id] = []
        self._connections[app_id].append(conn)

        await self.db.update_plugin_status(
            app_id, PluginStatus.ONLINE,
            ws_session_id=session_id,
            last_heartbeat_at=time.time(),
            connected_at=time.time(),
        )
        self.sec_logger.downstream_online(app_id, ip)
        count = len(self._connections[app_id])
        self.log(f"Downstream connected: {app_id} ip={ip} session={session_id} (total={count})")
        return ConnectOK(
            session_id=session_id,
            heartbeat_interval=self.heartbeat_interval,
        )

    async def disconnect(self, app_id: str, ws: Any = None):
        conns = self._connections.get(app_id, [])
        removed = None
        if ws is None:
            removed = conns.pop(0) if conns else None
            if not conns:
                self._connections.pop(app_id, None)
        else:
            for i, c in enumerate(conns):
                if c.ws is ws:
                    removed = conns.pop(i)
                    break
            if not conns:
                self._connections.pop(app_id, None)
        if removed:
            self.sec_logger.ws_closed(removed.ip, app_id)
            remaining = len(self._connections.get(app_id, []))
            if remaining == 0:
                await self.db.update_plugin_status(app_id, PluginStatus.OFFLINE)
            self.log(f"Downstream disconnected: {app_id} (remaining={remaining})")

    async def handle_message(self, app_id: str, data: dict):
        msg_type = data.get("type", "")

        if msg_type == TYPE_PONG:
            conns = self._connections.get(app_id, [])
            for conn in conns:
                if not conn.ws.closed:
                    conn.last_heartbeat = time.time()
            await self.db.update_plugin_status(
                app_id, PluginStatus.ONLINE,
                last_heartbeat_at=time.time(),
            )

        elif msg_type == TYPE_MESSAGE_SEND:
            if self.on_message:
                await self.on_message(app_id, data)

        elif msg_type == TYPE_MESSAGE_TYPING:
            if self.on_message:
                await self.on_message(app_id, data)

        else:
            if self.on_message:
                await self.on_message(app_id, data)

    async def send_to_downstream(self, app_id: str, message: MessageReceived) -> bool:
        conns = self._connections.get(app_id, [])
        if not conns:
            return False
        sent = False
        for conn in conns[:]:
            if not conn.ws.closed:
                try:
                    await conn.ws.send_json(message.to_dict())
                    sent = True
                except Exception:
                    pass
        return sent

    async def send_raw_to_downstream(self, app_id: str, data: dict) -> bool:
        conns = self._connections.get(app_id, [])
        if not conns:
            self.log(f"send_raw FAILED: {app_id} no connections")
            return False
        sent = False
        for conn in conns[:]:
            if not conn.ws.closed:
                try:
                    await conn.ws.send_json(data)
                    sent = True
                except Exception as e:
                    self.log(f"send_raw ERR: {app_id} session={conn.session_id} {e}")
        if not sent:
            self.log(f"send_raw FAILED: {app_id} all {len(conns)} connections closed")
        return sent

    def is_online(self, app_id: str) -> bool:
        conns = self._connections.get(app_id, [])
        return any(not c.ws.closed for c in conns)

    def get_online_app_ids(self) -> list[str]:
        return [aid for aid, conns in self._connections.items() if any(not c.ws.closed for c in conns)]

    def update_heartbeat(self, interval: int, timeout: int):
        self.heartbeat_interval = interval
        self.heartbeat_timeout = timeout
        self.log(f"Heartbeat updated: interval={interval}s, timeout={timeout}s")

    async def _heartbeat_loop(self):
        while True:
            await asyncio.sleep(30)
            now = time.time()
            timed_out = []
            for app_id, conns in list(self._connections.items()):
                alive = [c for c in conns if not c.ws.closed]
                dead = [c for c in conns if (now - c.last_heartbeat) > self.heartbeat_timeout]
                for c in dead:
                    try:
                        await c.ws.close()
                    except Exception:
                        pass
                if alive and not dead:
                    pass
                elif not alive:
                    timed_out.append(app_id)
                self._connections[app_id] = [c for c in conns if not c.ws.closed and (now - c.last_heartbeat) <= self.heartbeat_timeout]

            for app_id in timed_out:
                self._connections.pop(app_id, None)
                self.sec_logger.heartbeat_timeout(app_id, "")
                await self.db.update_plugin_status(app_id, PluginStatus.OFFLINE)
                self.log(f"Heartbeat timeout: {app_id}")

    async def send_ping(self):
        ping = PingMessage(request_id=f"ping_{uuid.uuid4().hex[:8]}")
        for app_id, conns in list(self._connections.items()):
            for conn in conns:
                if not conn.ws.closed:
                    try:
                        await conn.ws.send_json(ping.to_dict())
                    except Exception:
                        pass
