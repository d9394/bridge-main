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
    ConnectOK, ConnectError, PingMessage,
    TYPE_MESSAGE_SEND, TYPE_PONG, TYPE_MESSAGE_RECEIVED, TYPE_MESSAGE_TYPING,
    MessageReceived as DownstreamMessageReceived,
)


class UpstreamManager:
    def __init__(
        self,
        db: Database,
        sec_logger: SecurityLogger = None,
        heartbeat_interval: int = 60,
        heartbeat_timeout: int = 300,
        log_fn: Callable[[str], None] = None,
        on_message: Callable[[str, str, str, dict], Awaitable[None]] = None,
        on_connect: Callable[[str], Awaitable[None]] = None,
        on_disconnect: Callable[[str], Awaitable[None]] = None,
    ):
        self.db = db
        self.sec_logger = sec_logger
        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_timeout = heartbeat_timeout
        self.log = log_fn or (lambda msg: print(f"[upstream] {msg}"))
        self.on_message = on_message
        self.on_connect = on_connect
        self.on_disconnect = on_disconnect

        self._connections: dict[str, PluginConnection] = {}
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
        self, plugin_id: str, ws: web.WebSocketResponse, ip: str = ""
    ) -> ConnectOK | ConnectError:
        plugin = await self.db.get_plugin(plugin_id)
        if not plugin:
            return ConnectError(code=4002, message="unknown app_id")
        if not plugin["enabled"]:
            return ConnectError(code=4003, message="app disabled")

        session_id = f"us_{uuid.uuid4().hex[:12]}"
        conn = PluginConnection(plugin_id, "upstream", ws, session_id, ip)
        conn.last_heartbeat = time.time()
        conn.connected_at = time.time()
        self._connections[plugin_id] = conn

        await self.db.update_plugin_status(
            plugin_id, PluginStatus.ONLINE,
            ws_session_id=session_id,
            last_heartbeat_at=time.time(),
            connected_at=time.time(),
        )
        if self.sec_logger:
            self.sec_logger.info(f"upstream_online plugin_id={plugin_id} ip={ip} session={session_id}")
        self.log(f"Upstream connected: {plugin_id} ip={ip} session={session_id}")

        if self.on_connect:
            asyncio.create_task(self.on_connect(plugin_id))

        return ConnectOK(
            session_id=session_id,
            heartbeat_interval=self.heartbeat_interval,
        )

    async def disconnect(self, plugin_id: str, ws: Any = None):
        if ws is None:
            conn = self._connections.pop(plugin_id, None)
        else:
            conn = self._connections.get(plugin_id)
            if conn and conn.ws is ws:
                del self._connections[plugin_id]
            elif conn and conn.ws is not ws:
                conn = None
        if conn:
            await self.db.update_plugin_status(
                plugin_id, PluginStatus.OFFLINE
            )
            if self.sec_logger:
                self.sec_logger.info(f"ws_closed ip={conn.ip} app_id={plugin_id}")
            self.log(f"Upstream disconnected: {plugin_id}")
            if self.on_disconnect:
                await self.on_disconnect(plugin_id)

    async def handle_message(self, plugin_id: str, data: dict):
        msg_type = data.get("type", "")

        if msg_type == TYPE_PONG:
            conn = self._connections.get(plugin_id)
            if conn:
                conn.last_heartbeat = time.time()
                await self.db.update_plugin_status(
                    plugin_id, PluginStatus.ONLINE,
                    last_heartbeat_at=time.time(),
                )

        elif msg_type == TYPE_MESSAGE_RECEIVED:
            from_user = data.get("from", "")
            content = data.get("content", "")
            if self.on_message:
                await self.on_message(plugin_id, from_user, content, data)

    async def send_to_upstream(self, plugin_id: str, data: dict) -> bool:
        conn = self._connections.get(plugin_id)
        if not conn or conn.ws.closed:
            return False
        try:
            await conn.ws.send_json(data)
            return True
        except Exception:
            return False

    def is_online(self, plugin_id: str) -> bool:
        conn = self._connections.get(plugin_id)
        return conn is not None and not conn.ws.closed

    def get_online_upstream_ids(self) -> list[str]:
        return [pid for pid, c in self._connections.items() if not c.ws.closed]

    def get_connection(self, plugin_id: str) -> Optional[PluginConnection]:
        return self._connections.get(plugin_id)

    def update_heartbeat(self, interval: int, timeout: int):
        self.heartbeat_interval = interval
        self.heartbeat_timeout = timeout
        self.log(f"Upstream heartbeat updated: interval={interval}s, timeout={timeout}s")

    async def _heartbeat_loop(self):
        while True:
            await asyncio.sleep(30)
            now = time.time()
            timed_out = []
            for plugin_id, conn in list(self._connections.items()):
                elapsed = now - conn.last_heartbeat
                if elapsed > self.heartbeat_timeout:
                    timed_out.append(plugin_id)

            for plugin_id in timed_out:
                conn = self._connections.pop(plugin_id, None)
                if conn:
                    await self.db.update_plugin_status(
                        plugin_id, PluginStatus.OFFLINE
                    )
                    if self.sec_logger:
                        self.sec_logger.info(f"heartbeat_timeout ds_id={plugin_id} ip={conn.ip}")
                    self.log(f"Upstream heartbeat timeout: {plugin_id}")

    async def send_ping(self):
        ping = PingMessage(request_id=f"ping_{uuid.uuid4().hex[:8]}")
        for plugin_id, conn in list(self._connections.items()):
            if not conn.ws.closed:
                try:
                    await conn.ws.send_json(ping.to_dict())
                except Exception:
                    pass
