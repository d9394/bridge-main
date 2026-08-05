from __future__ import annotations

import asyncio
import json
import time
from typing import Any, Callable, Awaitable, Optional

from aiohttp import web

from core.database import Database
from downstream.downstream_manager import DownstreamManager
from downstream.ws_protocol import (
    ConnectOK, ConnectError, MessageReceived,
    TYPE_CONNECT, TYPE_MESSAGE_RECEIVED,
)
from upstream.upstream_manager import UpstreamManager
from security.logger import SecurityLogger

try:
    from core.file_cache import FileCache
except ImportError:
    FileCache = None


class WSServer:
    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 8765,
        db: Database = None,
        downstream_mgr: DownstreamManager = None,
        upstream_mgr: UpstreamManager = None,
        sec_logger: SecurityLogger = None,
        log_fn: Callable[[str], None] = None,
        file_cache=None,
        platform=None,
    ):
        self.host = host
        self.port = port
        self.db = db
        self.downstream_mgr = downstream_mgr
        self.upstream_mgr = upstream_mgr
        self.sec_logger = sec_logger
        self.file_cache = file_cache
        self.platform = platform
        self.log = log_fn or (lambda msg: print(f"[ws-server] {msg}"))
        self._app: Optional[web.Application] = None
        self._runner: Optional[web.AppRunner] = None
        self._ping_task: Optional[asyncio.Task] = None

    async def start(self):
        self._app = web.Application()
        self._app.router.add_get("/ws/downstream", self._handle_downstream_ws)
        self._app.router.add_get("/ws/upstream", self._handle_upstream_ws)

        if self.file_cache:
            self._app.router.add_get("/api/file/{file_id}", self._handle_file_download)
            self._app.router.add_get("/api/file/{file_id}/info", self._handle_file_info)

        self._app.router.add_get("/api/msg/read/{token}", self._handle_msg_read)
        self._app.router.add_get("/api/msg/read/{token}/", self._handle_msg_read)
        self._app.router.add_get("/api/msg/read/{token}/{n}", self._handle_msg_read)
        self._app.router.add_get("/api/msg/read/{token}/{n}/", self._handle_msg_read)
        self._app.router.add_get("/api/msg/send/{token}/{msg}", self._handle_msg_send)
        self._app.router.add_get("/api/msg/send/{token}/{msg}/", self._handle_msg_send)

        self._runner = web.AppRunner(self._app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, self.host, self.port)
        await site.start()
        self.log(f"WS server listening on {self.host}:{self.port}")

        self._ping_task = asyncio.create_task(self._ping_loop())

    async def stop(self):
        if self._ping_task:
            self._ping_task.cancel()
            try:
                await self._ping_task
            except asyncio.CancelledError:
                pass
        if self._runner:
            await self._runner.cleanup()

    async def _handle_downstream_ws(self, request: web.Request) -> web.WebSocketResponse:
        ip = request.remote or "unknown"
        app_id = ""

        ws = web.WebSocketResponse()
        await ws.prepare(request)

        try:
            try:
                msg = await asyncio.wait_for(ws.receive(), timeout=5.0)
            except asyncio.TimeoutError:
                self.sec_logger.auth_failed(ip, "", "connect_timeout")
                await ws.close(code=4001, message=b"connect timeout")
                return ws

            if msg.type == web.WSMsgType.TEXT:
                data = json.loads(msg.data)
            else:
                self.sec_logger.invalid_connection(ip, reason="non_text_first_message")
                await ws.close(code=4001)
                return ws

            msg_type = data.get("type", "")
            if msg_type != TYPE_CONNECT:
                self.sec_logger.auth_failed(ip, "", "not_connect_message")
                await ws.close(code=4001, message=b"expected connect")
                return ws

            app_id = data.get("app_id", "")
            app_secret = data.get("app_secret", "")
            role = data.get("role", "downstream")

            if not app_id:
                self.sec_logger.auth_failed(ip, "", "missing_app_id")
                await ws.close(code=4001, message=b"missing app_id")
                return ws

            plugin = await self.db.get_plugin(app_id)
            if not plugin:
                self.sec_logger.auth_failed(ip, app_id, "unknown_app_id")
                await ws.send_json(ConnectError(4002, "unknown app_id").to_dict())
                await ws.close(code=4001)
                return ws

            if plugin["app_secret"] and plugin["app_secret"] != app_secret:
                self.sec_logger.auth_failed(ip, app_id, "invalid_secret")
                await ws.send_json(ConnectError(4001, "invalid secret").to_dict())
                await ws.close(code=4001)
                return ws

            if not plugin["enabled"]:
                self.sec_logger.connection_refused(ip, reason="app_disabled")
                await ws.send_json(ConnectError(4003, "app disabled").to_dict())
                await ws.close(code=4003)
                return ws

            if plugin["mode"] != "downstream":
                self.sec_logger.connection_refused(ip, reason="wrong_path")
                await ws.send_json(ConnectError(4004, "use /ws/upstream for upstream plugins").to_dict())
                await ws.close(code=4004)
                return ws

            conn_info = await self.downstream_mgr.connect(app_id, ws, ip)
            await ws.send_json(conn_info.to_dict())

            async for msg in ws:
                if msg.type == web.WSMsgType.TEXT:
                    try:
                        payload = json.loads(msg.data)
                        await self.downstream_mgr.handle_message(app_id, payload)
                    except json.JSONDecodeError:
                        pass
                elif msg.type in (web.WSMsgType.ERROR, web.WSMsgType.CLOSED):
                    break

        except Exception as e:
            self.sec_logger.invalid_connection(ip, reason=f"exception:{e}")
        finally:
            if app_id:
                await self.downstream_mgr.disconnect(app_id, ws)

        return ws

    async def _handle_upstream_ws(self, request: web.Request) -> web.WebSocketResponse:
        ip = request.remote or "unknown"
        app_id = ""

        ws = web.WebSocketResponse()
        await ws.prepare(request)

        app_id = ""
        try:
            try:
                msg = await asyncio.wait_for(ws.receive(), timeout=5.0)
            except asyncio.TimeoutError:
                self.sec_logger.auth_failed(ip, "", "connect_timeout")
                await ws.close(code=4001, message=b"connect timeout")
                return ws

            if msg.type == web.WSMsgType.TEXT:
                data = json.loads(msg.data)
            else:
                self.sec_logger.invalid_connection(ip, reason="non_text_first_message")
                await ws.close(code=4001)
                return ws

            msg_type = data.get("type", "")
            if msg_type != TYPE_CONNECT:
                self.sec_logger.auth_failed(ip, "", "not_connect_message")
                await ws.close(code=4001, message=b"expected connect")
                return ws

            app_id = data.get("app_id", "")
            app_secret = data.get("app_secret", "")

            if not app_id:
                self.sec_logger.auth_failed(ip, "", "missing_app_id")
                await ws.close(code=4001, message=b"missing app_id")
                return ws

            plugin = await self.db.get_plugin(app_id)
            if not plugin:
                self.sec_logger.auth_failed(ip, app_id, "unknown_app_id")
                await ws.send_json(ConnectError(4002, "unknown app_id").to_dict())
                await ws.close(code=4001)
                return ws

            if plugin["app_secret"] and plugin["app_secret"] != app_secret:
                self.sec_logger.auth_failed(ip, app_id, "invalid_secret")
                await ws.send_json(ConnectError(4001, "invalid secret").to_dict())
                await ws.close(code=4001)
                return ws

            if not plugin["enabled"]:
                self.sec_logger.connection_refused(ip, reason="app_disabled")
                await ws.send_json(ConnectError(4003, "app disabled").to_dict())
                await ws.close(code=4003)
                return ws

            if plugin["mode"] != "upstream":
                self.sec_logger.connection_refused(ip, reason="wrong_path")
                await ws.send_json(ConnectError(4004, "use /ws/downstream for downstream plugins").to_dict())
                await ws.close(code=4004)
                return ws

            conn_info = await self.upstream_mgr.connect(app_id, ws, ip)
            await ws.send_json(conn_info.to_dict())

            async for msg in ws:
                if msg.type == web.WSMsgType.TEXT:
                    try:
                        payload = json.loads(msg.data)
                        await self.upstream_mgr.handle_message(app_id, payload)
                    except json.JSONDecodeError:
                        pass
                elif msg.type in (web.WSMsgType.ERROR, web.WSMsgType.CLOSED):
                    break

        except Exception as e:
            self.sec_logger.invalid_connection(ip, reason=f"exception:{e}")
        finally:
            if app_id:
                await self.upstream_mgr.disconnect(app_id, ws)

        return ws

    async def _ping_loop(self):
        while True:
            await asyncio.sleep(self.downstream_mgr.heartbeat_interval)
            await self.downstream_mgr.send_ping()
            if self.upstream_mgr:
                await self.upstream_mgr.send_ping()

    async def _handle_file_download(self, request: web.Request) -> web.Response:
        if not self.file_cache:
            return web.json_response({"error": "file cache not enabled"}, status=503)
        from urllib.parse import quote
        file_id = request.match_info["file_id"]
        file_path = self.file_cache.get_file_path(file_id)
        if not file_path or not file_path.exists():
            return web.json_response({"error": "file not found"}, status=404)
        meta = self.file_cache.get_metadata(file_id)
        file_name = meta.get("original_name", file_path.name) if meta else file_path.name
        safe_name = file_name.encode("ascii", "ignore").decode("ascii").strip()
        if not safe_name:
            safe_name = "download"
        encoded_name = quote(file_name)
        return web.FileResponse(
            path=file_path,
            headers={
                "Content-Disposition": f"attachment; filename=\"{safe_name}\"; filename*=UTF-8''{encoded_name}",
                "Access-Control-Allow-Origin": "*",
            },
        )

    async def _handle_file_info(self, request: web.Request) -> web.Response:
        if not self.file_cache:
            return web.json_response({"error": "file cache not enabled"}, status=503)
        file_id = request.match_info["file_id"]
        meta = self.file_cache.get_metadata(file_id)
        if not meta:
            return web.json_response({"error": "file not found"}, status=404)
        return web.json_response(meta)

    def _get_buffer(self, plugin_id: str) -> list:
        if not self.platform:
            return []
        buf = self.platform._ds_msg_buffer.get(plugin_id)
        return list(buf) if buf else []

    async def _handle_msg_read(self, request: web.Request) -> web.Response:
        token = request.match_info["token"]
        n = request.match_info.get("n", "1")
        try:
            n = int(n)
        except ValueError:
            return web.json_response(
                {"ok": False, "error": "n must be an integer"}, status=400
            )
        if n < 1:
            return web.json_response(
                {"ok": False, "error": "n must be >= 1"}, status=400
            )

        plugin = await self.db.get_plugin_by_secret(token)
        if not plugin:
            return web.json_response(
                {"ok": False, "error": "unknown token"}, status=404
            )

        buffer = self._get_buffer(plugin["id"])
        read_seq = self.platform._ds_read_seq.get(plugin["id"], 0) if self.platform else 0
        unread = [m for m in buffer if m.get("seq", 0) > read_seq]
        total = len(unread)
        if total == 0:
            return web.json_response(
                {"ok": True, "total": 0, "message": None}
            )
        if n > total:
            return web.json_response(
                {"ok": False, "error": "index out of range", "total": total},
                status=404,
            )

        message = unread[n - 1]
        if self.platform:
            self.platform._ds_read_seq[plugin["id"]] = max(
                read_seq, message.get("seq", read_seq)
            )

        return web.json_response(
            {"ok": True, "total": total - n, "n": n, "message": message}
        )

    async def _handle_msg_send(self, request: web.Request) -> web.Response:
        token = request.match_info["token"]
        msg = request.match_info["msg"]
        if not msg:
            return web.json_response(
                {"ok": False, "error": "empty message"}, status=400
            )

        plugin = await self.db.get_plugin_by_secret(token)
        if not plugin:
            return web.json_response(
                {"ok": False, "error": "unknown token"}, status=404
            )
        if self.platform is None:
            return web.json_response(
                {"ok": False, "error": "platform not available"}, status=500
            )

        buffer = self._get_buffer(plugin["id"])
        if buffer:
            last = buffer[-1]
            target_user = last.get("from") or last.get("from_user", "")
        else:
            target_user = self.platform._last_upstream_user if self.platform else None
        if not target_user:
            target_user = await self.db.get_last_inbound_user()
        if not target_user:
            return web.json_response(
                {"ok": False, "error": "no conversation to reply to"}, status=404
            )

        data = {
            "type": "message.send",
            "to": target_user,
            "content": msg,
            "message_type": "text",
            "request_id": f"http_{int(time.time() * 1000)}",
        }
        success = await self.platform._on_downstream_message(plugin["id"], data)
        if success:
            return web.json_response({"ok": True, "to": target_user})
        return web.json_response(
            {"ok": False, "error": "send failed"}, status=502
        )
