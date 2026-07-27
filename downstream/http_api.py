from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Optional
from urllib.parse import quote

from aiohttp import web

from core.database import Database
from core.file_cache import FileCache


class HttpAPI:
    def __init__(self, db: Database, host: str = "0.0.0.0", port: int = 8766,
                 file_cache: FileCache | None = None):
        self.db = db
        self.host = host
        self.port = port
        self.file_cache = file_cache
        self._app: Optional[web.Application] = None
        self._runner: Optional[web.AppRunner] = None

    async def start(self):
        self._app = web.Application()
        self._app.router.add_get("/api/status", self._handle_status)
        self._app.router.add_get("/api/plugins", self._handle_plugins)
        self._app.router.add_get("/api/sessions", self._handle_sessions)
        self._app.router.add_get("/api/file/{file_id}", self._handle_file_download)
        self._app.router.add_get("/api/file/{file_id}/info", self._handle_file_info)
        self._app.router.add_options("/api/{path:.*}", self._handle_cors)

        self._runner = web.AppRunner(self._app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, self.host, self.port)
        await site.start()
        print(f"[http-api] Listening on {self.host}:{self.port}")

    async def stop(self):
        if self._runner:
            await self._runner.cleanup()

    def _cors_headers(self) -> dict:
        return {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type",
        }

    async def _handle_cors(self, request: web.Request) -> web.Response:
        return web.Response(status=204, headers=self._cors_headers())

    async def _handle_status(self, request: web.Request) -> web.Response:
        plugins = await self.db.get_all_plugins()
        online = sum(1 for p in plugins if p["status"] == 1)
        return web.json_response(
            {"status": "ok", "plugins_total": len(plugins),
             "plugins_online": online},
            headers=self._cors_headers(),
        )

    async def _handle_plugins(self, request: web.Request) -> web.Response:
        plugins = await self.db.get_all_plugins()
        result = []
        for p in plugins:
            result.append({
                "id": p["id"],
                "mode": p["mode"],
                "type": p["type"],
                "name": p["name"],
                "alias": p["alias"],
                "enabled": bool(p["enabled"]),
                "status": "online" if p["status"] == 1 else "offline",
                "connected_at": p["connected_at"],
                "capabilities": p.get("capabilities", ["text"]),
            })
        return web.json_response(
            {"plugins": result},
            headers=self._cors_headers(),
        )

    async def _handle_sessions(self, request: web.Request) -> web.Response:
        return web.json_response(
            {"sessions": []},
            headers=self._cors_headers(),
        )

    async def _handle_file_download(self, request: web.Request) -> web.Response:
        if not self.file_cache:
            return web.json_response({"error": "file cache not enabled"}, status=503)

        file_id = request.match_info["file_id"]
        file_path = self.file_cache.get_file_path(file_id)
        if not file_path or not file_path.exists():
            return web.json_response({"error": "file not found"}, status=404)

        meta = self.file_cache.get_metadata(file_id)
        mime_type = meta.get("mime_type", "application/octet-stream") if meta else "application/octet-stream"
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

        return web.json_response(meta, headers=self._cors_headers())
