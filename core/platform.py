from __future__ import annotations

import asyncio
import base64
import os
import subprocess
import time
from collections import deque
from typing import Optional

from core.database import Database
from core.file_cache import FileCache
from core.router import Router
from core.session import SessionManager
from downstream.downstream_manager import DownstreamManager
from downstream.ws_server import WSServer

from downstream.ws_protocol import MessageReceived
from upstream.upstream_manager import UpstreamManager
from commands.bridge_commands import BridgeCommands
from security.logger import SecurityLogger
from utils.config_parser import AppConfig, expand_path


class BridgePlatform:
    def __init__(self, config: AppConfig, config_path: str = "config.yaml"):
        self.config = config
        self.config_path = config_path
        self._running = False

        log_dir = os.path.join(config.bridge.storage_dir, "logs")
        self.sec_logger = SecurityLogger(
            log_dir,
            level=config.bridge.log.level,
            max_size_mb=config.bridge.log.max_size_mb,
            backup_count=config.bridge.log.backup_count,
        )

        db_path = os.path.join(config.bridge.storage_dir, "bridge.db")
        self.db = Database(db_path)

        self.router = Router(config)

        self.session_mgr = SessionManager(self.db)

        cache_dir = os.path.join(config.bridge.storage_dir, config.file_cache.cache_dir)
        self.file_cache = FileCache(
            cache_dir=cache_dir,
            max_days=config.file_cache.max_days,
            cleanup_interval=config.file_cache.cleanup_interval,
            max_file_size=config.file_cache.max_file_size,
            log_fn=self.sec_logger.info,
        )

        self.upstream_mgr = UpstreamManager(
            db=self.db,
            sec_logger=self.sec_logger,
            heartbeat_interval=config.heartbeat.interval_seconds,
            heartbeat_timeout=config.heartbeat.timeout_seconds,
            log_fn=self.sec_logger.info,
            on_message=self._on_upstream_message,
            on_connect=self._on_upstream_connect,
            on_disconnect=self._on_upstream_disconnect,
        )

        self.downstream_mgr = DownstreamManager(
            db=self.db,
            sec_logger=self.sec_logger,
            heartbeat_interval=config.heartbeat.interval_seconds,
            heartbeat_timeout=config.heartbeat.timeout_seconds,
            log_fn=self.sec_logger.info,
            on_message=self._on_downstream_message,
        )

        self.ws_server = WSServer(
            host=config.ws_server.host,
            port=config.ws_server.port,
            db=self.db,
            downstream_mgr=self.downstream_mgr,
            upstream_mgr=self.upstream_mgr,
            sec_logger=self.sec_logger,
            log_fn=self.sec_logger.info,
            file_cache=self.file_cache,
            platform=self,
        )

        self.cmd_handler = BridgeCommands(
            config=config,
            router=self.router,
            session_mgr=self.session_mgr,
            downstream_mgr=self.downstream_mgr,
            db=self.db,
        )

        self._ds_msg_buffer: dict[str, deque] = {}
        self._ds_buffer_max = 200
        self._ds_msg_seq: dict[str, int] = {}
        self._ds_read_seq: dict[str, int] = {}

        self._user_upstream_map: dict[str, str] = {}
        self._last_upstream_user: Optional[str] = None
        self._plugin_processes: dict[str, subprocess.Popen] = {}
        self._last_context_tokens: dict[str, str] = {}
        self._active_upstream: Optional[str] = None
        self._dedup_cache: dict[str, float] = {}
        self._dedup_window = 8.0

    async def start(self):
        self._running = True
        self.sec_logger.info("Starting ilink-bridge...")

        await self.db.init()
        self.sec_logger.info("Database initialized")

        for plugin_id, plugin in self.config.plugins.items():
            await self.db.upsert_plugin(
                plugin_id, plugin.mode, plugin.type, plugin.name,
                plugin.alias, plugin.enabled, plugin.app_secret,
                plugin.app_version, plugin.capabilities,
            )
        self.sec_logger.info(
            f"Registered {len(self.config.plugins)} plugins "
            f"({sum(1 for p in self.config.plugins.values() if p.mode == 'upstream')} upstream, "
            f"{sum(1 for p in self.config.plugins.values() if p.mode == 'downstream')} downstream)"
        )

        if self.config.file_cache.enabled:
            await self.file_cache.start()

        await self.upstream_mgr.start()
        await self.downstream_mgr.start()

        await self.ws_server.start()

        await self._launch_managed_plugins()

        self.sec_logger.info("ilink-bridge started successfully")

    async def stop(self):
        self._running = False
        self.sec_logger.info("Stopping ilink-bridge...")

        loop = asyncio.get_event_loop()
        for name, proc in self._plugin_processes.items():
            if proc.returncode is None:
                proc.terminate()
                try:
                    await asyncio.wait_for(
                        loop.run_in_executor(None, proc.wait), timeout=5
                    )
                except asyncio.TimeoutError:
                    proc.kill()

        await self.ws_server.stop()
        await self.downstream_mgr.stop()
        await self.upstream_mgr.stop()
        if self.config.file_cache.enabled:
            await self.file_cache.stop()
        await self.db.close()
        self.sec_logger.info("ilink-bridge stopped")

    async def _launch_managed_plugins(self):
        for plugin_id, plugin in self.config.plugins.items():
            if not plugin.auto_launch or not plugin.auto_launch.command:
                continue
            if not plugin.enabled:
                continue

            cmd = plugin.auto_launch.command
            workdir = plugin.auto_launch.workdir or "."
            restart_policy = plugin.auto_launch.restart

            self.sec_logger.info(
                f"Launching managed plugin: {plugin_id} cmd={cmd}"
            )

            asyncio.create_task(
                self._run_managed_plugin(plugin_id, cmd, workdir, restart_policy)
            )

    async def _run_managed_plugin(
        self, plugin_id: str, command: str, workdir: str, restart: str
    ):
        max_restarts = 5 if restart == "on-failure" else 999
        restart_count = 0

        while self._running and restart_count < max_restarts:
            loop = asyncio.get_event_loop()
            try:
                proc = await loop.run_in_executor(
                    None,
                    lambda: subprocess.Popen(command.split(), cwd=workdir),
                )
                self._plugin_processes[plugin_id] = proc
                self.sec_logger.info(f"Plugin process started: {plugin_id} (pid={proc.pid})")

                await loop.run_in_executor(None, proc.wait)

                if proc.returncode != 0:
                    self.sec_logger.warn(
                        f"Plugin {plugin_id} exited with code {proc.returncode}"
                    )
                    if restart == "never":
                        break
                    restart_count += 1
                else:
                    restart_count = 0

            except Exception as e:
                self.sec_logger.error(f"Plugin {plugin_id} launch error: {e}")
                break

            if self._running and restart != "never":
                wait = min(restart_count * 2, 30)
                self.sec_logger.info(
                    f"Restarting {plugin_id} in {wait}s (attempt {restart_count})..."
                )
                await asyncio.sleep(wait)

    async def reload_config(self):
        from utils.config_parser import load_config

        try:
            new_config = load_config(self.config_path)
        except Exception as e:
            self.sec_logger.error(f"Config reload failed: {e}")
            return False

        self.sec_logger.info("Reloading config.yaml...")

        for plugin_id, plugin in new_config.plugins.items():
            await self.db.upsert_plugin(
                plugin_id, plugin.mode, plugin.type, plugin.name,
                plugin.alias, plugin.enabled, plugin.app_secret,
                plugin.app_version, plugin.capabilities,
            )

        old_ids = set(self.config.plugins.keys())
        new_ids = set(new_config.plugins.keys())
        for removed_id in old_ids - new_ids:
            if self.downstream_mgr.is_online(removed_id):
                await self.downstream_mgr.disconnect(removed_id)
                self.sec_logger.info(f"Disconnected removed plugin: {removed_id}")
            if self.upstream_mgr.is_online(removed_id):
                await self.upstream_mgr.disconnect(removed_id)
                self.sec_logger.info(f"Disconnected removed upstream: {removed_id}")

        for plugin_id in old_ids & new_ids:
            plugin = new_config.plugins.get(plugin_id)
            if plugin and not plugin.enabled:
                if self.downstream_mgr.is_online(plugin_id):
                    await self.downstream_mgr.disconnect(plugin_id)
                if self.upstream_mgr.is_online(plugin_id):
                    await self.upstream_mgr.disconnect(plugin_id)

        if self._active_upstream and self._active_upstream not in new_ids:
            self._active_upstream = None
            remaining = [p for p in new_config.plugins.values() if p.mode == "upstream"]
            if remaining:
                first = list(new_config.plugins.keys())[
                    list(new_config.plugins.values()).index(remaining[0])
                ]
                self._active_upstream = first

        self.router.update_config(new_config)
        self.downstream_mgr.update_heartbeat(
            new_config.heartbeat.interval_seconds,
            new_config.heartbeat.timeout_seconds,
        )
        self.upstream_mgr.update_heartbeat(
            new_config.heartbeat.interval_seconds,
            new_config.heartbeat.timeout_seconds,
        )
        self.cmd_handler.config = new_config
        self.config = new_config
        self.sec_logger.info(
            f"Config reloaded: {len(new_config.plugins)} plugins"
        )
        return True

    async def _on_upstream_connect(self, upstream_id: str):
        if self._active_upstream is None:
            self._active_upstream = upstream_id
            self.sec_logger.info(f"Active upstream auto-set to first connected: {upstream_id}")

    async def _on_upstream_disconnect(self, upstream_id: str):
        if self._active_upstream == upstream_id:
            remaining = self.upstream_mgr.get_online_upstream_ids()
            self._active_upstream = remaining[0] if remaining else None
            self.sec_logger.info(
                f"Active upstream {upstream_id} disconnected, switched to: {self._active_upstream}"
            )

    async def _on_upstream_message(
        self, upstream_id: str, from_user: str, content: str, raw_data: dict
    ):
        if not from_user:
            return

        self._active_upstream = upstream_id
        context_token = raw_data.get("context_token", "")
        if context_token:
            self._last_context_tokens[from_user] = context_token
        self._user_upstream_map[from_user] = upstream_id
        self._last_upstream_user = from_user

        self.sec_logger.info(
            f"Upstream({upstream_id}) inbound from {from_user}: {content[:100]}"
        )

        stripped = content.strip()

        if stripped.lower() == "/online -list":
            all_plugins = await self.db.get_all_plugins()
            online_upstreams = set(self.upstream_mgr.get_online_upstream_ids())
            online_downstreams = set(self.downstream_mgr.get_online_app_ids())
            lines = ["Registered plugins:"]
            for p in all_plugins:
                mode_label = "upstream" if p["mode"] == "upstream" else "downstream"
                is_online = (p["mode"] == "upstream" and p["id"] in online_upstreams) or \
                            (p["mode"] == "downstream" and p["id"] in online_downstreams)
                status_str = "online" if is_online else "offline"
                active_mark = " <ACTIVE" if p["id"] == self._active_upstream else ""
                lines.append(f"  {p['id']} ({mode_label}) [{status_str}]{active_mark}")
            reply = "\n".join(lines)
            await self.upstream_mgr.send_to_upstream(upstream_id, {
                "type": "message.send",
                "to": from_user,
                "content": reply,
                "context_token": context_token,
            })
            return

        if stripped.startswith("/") and stripped.split()[0].lower() in (
            "/help", "/h", "/status", "/list", "/echo", "/default",
            "/switch", "/silent", "/next", "/version", "/caps"
        ):
            reply = await self.cmd_handler.handle(from_user, stripped)
            if reply:
                payload = {
                    "type": "message.send",
                    "to": from_user,
                    "content": reply,
                }
                if context_token:
                    payload["context_token"] = context_token
                await self.upstream_mgr.send_to_upstream(upstream_id, payload)
            return

        downstream_id = self.router.resolve(content)
        if not downstream_id:
            downstream_id = await self.session_mgr.get_binding(from_user)
        if not downstream_id:
            downstream_id = self.config.routing.default_downstream

        clean_content = self.router.strip_mention(content, downstream_id)

        ds_msg = MessageReceived(
            request_id=f"in_{raw_data.get('request_id', '')}",
            from_user=from_user,
            sender_name=raw_data.get("sender_name", ""),
            content=clean_content,
            message_type=raw_data.get("message_type", "text"),
            media_url=raw_data.get("media_url", ""),
        )

        msg_dict = ds_msg.to_dict()
        files = raw_data.get("files") or raw_data.get("file_info")
        if files:
            msg_dict["files"] = files

        buf = self._ds_msg_buffer.setdefault(
            downstream_id, deque(maxlen=self._ds_buffer_max)
        )
        seq = self._ds_msg_seq.get(downstream_id, 0) + 1
        self._ds_msg_seq[downstream_id] = seq
        msg_dict["seq"] = seq
        buf.append(dict(msg_dict))

        success = await self.downstream_mgr.send_raw_to_downstream(downstream_id, msg_dict)
        if not success:
            await self.upstream_mgr.send_to_upstream(upstream_id, {
                "type": "message.send",
                "to": from_user,
                "content": f"\u26a0\ufe0f \u4e0b\u6e38 {downstream_id} \u5f53\u524d\u79bb\u7ebf\uff0c\u6d88\u606f\u672a\u9001\u8fbe",
                "context_token": context_token,
            })

        await self.db.log_message(from_user, downstream_id, "inbound", raw_data.get("message_type", "text"), content)

    async def _on_downstream_message(self, app_id: str, data: dict):
        msg_type = data.get("type", "")
        target_user = data.get("to", "")

        if not target_user:
            return False

        now = time.time()
        request_id = data.get("request_id", "")
        if request_id and request_id.startswith(("q_", "p_")):
            if request_id in self._dedup_cache and (now - self._dedup_cache[request_id]) < self._dedup_window:
                self.sec_logger.info(
                    f"Dedup: dropped duplicate request_id={request_id} to {target_user} from {app_id}"
                )
                return False
            self._dedup_cache[request_id] = now

        content = data.get("content", "")
        if content and msg_type in ("message", "message.send", ""):
            import hashlib
            dedup_key = f"{target_user}:{hashlib.md5(content.encode()).hexdigest()}"
            if dedup_key in self._dedup_cache and (now - self._dedup_cache[dedup_key]) < self._dedup_window:
                self.sec_logger.info(
                    f"Dedup: dropped duplicate content to {target_user} from {app_id}: {content[:40]}"
                )
                return False
            self._dedup_cache[dedup_key] = now

        expired = [k for k, t in self._dedup_cache.items() if now - t > self._dedup_window * 2]
        for k in expired:
            del self._dedup_cache[k]

        upstream_id = self._user_upstream_map.get(target_user)
        if not upstream_id:
            upstream_id = self._active_upstream
        if not upstream_id or not self.upstream_mgr.is_online(upstream_id):
            online_upstreams = self.upstream_mgr.get_online_upstream_ids()
            if online_upstreams:
                upstream_id = online_upstreams[0]
            else:
                self.sec_logger.warn(
                    f"No upstream available for downstream reply to {target_user}"
                )
                return False

        if msg_type == "file.send" and not self._plugin_has_capability(upstream_id, "files"):
            await self._handle_file_degradation(upstream_id, target_user, data)
            return True

        modified_data = dict(data)
        content = data.get("content", "")
        if content and msg_type in ("message", "message.send", ""):
            plugin = self.config.plugins.get(app_id)
            if plugin:
                prefix = f"({plugin.alias})" if plugin.alias else f"({app_id})"
            else:
                prefix = f"({app_id})"
            modified_data["content"] = f"{prefix} {content}"

        success = await self.upstream_mgr.send_to_upstream(upstream_id, modified_data)
        if success:
            await self.db.log_message(
                target_user, app_id, "outbound",
                data.get("message_type", msg_type),
                data.get("content", ""),
            )
        return success

    def _plugin_has_capability(self, plugin_id: str, cap: str) -> bool:
        plugin = self.config.plugins.get(plugin_id)
        if not plugin:
            return False
        return cap in (plugin.capabilities or ["text"])

    def _format_size(self, size: int) -> str:
        if size < 1024:
            return f"{size} B"
        elif size < 1024 * 1024:
            return f"{size / 1024:.1f} KB"
        else:
            return f"{size / (1024 * 1024):.1f} MB"

    async def _handle_file_degradation(self, upstream_id: str, target_user: str, data: dict):
        file_data_b64 = data.get("file_data", "")
        file_name = data.get("file_name", "unknown")
        file_size = len(base64.b64decode(file_data_b64)) if file_data_b64 else 0

        plugin = self.config.plugins.get(upstream_id)
        plugin_name = plugin.name if plugin else upstream_id

        if self.config.file_cache.enabled and file_data_b64:
            try:
                file_bytes = base64.b64decode(file_data_b64)
                mime_type = data.get("mime_type", "application/octet-stream")
                file_id = await self.file_cache.store_file(
                    file_bytes, file_name, mime_type,
                    source_plugin=data.get("app_id", ""),
                    target_plugin=upstream_id,
                )
                base = self.config.file_cache.download_base_url
                if base:
                    download_url = f"{base}/api/file/{file_id}"
                else:
                    host = self.config.ws_server.host
                    if host == "0.0.0.0":
                        host = "localhost"
                    port = self.config.ws_server.port
                    download_url = f"http://{host}:{port}/api/file/{file_id}"

                content = (
                    f"📎 文件: {file_name} ({self._format_size(file_size)})\n"
                    f"⬇️ 下载链接: {download_url}\n"
                    f"⏰ 链接有效期: {self.file_cache.max_days} 天\n"
                    f"💡 {plugin_name} 不支持直接接收文件，请点击链接下载。"
                )
            except Exception as e:
                self.sec_logger.error(f"File cache error: {e}")
                content = (
                    f"📎 文件: {file_name} ({self._format_size(file_size)})\n"
                    f"⚠️ {plugin_name} 不支持文件接收，且缓存存储失败: {e}"
                )
        else:
            content = (
                f"📎 文件: {file_name} ({self._format_size(file_size)})\n"
                f"⚠️ {plugin_name} 不支持文件接收，文件内容无法传递。"
            )

        await self.upstream_mgr.send_to_upstream(upstream_id, {
            "type": "message.send",
            "to": target_user,
            "content": content,
        })


