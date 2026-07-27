from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from commands.base import parse_command

if TYPE_CHECKING:
    from core.router import Router
    from core.session import SessionManager
    from core.database import Database
    from downstream.downstream_manager import DownstreamManager
    from utils.config_parser import AppConfig


HELP_TEXT = """\U0001f4da ilink-bridge \u547d\u4ee4\u5217\u8868

/help              \u663e\u793a\u6b64\u5e2e\u52a9
/status            \u663e\u793a\u5e73\u53f0\u72b6\u6001
/list              \u5217\u51fa\u6240\u6709\u63d2\u4ef6\u53ca\u5728\u7ebf\u72b6\u6001
/echo <text>       \u56de\u663e\u6d88\u606f\uff08\u6d4b\u8bd5\u8fde\u63a5\u7528\uff09
/default           \u663e\u793a\u5f53\u524d\u9ed8\u8ba4\u4e0b\u6e38
/default -list     \u5217\u51fa\u6240\u6709\u4e0b\u6e38\u53ca\u522b\u540d
/switch <alias>    \u5207\u6362\u5230\u6307\u5b9a\u4e0b\u6e38\uff08\u5982 /switch oc1\uff09
/silent on|off     \u9759\u9ed8\u6a21\u5f0f\u5f00\u5173
/next              \u91cd\u7f6e\u6d88\u606f\u8ba1\u6570
/version           \u663e\u793a\u7248\u672c\u4fe1\u606f
/caps              \u663e\u793a\u6240\u6709\u63d2\u4ef6\u7684\u80fd\u529b\u914d\u7f6e
/caps <alias>      \u663e\u793a\u6307\u5b9a\u63d2\u4ef6\u7684\u80fd\u529b"""


class BridgeCommands:
    def __init__(
        self,
        config: "AppConfig",
        router: "Router",
        session_mgr: "SessionManager",
        downstream_mgr: "DownstreamManager",
        db: "Database",
    ):
        self.config = config
        self.router = router
        self.session_mgr = session_mgr
        self.downstream_mgr = downstream_mgr
        self.db = db

    async def handle(self, user_id: str, text: str) -> str | None:
        parsed = parse_command(text)
        if not parsed:
            return None

        cmd = parsed.command
        args = parsed.args

        if cmd in ("help", "h"):
            return await self._cmd_help()
        elif cmd == "status":
            return await self._cmd_status()
        elif cmd == "list":
            return await self._cmd_list()
        elif cmd == "switch":
            return await self._cmd_switch(user_id, args)
        elif cmd == "silent":
            return await self._cmd_silent(args)
        elif cmd == "echo":
            return await self._cmd_echo(args)
        elif cmd == "default":
            return await self._cmd_default(args)
        elif cmd == "next":
            return "\u2705 \u6d88\u606f\u8ba1\u6570\u5df2\u91cd\u7f6e"
        elif cmd == "version":
            return "ilink-bridge v2.0.0"
        elif cmd == "caps":
            return await self._cmd_caps(args)
        else:
            return None

    async def _cmd_echo(self, args: list[str]) -> str:
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        text = " ".join(args)
        if text:
            return f"\u23f0 {now}\uff0c{text}"
        return f"\u23f0 {now}"

    async def _cmd_default(self, args: list[str]) -> str:
        if args and args[0] == "-list":
            from downstream.downstream_manager import DownstreamManager
            lines = ["Available downstreams:"]
            for alias, pid, name in self.router.list_aliases():
                is_online = self.downstream_mgr.is_online(pid)
                status_icon = "\U0001f4aa" if is_online else "\U0001f4a4"
                lines.append(f"  @{alias} -> {pid} ({name}) {status_icon}")
            if not self.config.plugins:
                lines.append("  (no downstreams configured)")
            return "\n".join(lines)
        default = self.config.routing.default_downstream
        plugin = self.config.plugins.get(default)
        name = plugin.name if plugin else default
        alias = f" @{plugin.alias}" if plugin and plugin.alias else ""
        is_online = self.downstream_mgr.is_online(default)
        status_icon = "\U0001f4aa" if is_online else "\U0001f4a4"
        return f"Current default: {default} ({name}){alias} {status_icon}"

    async def _cmd_help(self) -> str:
        return HELP_TEXT

    async def _cmd_status(self) -> str:
        all_plugins = await self.db.get_all_plugins()
        online = sum(1 for p in all_plugins if p["status"] == 1)
        total = len(all_plugins)
        return (
            f"\U0001F4CA ilink-bridge \u72b6\u6001\n"
            f"\u63d2\u4ef6: {online}/{total} \u5728\u7ebf\n"
            f"\u5fc3\u8df3\u95f4\u9694: {self.config.heartbeat.interval_seconds}s\n"
            f"\u8d85\u65f6\u9608\u503c: {self.config.heartbeat.timeout_seconds}s"
        )

    async def _cmd_list(self) -> str:
        all_plugins = await self.db.get_all_plugins()
        if not all_plugins:
            return "\U0001F4CB \u6682\u65e0\u63d2\u4ef6\u914d\u7f6e"

        lines = ["\U0001F4CA \u63d2\u4ef6\u72b6\u6001\uff1a"]
        for p in all_plugins:
            status_icon = "\u2705" if p["status"] == 1 else "\u274c"
            mode_tag = f"[{p['mode']}]" if p["mode"] else ""
            alias = f" ({p['alias']})" if p["alias"] else ""
            enabled = "" if p["enabled"] else " [\u5df2\u7981\u7528]"
            lines.append(f"{status_icon} {mode_tag} {p['id']}{alias} - {p['name']}{enabled}")
        return "\n".join(lines)

    async def _cmd_switch(self, user_id: str, args: list[str]) -> str:
        if not args:
            return "\u7528\u6cd5: /switch <alias>\n\u53ef\u7528\u522b\u540d: " + ", ".join(
                f"{a}({n})" for a, _, n in self.router.list_aliases()
            )

        alias = args[0].lower()
        plugin = await self.db.get_plugin_by_alias(alias)
        if not plugin:
            return f"\u274c \u672a\u627e\u5230\u522b\u540d '{alias}' \u5bf9\u5e94\u7684\u63d2\u4ef6"

        await self.session_mgr.set_binding(user_id, plugin["id"])
        return f"\u2705 \u5df2\u5207\u6362\u5230 {plugin['name']} ({plugin['id']})"

    async def _cmd_silent(self, args: list[str]) -> str:
        if not args:
            return "\u7528\u6cd5: /silent on|off"
        mode = args[0].lower()
        if mode not in ("on", "off"):
            return "\u7528\u6cd5: /silent on|off"
        return "\u2705 \u9759\u9ed8\u6a21\u5f0f\u5df2" + ("\u5f00\u542f" if mode == "on" else "\u5173\u95ed")

    async def _cmd_version(self) -> str:
        return "ilink-bridge v2.0.0"

    async def _cmd_caps(self, args: list[str]) -> str:
        if args:
            alias = args[0].lower()
            plugin = await self.db.get_plugin_by_alias(alias)
            if not plugin:
                plugin_data = await self.db.get_plugin(alias)
                if not plugin_data:
                    return f"\u274c \u672a\u627e\u5230\u63d2\u4ef6 '{alias}'"
                plugin = plugin_data
            caps = plugin.get("capabilities", ["text"])
            status = "\u2705" if plugin["status"] == 1 else "\u274c"
            return (
                f"\U0001f4ca {plugin['name']} ({plugin['id']}) {status}\n"
                f"\u80fd\u529b: {', '.join(caps)}"
            )

        all_plugins = await self.db.get_all_plugins()
        if not all_plugins:
            return "\U0001F4CB \u6682\u65e0\u63d2\u4ef6\u914d\u7f6e"

        lines = ["\U0001f4ca \u63d2\u4ef6\u80fd\u529b\u914d\u7f6e\uff1a"]
        for p in all_plugins:
            caps = p.get("capabilities", ["text"])
            status = "\u2705" if p["status"] == 1 else "\u274c"
            alias = f" ({p['alias']})" if p["alias"] else ""
            lines.append(f"{status} {p['id']}{alias}: {', '.join(caps)}")
        return "\n".join(lines)
