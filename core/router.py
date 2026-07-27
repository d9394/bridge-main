from __future__ import annotations

import re
from typing import Optional

from utils.config_parser import AppConfig


class Router:
    def __init__(self, config: AppConfig):
        self.config = config
        self._alias_map: dict[str, str] = {}
        self._rebuild_map()

    def _rebuild_map(self):
        self._alias_map.clear()
        for plugin_id, plugin in self.config.plugins.items():
            if plugin.mode == "downstream" and plugin.alias and plugin.enabled:
                self._alias_map[plugin.alias.lower()] = plugin_id

    def update_config(self, config: AppConfig):
        self.config = config
        self._rebuild_map()

    def resolve(self, text: str) -> Optional[str]:
        text_lower = text.lower().strip()
        for alias, plugin_id in self._alias_map.items():
            pattern = f"@{alias}"
            if text_lower.startswith(pattern + " ") or text_lower == pattern:
                return plugin_id
        return None

    def strip_mention(self, text: str, plugin_id: str) -> str:
        plugin = self.config.plugins.get(plugin_id)
        if plugin and plugin.alias:
            pattern = f"@{plugin.alias.lower()}"
            text_lower = text.lower()
            if text_lower.startswith(pattern + " "):
                return text[len(plugin.alias) + 2:].strip()
            elif text_lower == pattern:
                return ""
        return text

    def format_reply_prefix(self, plugin_id: str) -> str:
        plugin = self.config.plugins.get(plugin_id)
        if plugin and plugin.alias:
            return f"@{plugin.alias} "
        return ""

    def list_aliases(self) -> list[tuple[str, str, str]]:
        result = []
        for plugin_id, plugin in self.config.plugins.items():
            if plugin.mode == "downstream" and plugin.alias and plugin.enabled:
                result.append((plugin.alias, plugin_id, plugin.name))
        return result
