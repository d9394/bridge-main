"""配置文件监控，支持热重载"""

from __future__ import annotations

import asyncio
import os
import time
from typing import Callable, Awaitable, Optional

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileModifiedEvent


class ConfigFileHandler(FileSystemEventHandler):
    """监控 config.yaml 文件变化"""

    def __init__(self, config_path: str, callback: Callable[[], None]):
        self.config_path = os.path.abspath(config_path)
        self.callback = callback
        self._last_modified = 0
        self._debounce_seconds = 1.0

    def on_modified(self, event: FileModifiedEvent):
        if event.is_directory:
            return

        src_path = os.path.abspath(event.src_path)
        if src_path != self.config_path:
            return

        now = time.time()
        if now - self._last_modified < self._debounce_seconds:
            return
        self._last_modified = now

        self.callback()


class ConfigWatcher:
    """配置文件监控器，检测变化后触发回调"""

    def __init__(
        self,
        config_path: str,
        on_change: Callable[[], None],
        log_fn: Callable[[str], None] = None,
    ):
        self.config_path = os.path.abspath(config_path)
        self.on_change = on_change
        self.log = log_fn or (lambda msg: print(f"[config-watcher] {msg}"))
        self._observer: Optional[Observer] = None

    def start(self):
        watch_dir = os.path.dirname(self.config_path) or "."
        handler = ConfigFileHandler(self.config_path, self._handle_change)
        self._observer = Observer()
        self._observer.schedule(handler, watch_dir, recursive=False)
        self._observer.start()
        self.log(f"Watching config: {self.config_path}")

    def stop(self):
        if self._observer:
            self._observer.stop()
            self._observer.join(timeout=3)
            self._observer = None
            self.log("Config watcher stopped")

    def _handle_change(self):
        self.log("Config file changed, reloading...")
        try:
            self.on_change()
        except Exception as e:
            self.log(f"Config reload error: {e}")
