from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Optional

import yaml
from pydantic import BaseModel, Field


class LogConfig(BaseModel):
    file: str = "bridge.log"
    level: str = "INFO"
    max_size_mb: int = 10
    backup_count: int = 5


class BridgeConfig(BaseModel):
    name: str = "ilink-bridge"
    log_level: str = "INFO"
    debug: bool = False
    storage_dir: str = "~/.ilink-bridge"
    log: LogConfig = Field(default_factory=LogConfig)


class FileCacheConfig(BaseModel):
    enabled: bool = True
    cache_dir: str = "cache/files"
    max_days: int = 14
    cleanup_interval: int = 3600
    max_file_size: int = 52428800
    download_base_url: str = ""


class AutoLaunchEntry(BaseModel):
    command: str = ""
    workdir: str = "."
    restart: str = "on-failure"


class PluginEntry(BaseModel):
    mode: str = "downstream"
    type: str = ""
    name: str = ""
    enabled: bool = True
    app_secret: str = ""
    app_version: int = 1
    alias: str = ""
    auto_launch: Optional[AutoLaunchEntry] = None
    capabilities: list[str] = Field(default_factory=lambda: ["text"])


class RoutingConfig(BaseModel):
    default_downstream: str = "opencode_main"


class HeartbeatConfig(BaseModel):
    interval_seconds: int = 60
    timeout_seconds: int = 300


class WSServerConfig(BaseModel):
    host: str = "0.0.0.0"
    port: int = 8765


class AppConfig(BaseModel):
    bridge: BridgeConfig = Field(default_factory=BridgeConfig)
    plugins: dict[str, PluginEntry] = Field(default_factory=dict)
    routing: RoutingConfig = Field(default_factory=RoutingConfig)
    heartbeat: HeartbeatConfig = Field(default_factory=HeartbeatConfig)
    ws_server: WSServerConfig = Field(default_factory=WSServerConfig)
    file_cache: FileCacheConfig = Field(default_factory=FileCacheConfig)


def expand_path(p: str) -> str:
    return os.path.expandvars(os.path.expanduser(p))


def load_config(path: str = "config.yaml") -> AppConfig:
    config_path = Path(expand_path(path))
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        raw: dict[str, Any] = yaml.safe_load(f) or {}

    config = AppConfig(**raw)
    config.bridge.storage_dir = expand_path(config.bridge.storage_dir)
    return config
