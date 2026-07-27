from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional


class MessageType(str, Enum):
    TEXT = "text"
    IMAGE = "image"
    FILE = "file"
    VOICE = "voice"
    VIDEO = "video"


class PluginStatus(int, Enum):
    OFFLINE = 0
    ONLINE = 1


class PluginMode(str, Enum):
    UPSTREAM = "upstream"
    DOWNSTREAM = "downstream"


@dataclass
class ILinkMessage:
    message_id: str = ""
    from_user_id: str = ""
    context_token: str = ""
    message_type: int = 0
    text_content: str = ""
    group_id: str = ""
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass
class PluginInfo:
    id: str
    mode: str
    type: str
    name: str
    alias: str
    enabled: bool
    app_secret: str
    status: PluginStatus = PluginStatus.OFFLINE
    last_heartbeat_at: Optional[float] = None
    connected_at: Optional[float] = None
    ws_session_id: str = ""


@dataclass
class PluginConnection:
    plugin_id: str
    mode: str
    ws: Any
    session_id: str
    ip: str = ""
    last_heartbeat: float = 0
    connected_at: float = 0


@dataclass
class ConnectRequest:
    app_id: str = ""
    app_secret: str = ""
    role: str = ""


@dataclass
class ConnectResponse:
    session_id: str = ""
    heartbeat_interval: int = 60
    ok: bool = True
    error: str = ""


@dataclass
class WSMessage:
    type: str = ""
    request_id: str = ""
    data: dict[str, Any] = field(default_factory=dict)
