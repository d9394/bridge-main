"""WebSocket 协议消息类型定义"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


# ── 消息类型常量 ──

# 下游 → Bridge
TYPE_CONNECT = "connect"
TYPE_PONG = "pong"
TYPE_MESSAGE_SEND = "message.send"
TYPE_MESSAGE_TYPING = "message.typing"

# Bridge → 下游
TYPE_CONNECT_OK = "connect.ok"
TYPE_CONNECT_ERROR = "connect.error"
TYPE_MESSAGE_RECEIVED = "message.received"
TYPE_PING = "ping"
TYPE_ERROR = "error"


@dataclass
class ConnectRequest:
    """下游连接注册请求"""
    app_id: str = ""
    app_secret: str = ""


@dataclass
class ConnectOK:
    """连接成功响应"""
    session_id: str = ""
    heartbeat_interval: int = 60  # 下发给下游的心跳间隔

    def to_dict(self) -> dict:
        return {
            "type": TYPE_CONNECT_OK,
            "session_id": self.session_id,
            "heartbeat_interval": self.heartbeat_interval,
        }


@dataclass
class ConnectError:
    """连接失败响应"""
    code: int = 4001
    message: str = "auth failed"

    def to_dict(self) -> dict:
        return {
            "type": TYPE_CONNECT_ERROR,
            "code": self.code,
            "message": self.message,
        }


@dataclass
class MessageReceived:
    """推送给下游的上游消息"""
    request_id: str = ""
    from_user: str = ""
    sender_name: str = ""
    content: str = ""
    message_type: str = "text"  # text | image | file | voice | video
    media_url: str = ""

    def to_dict(self) -> dict:
        d = {
            "type": TYPE_MESSAGE_RECEIVED,
            "request_id": self.request_id,
            "from": self.from_user,
            "sender_name": self.sender_name,
            "content": self.content,
            "message_type": self.message_type,
        }
        if self.media_url:
            d["media_url"] = self.media_url
        return d


@dataclass
class MessageSend:
    """下游发给 bridge 的回复消息"""
    request_id: str = ""
    content: str = ""
    to: str = ""  # 目标用户 ID

    @classmethod
    def from_dict(cls, data: dict) -> "MessageSend":
        return cls(
            request_id=data.get("request_id", ""),
            content=data.get("content", ""),
            to=data.get("to", ""),
        )


@dataclass
class PingMessage:
    """心跳 ping"""
    request_id: str = ""

    def to_dict(self) -> dict:
        return {"type": TYPE_PING, "request_id": self.request_id}


@dataclass
class PongMessage:
    """心跳 pong"""
    request_id: str = ""

    @classmethod
    def from_dict(cls, data: dict) -> "PongMessage":
        return cls(request_id=data.get("request_id", ""))


@dataclass
class ErrorMessage:
    """错误响应"""
    request_id: str = ""
    code: int = 500
    message: str = ""

    def to_dict(self) -> dict:
        return {
            "type": TYPE_ERROR,
            "request_id": self.request_id,
            "code": self.code,
            "message": self.message,
        }


def parse_ws_message(data: dict) -> tuple[str, dict]:
    """解析 WS 消息，返回 (type, payload_dict)"""
    msg_type = data.get("type", "")
    return msg_type, data
