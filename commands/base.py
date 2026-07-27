"""命令解析基类"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional


@dataclass
class ParsedCommand:
    command: str
    args: list[str]
    raw: str


def parse_command(text: str) -> Optional[ParsedCommand]:
    """解析 /command args 格式的命令"""
    text = text.strip()
    if not text.startswith("/"):
        return None

    parts = text.split(None, 1)
    cmd = parts[0].lstrip("/").lower()
    args = parts[1].split() if len(parts) > 1 else []

    return ParsedCommand(command=cmd, args=args, raw=text)
