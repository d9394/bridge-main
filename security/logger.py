"""结构化安全日志（fail2ban 友好格式）"""

from __future__ import annotations

import logging
import logging.handlers
import os


class SecurityLogger:
    """输出 fail2ban 可解析的结构化日志，所有含 IP 的行用于 fail2ban 匹配。"""

    def __init__(self, log_dir: str, level: str = "INFO",
                 max_size_mb: int = 10, backup_count: int = 5):
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, "bridge.log")

        self.logger = logging.getLogger("ilink-bridge")
        self.logger.setLevel(getattr(logging, level.upper(), logging.INFO))
        self.logger.propagate = False

        if not self.logger.handlers:
            handler = logging.handlers.RotatingFileHandler(
                log_file,
                maxBytes=max_size_mb * 1024 * 1024,
                backupCount=backup_count,
                encoding="utf-8",
            )
            fmt = logging.Formatter(
                "%(asctime)s [%(levelname)s] %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
            handler.setFormatter(fmt)
            self.logger.addHandler(handler)

            console = logging.StreamHandler()
            console.setFormatter(fmt)
            self.logger.addHandler(console)

    def info(self, msg: str):
        self.logger.info(msg)

    def warn(self, msg: str):
        self.logger.warning(msg)

    def error(self, msg: str):
        self.logger.error(msg)

    # ── 结构化安全事件 ──

    def auth_ok(self, ip: str, app_id: str, session_id: str):
        self.logger.info(f"auth_ok ip={ip} app_id={app_id} session_id={session_id}")

    def auth_failed(self, ip: str, app_id: str, reason: str, attempts: int = 1):
        self.logger.warning(
            f"auth_failed ip={ip} app_id={app_id} reason={reason} attempts={attempts}"
        )

    def invalid_connection(self, ip: str, reason: str, path: str = ""):
        self.logger.warning(f"invalid_connection ip={ip} reason={reason} path={path}")

    def ws_closed(self, ip: str, app_id: str = ""):
        self.logger.info(f"ws_closed ip={ip} app_id={app_id}")

    def http_auth_failed(self, ip: str, path: str):
        self.logger.warning(f"http_auth_failed ip={ip} path={path}")

    def connection_refused(self, ip: str, reason: str):
        self.logger.warning(f"connection_refused ip={ip} reason={reason}")

    def heartbeat_timeout(self, ds_id: str, ip: str = ""):
        self.logger.warning(f"heartbeat_timeout ds_id={ds_id} ip={ip}")

    def downstream_online(self, ds_id: str, ip: str = ""):
        self.logger.info(f"downstream_online ds_id={ds_id} ip={ip}")

    def downstream_offline(self, ds_id: str):
        self.logger.info(f"downstream_offline ds_id={ds_id}")
