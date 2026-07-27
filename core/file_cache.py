from __future__ import annotations

import asyncio
import hashlib
import json
import os
import time
from pathlib import Path
from typing import Optional


class FileCache:
    """文件缓存服务，提供临时存储和下载链接"""

    def __init__(
        self,
        cache_dir: str = "cache/files",
        max_days: int = 14,
        cleanup_interval: int = 3600,
        max_file_size: int = 52428800,
        log_fn=None,
    ):
        self.cache_dir = Path(cache_dir)
        self.max_days = max_days
        self.cleanup_interval = cleanup_interval
        self.max_file_size = max_file_size
        self.log = log_fn or (lambda msg: print(f"[file-cache] {msg}"))
        self._cleanup_task: Optional[asyncio.Task] = None

    async def start(self):
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.log(f"File cache started: {self.cache_dir} (max {self.max_days} days)")
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())

    async def stop(self):
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass

    async def store_file(
        self,
        file_data: bytes,
        file_name: str,
        mime_type: str = "application/octet-stream",
        source_plugin: str = "",
        target_plugin: str = "",
    ) -> str:
        """存储文件，返回 file_id"""
        if len(file_data) > self.max_file_size:
            raise ValueError(f"File size {len(file_data)} exceeds max {self.max_file_size}")

        file_id = self._generate_id(file_name)
        file_path = self.cache_dir / f"{file_id}_{file_name}"
        meta_path = self.cache_dir / f"{file_id}_{file_name}.meta.json"

        file_path.write_bytes(file_data)

        expire_time = time.time() + self.max_days * 86400
        metadata = {
            "file_id": file_id,
            "original_name": file_name,
            "mime_type": mime_type,
            "size": len(file_data),
            "upload_time": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "expire_time": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(expire_time)),
            "expire_timestamp": expire_time,
            "source_plugin": source_plugin,
            "target_plugin": target_plugin,
        }
        meta_path.write_text(json.dumps(metadata, ensure_ascii=False), encoding="utf-8")

        self.log(f"Stored: {file_id} -> {file_name} ({len(file_data)} bytes)")
        return file_id

    def get_file_path(self, file_id: str) -> Optional[Path]:
        """根据 file_id 查找文件路径"""
        for f in self.cache_dir.iterdir():
            if f.name.startswith(file_id + "_") and not f.name.endswith(".meta.json"):
                if f.exists():
                    return f
        return None

    def get_metadata(self, file_id: str) -> Optional[dict]:
        """获取文件元数据"""
        for f in self.cache_dir.iterdir():
            if f.name.startswith(file_id + "_") and f.name.endswith(".meta.json"):
                try:
                    return json.loads(f.read_text(encoding="utf-8"))
                except Exception:
                    return None
        return None

    def list_files(self) -> list[dict]:
        """列出所有缓存文件"""
        files = []
        for f in self.cache_dir.iterdir():
            if f.name.endswith(".meta.json"):
                try:
                    meta = json.loads(f.read_text(encoding="utf-8"))
                    files.append(meta)
                except Exception:
                    pass
        return files

    async def cleanup_expired(self):
        """清理过期文件"""
        now = time.time()
        removed = 0
        for f in self.cache_dir.iterdir():
            if f.name.endswith(".meta.json"):
                try:
                    meta = json.loads(f.read_text(encoding="utf-8"))
                    if meta.get("expire_timestamp", 0) < now:
                        data_file = self.cache_dir / f.name.replace(".meta.json", "")
                        if data_file.exists():
                            data_file.unlink()
                        f.unlink()
                        removed += 1
                except Exception:
                    pass
        if removed > 0:
            self.log(f"Cleaned up {removed} expired files")

    async def _cleanup_loop(self):
        while True:
            try:
                await asyncio.sleep(self.cleanup_interval)
                await self.cleanup_expired()
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.log(f"Cleanup error: {e}")

    def _generate_id(self, file_name: str) -> str:
        ts = str(int(time.time()))
        h = hashlib.md5(f"{ts}{file_name}".encode()).hexdigest()[:8]
        return f"{ts}_{h}"
