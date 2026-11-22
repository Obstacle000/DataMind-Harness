"""
Redis 高速缓存与异步重任务队列客户端

核心职责：
1. SQL 结果集语义/指纹缓存（避免高频重复查询打崩 PostgreSQL / OLAP 数仓）
2. Schema-Map 图拓扑缓存（缓存 NetworkX PageRank 计算结果）
3. 沙箱重型数据科学任务（如 ARIMA 时序预测、蒙特卡洛模拟）的异步任务队列状态追踪
4. 本地未启动 Redis 服务时，自动透明降级为线程安全的内存 TTL 字典，确保开发环境零报错
"""
from __future__ import annotations

import hashlib
import json
import threading
import time
import uuid
from typing import Any, Dict, Optional

from ..config import REDIS_URL, SQL_CACHE_TTL_SECONDS, SCHEMA_MAP_CACHE_TTL_SECONDS
from ..log_utils import agent_logger


class RedisClient:
    """支持自动内存降级的 Redis 缓存与异步队列管理器"""

    def __init__(self, url: str = REDIS_URL):
        self.url = url
        self._redis = None
        self._memory_fallback: Dict[str, tuple[float, str]] = {}
        self._lock = threading.RLock()
        self._connect()

    def _connect(self) -> None:
        try:
            import redis
            client = redis.Redis.from_url(
                self.url,
                decode_responses=True,
                socket_connect_timeout=1.5,
                socket_timeout=1.5,
            )
            client.ping()
            self._redis = client
            agent_logger.info(f"RedisClient connected: {self.url}")
        except Exception as exc:
            self._redis = None
            agent_logger.info(
                f"Redis server unavailable ({exc}), using in-memory TTL cache & queue fallback"
            )

    @property
    def is_live_redis(self) -> bool:
        return self._redis is not None

    # ============================================================
    # 基础 KV 操作（带 TTL 自动过期）
    # ============================================================

    def get(self, key: str) -> Optional[str]:
        if self._redis is not None:
            try:
                return self._redis.get(key)
            except Exception:
                pass
        with self._lock:
            entry = self._memory_fallback.get(key)
            if entry is None:
                return None
            expire_at, val = entry
            if expire_at > 0 and time.time() > expire_at:
                self._memory_fallback.pop(key, None)
                return None
            return val

    def set(self, key: str, value: str, ttl_seconds: int = 600) -> None:
        if self._redis is not None:
            try:
                self._redis.setex(key, ttl_seconds, value)
                return
            except Exception:
                pass
        with self._lock:
            expire_at = time.time() + ttl_seconds if ttl_seconds > 0 else 0.0
            self._memory_fallback[key] = (expire_at, value)

    # ============================================================
    # 1. SQL 结果集缓存
    # ============================================================

    @staticmethod
    def _sql_cache_key(sql: str, engine: str = "pg") -> str:
        normalized = " ".join((sql or "").strip().split()).lower()
        digest = hashlib.sha256(f"{engine}::{normalized}".encode("utf-8")).hexdigest()[:24]
        return f"datamind:sql_cache:{engine}:{digest}"

    def get_sql_cache(self, sql: str, engine: str = "pg") -> Optional[Dict[str, Any]]:
        raw = self.get(self._sql_cache_key(sql, engine))
        if not raw:
            return None
        try:
            data = json.loads(raw)
            if isinstance(data, dict):
                data["cache_hit"] = True
                return data
        except Exception:
            pass
        return None

    def set_sql_cache(
        self, sql: str, result: Dict[str, Any], engine: str = "pg", ttl: int = SQL_CACHE_TTL_SECONDS
    ) -> None:
        if not isinstance(result, dict) or result.get("error"):
            return
        try:
            self.set(
                self._sql_cache_key(sql, engine),
                json.dumps(result, ensure_ascii=False, default=str),
                ttl_seconds=ttl,
            )
        except Exception:
            pass

    # ============================================================
    # 2. Schema-Map (PageRank 图谱) 缓存
    # ============================================================

    def get_schema_graph_cache(self, cache_id: str = "default") -> Optional[Dict[str, Any]]:
        raw = self.get(f"datamind:schema_map:{cache_id}")
        if not raw:
            return None
        try:
            return json.loads(raw)
        except Exception:
            return None

    def set_schema_graph_cache(
        self, payload: Dict[str, Any], cache_id: str = "default", ttl: int = SCHEMA_MAP_CACHE_TTL_SECONDS
    ) -> None:
        try:
            self.set(
                f"datamind:schema_map:{cache_id}",
                json.dumps(payload, ensure_ascii=False, default=str),
                ttl_seconds=ttl,
            )
        except Exception:
            pass

    # ============================================================
    # 3. 沙箱重型数据科学任务异步队列 (Async Analytics Queue)
    # ============================================================

    def enqueue_analytics_job(
        self,
        job_type: str,
        params: Dict[str, Any],
        user_id: str = "default_user",
    ) -> Dict[str, Any]:
        """将耗时建模/预测任务推入异步队列，返回 job_id 与状态"""
        job_id = f"job_{uuid.uuid4().hex[:12]}"
        job_record = {
            "job_id": job_id,
            "job_type": job_type,
            "user_id": user_id,
            "status": "queued",
            "progress": 0,
            "params": params,
            "created_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "result": None,
        }
        self.set(f"datamind:async_job:{job_id}", json.dumps(job_record, ensure_ascii=False), ttl_seconds=3600)
        return job_record

    def update_job_progress(
        self,
        job_id: str,
        *,
        status: str = "running",
        progress: int = 50,
        result: Any = None,
    ) -> Optional[Dict[str, Any]]:
        key = f"datamind:async_job:{job_id}"
        raw = self.get(key)
        record = json.loads(raw) if raw else {"job_id": job_id}
        record["status"] = status
        record["progress"] = max(0, min(100, int(progress)))
        if result is not None:
            record["result"] = result
        record["updated_at"] = time.strftime("%Y-%m-%dT%H:%M:%S")
        self.set(key, json.dumps(record, ensure_ascii=False, default=str), ttl_seconds=3600)
        return record

    def get_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        raw = self.get(f"datamind:async_job:{job_id}")
        if not raw:
            return None
        try:
            return json.loads(raw)
        except Exception:
            return None


# 全局单例
redis_client = RedisClient()
