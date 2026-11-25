"""
Qdrant 向量检索客户端（Schema-Map 表名片 Table Card 检索 + 历史 Golden SQL 语义召回）

核心职责：
1. 管理两个 Qdrant 向量集合：
   - `datamind_table_cards`：存储每张数仓表的 Table Card（表名 + 业务描述 + LLM 离线画像同义词 + 字段解释）向量；
   - `datamind_golden_sqls`：存储历史黄金 SQL 的 `(question, sql, tables)` 向量，用于按问题相似度召回高相关表。
2. 渐进式连接与零依赖降级：
   - 若配置了 `QDRANT_URL`，连接远程 Qdrant 服务；
   - 若未配置 `QDRANT_URL` 但安装了 `qdrant-client`，使用 `QdrantClient(location=":memory:")` 本地内存模式；
   - 若环境中未安装 `qdrant-client`，自动降级为内置 L2 归一化余弦相似度内存向量库，API 100% 兼容。
3. Embedding 生成：
   - 若配置了 `EMBEDDING_API_KEY`，调用 OpenAI 兼容 Embedding 接口生成稠密向量；
   - 否则使用确定性中英文字符/词级 N-Gram 特征哈希（Feature Hashing + L2 Normalization）生成稠密向量，离线测试 0 延迟、0 费用。
"""
# 表名片集合 datamind_table_cards 里的一个 Point
# {
#   "id": "8f3c1e72-4b1a-5d2c-9a01-3e8b7c6d5f4a",
#   "vector": [0.0412, -0.1185, 0.0873, "...", "共 256 个浮点数 (L2 归一化)"],
#   "payload": {
#     "table_name": "fact_order_items",
#     "card_text": "表名: fact_order_items\n业务描述: 订单明细行事实表，记录单品成交数量、成交单价与单行毛利额（核算赚了多少钱的核心表）\n业务同义词: 订单明细, 明细行, 毛利, 利润, 赚了多少钱, 盈利, 净利, 销量\n核心字段: item_id(明细行主键), order_id(关联主订单ID), product_id(商品或零件ID), quantity(单品成交数量), unit_price(实际成交单价), line_profit(单行毛利额)",
#     "synonyms": ["订单明细", "明细行", "毛利", "利润", "赚了多少钱", "盈利", "净利", "销量"]
#   }
# }
# 黄金 SQL 集合 datamind_golden_sqls 里的一个 Point
# {
#   "id": "2a9d4c10-7e3f-5b8a-8c12-9f0e1d2c3b4a",
#   "vector": [-0.0521, 0.0943, 0.1302, "...", "共 256 个浮点数 (由 question + sql 编码)"],
#   "payload": {
#     "question": "分析各供应商及商品赚了多少钱（毛利利润）与退货退款金额对比",
#     "sql": "SELECT s.supplier_name, p.product_name, SUM(oi.line_profit) AS profit ...",
#     "tables": [
#       "dim_products",
#       "dim_suppliers",
#       "fact_order_items",
#       "fact_orders",
#       "fact_returns"
#     ]
#   }
# }
from __future__ import annotations

import hashlib
import math
import re
import threading
import uuid
from typing import Any, Dict, List, Optional

from ..config import (
    EMBEDDING_API_KEY,
    EMBEDDING_BASE_URL,
    EMBEDDING_DIM,
    EMBEDDING_MODEL,
    QDRANT_API_KEY,
    QDRANT_LOCAL_PATH,
    QDRANT_SQL_COLLECTION,
    QDRANT_TABLE_COLLECTION,
    QDRANT_URL,
)
from ..log_utils import agent_logger


class QdrantSchemaRetriever:
    """基于 Qdrant 的数仓 Schema Table-Card 与 Golden-SQL 向量检索器"""

    def __init__(self):
        self._lock = threading.RLock()
        self.table_collection = QDRANT_TABLE_COLLECTION
        self.sql_collection = QDRANT_SQL_COLLECTION
        self.dim = max(64, int(EMBEDDING_DIM or 256))
        self._qdrant = None
        self._qdrant_models = None
        self._backend_mode = "in_memory_cosine_fallback"
        # 内置降级存储: collection_name -> {point_id: {"vector": list[float], "payload": dict}}
        self._fallback_collections: Dict[str, Dict[str, Dict[str, Any]]] = {
            self.table_collection: {},
            self.sql_collection: {},
        }
        self._init_qdrant()

    def _init_qdrant(self) -> None:
        try:
            from qdrant_client import QdrantClient
            from qdrant_client.http import models as qmodels
            # 如果你使用的是 Qdrant 官方云服务，API Key 是必须的，且需要在控制台手动创建。
            if QDRANT_URL:
                self._qdrant = QdrantClient(
                    url=QDRANT_URL,
                    #
                    api_key=QDRANT_API_KEY or None,
                    timeout=5.0,
                )
                self._backend_mode = f"qdrant_remote({QDRANT_URL})"
            else:
                # 装了 pip install qdrant-client（但没起外部 Qdrant 服务） → 走 QdrantClient(location=":memory:")
                self._qdrant = QdrantClient(location=QDRANT_LOCAL_PATH or ":memory:")
                self._backend_mode = f"qdrant_embedded({QDRANT_LOCAL_PATH or ':memory:'})"
            self._qdrant_models = qmodels
            self._ensure_collections()
            agent_logger.info(f"QdrantSchemaRetriever initialized: {self._backend_mode}")
        except ImportError:
            self._qdrant = None
            self._qdrant_models = None
            # 用纯 Python 字典 + 数学公式手写的兜底替身
            agent_logger.info(
                "qdrant-client not installed; using built-in L2 cosine vector engine for Schema-Map"
            )
        except Exception as exc:
            self._qdrant = None
            self._qdrant_models = None
            agent_logger.warning(
                f"Qdrant connection failed ({exc}); falling back to built-in cosine vector engine"
            )

    def _ensure_collections(self) -> None:
        if self._qdrant is None or self._qdrant_models is None:
            return
        qmodels = self._qdrant_models
        for col_name in (self.table_collection, self.sql_collection):
            try:
                if not self._qdrant.collection_exists(col_name):
                    self._qdrant.create_collection(
                        collection_name=col_name,
                        vectors_config=qmodels.VectorParams(
                            size=self.dim,
                            distance=qmodels.Distance.COSINE,
                        ),
                    )
            except Exception as exc:
                agent_logger.warning(f"Qdrant ensure_collection({col_name}) warning: {exc}")

    # ============================================================
    # 向量编码器（支持远程 Embedding API 与本地确定性 N-Gram 特征哈希）
    # ============================================================

    def embed_text(self, text: str) -> List[float]:
        """将查询或 Table Card 编码为单位长度稠密向量"""
        cleaned = (text or "").strip().lower()
        if not cleaned:
            return [0.0] * self.dim

        if EMBEDDING_API_KEY:
            try:
                from langchain_openai import OpenAIEmbeddings
                emb = OpenAIEmbeddings(
                    model=EMBEDDING_MODEL,
                    api_key=EMBEDDING_API_KEY,
                    base_url=EMBEDDING_BASE_URL or None,
                    dimensions=self.dim,
                )
                vec = emb.embed_query(cleaned)
                # 正则化:为了伺候下面那个“没装 qdrant-client 时的纯 Python 兜底分支”：
                return self._l2_normalize(vec)
            except Exception as exc:
                agent_logger.debug(f"Remote embedding fallback to local feature hashing: {exc}")

        return self._local_ngram_hash_embedding(cleaned)

    def _local_ngram_hash_embedding(self, text: str) -> List[float]:
        """确定性中英文混合 N-Gram 特征哈希向量（支持中文单字/双字 Bigram + 英文标识符分词）"""
        # 用的是经典机器学习里的 “特征哈希投篮法（Feature Hashing）”,在没配神经网络 Embedding API Key 时, 把文字变成 256 维向量

        vec = [0.0] * self.dim
        # 1. 提取英文/数字标识符词元（按 _ 拆分表名与字段名，如 fact_orders -> fact, orders）
        ascii_tokens = re.findall(r"[a-z0-9]+", text)
        # 2. 提取中文连续片段并生成 1-gram + 2-gram + 3-gram
        cjk_chunks = re.findall(r"[\u4e00-\u9fff]+", text)

        features: List[tuple[str, float]] = []
        for tok in ascii_tokens:
            features.append((f"w:{tok}", 2.0))
        for chunk in cjk_chunks:
            n = len(chunk)
            for i in range(n):
                features.append((f"c1:{chunk[i]}", 0.8))
                if i + 1 < n:
                    features.append((f"c2:{chunk[i:i+2]}", 2.2))
                if i + 2 < n:
                    features.append((f"c3:{chunk[i:i+3]}", 2.8))

        for feat, weight in features:
            digest = hashlib.md5(feat.encode("utf-8")).digest()
            idx = int.from_bytes(digest[:4], "little") % self.dim
            sign = 1.0 if (digest[4] & 1) == 0 else -1.0
            vec[idx] += sign * weight

        return self._l2_normalize(vec)

    @staticmethod
    def _l2_normalize(vec: List[float]) -> List[float]:
        norm = math.sqrt(sum(v * v for v in vec))
        if norm <= 1e-12:
            return list(vec)
        return [v / norm for v in vec]

    @staticmethod
    def _cosine(v1: List[float], v2: List[float]) -> float:
        return float(sum(a * b for a, b in zip(v1, v2)))

    @staticmethod
    def _stable_uuid(key: str) -> str:
        return str(uuid.uuid5(uuid.NAMESPACE_DNS, f"datamind.qdrant.{key}"))

    # ============================================================
    # 写入与检索接口：1. Table Cards  2. Golden SQLs
    # ============================================================

    def upsert_table_cards(self, table_cards: Dict[str, Dict[str, Any]]) -> int:
        """批量更新数仓表名片（Table Card）向量至 Qdrant

        table_cards 格式:
            {
                "fact_orders": {
                    "card_text": "表名: fact_orders | 业务描述: ... | 同义词: GMV, 营收... | 字段: ...",
                    "synonyms": [...],
                },
                ...
            }
        """
        with self._lock:
            points = []
            self._fallback_collections[self.table_collection].clear()

            for table_name, info in table_cards.items():
                card_text = str(info.get("card_text", table_name))
                vec = self.embed_text(card_text)
                pid = self._stable_uuid(f"table::{table_name}")
                payload = {
                    "table_name": table_name,
                    "card_text": card_text,
                    "synonyms": info.get("synonyms", []),
                }
                self._fallback_collections[self.table_collection][pid] = {
                    "vector": vec,
                    "payload": payload,
                }
                if self._qdrant is not None and self._qdrant_models is not None:
                    points.append(
                        self._qdrant_models.PointStruct(
                            id=pid,
                            vector=vec,
                            payload=payload,
                        )
                    )

            if points and self._qdrant is not None:
                try:
                    self._ensure_collections()
                    self._qdrant.upsert(
                        collection_name=self.table_collection,
                        points=points,
                    )
                except Exception as exc:
                    agent_logger.warning(f"Qdrant upsert_table_cards fallback: {exc}")

            return len(table_cards)

    def upsert_golden_sqls(self, golden_records: List[Dict[str, Any]]) -> int:
        """批量更新历史 Golden SQL `(question, sql, tables)` 向量至 Qdrant"""
        with self._lock:
            points = []
            self._fallback_collections[self.sql_collection].clear()

            for item in golden_records:
                sql = str(item.get("sql", "")).strip()
                if not sql:
                    continue
                question = str(item.get("question", "")).strip()
                tables = list(item.get("tables", []) or [])
                index_text = f"{question}\n{sql}\n{' '.join(tables)}".strip()
                vec = self.embed_text(index_text)
                pid = self._stable_uuid(f"sql::{sql}")
                payload = {
                    "question": question,
                    "sql": sql,
                    "tables": tables,
                }
                self._fallback_collections[self.sql_collection][pid] = {
                    "vector": vec,
                    "payload": payload,
                }
                if self._qdrant is not None and self._qdrant_models is not None:
                    points.append(
                        self._qdrant_models.PointStruct(
                            id=pid,
                            vector=vec,
                            payload=payload,
                        )
                    )

            if points and self._qdrant is not None:
                try:
                    self._ensure_collections()
                    self._qdrant.upsert(
                        collection_name=self.sql_collection,
                        points=points,
                    )
                except Exception as exc:
                    agent_logger.warning(f"Qdrant upsert_golden_sqls fallback: {exc}")

            return len(golden_records)

    def search_table_cards(
        self,
        query: str,
        *,
        query_vector: Optional[List[float]] = None,
        top_k: int = 20,
    ) -> Dict[str, float]:
        """在 Qdrant 中检索与 query 余弦相似度最高的表名片，返回 {table_name: cosine_sim}"""
        q_vec = query_vector if query_vector is not None else self.embed_text(query)
        hits = self._search_collection(self.table_collection, q_vec, top_k=top_k)
        return {
            str(h["payload"]["table_name"]): float(h["score"])
            for h in hits
            if h.get("payload", {}).get("table_name")
        }

    def search_golden_sqls(
        self,
        query: str,
        *,
        query_vector: Optional[List[float]] = None,
        top_k: int = 3,
    ) -> List[Dict[str, Any]]:
        """在 Qdrant 中检索与 query 最相似的 Top-K 历史 Golden SQL，返回 [{question, sql, tables, score}]"""
        q_vec = query_vector if query_vector is not None else self.embed_text(query)
        hits = self._search_collection(self.sql_collection, q_vec, top_k=top_k)
        results: List[Dict[str, Any]] = []
        for h in hits:
            payload = h.get("payload", {})
            results.append({
                "question": payload.get("question", ""),
                "sql": payload.get("sql", ""),
                "tables": payload.get("tables", []),
                "score": float(h.get("score", 0.0)),
            })
        return results

    def _search_collection(
        self, collection_name: str, q_vec: List[float], top_k: int = 10
    ) -> List[Dict[str, Any]]:
        with self._lock:
            if self._qdrant is not None:
                try:
                    if hasattr(self._qdrant, "query_points"):
                        resp = self._qdrant.query_points(
                            collection_name=collection_name,
                            query=q_vec,
                            limit=top_k,
                        )
                        pts = getattr(resp, "points", []) or []
                    else:
                        pts = self._qdrant.search(
                            collection_name=collection_name,
                            query_vector=q_vec,
                            limit=top_k,
                        )
                    if pts:
                        return [
                            {
                                "score": float(getattr(p, "score", 0.0)),
                                "payload": dict(getattr(p, "payload", {}) or {}),
                            }
                            for p in pts
                        ]
                except Exception as exc:
                    agent_logger.debug(f"Qdrant query fallback to memory store: {exc}")

            store = self._fallback_collections.get(collection_name, {})
            scored = []
            for item in store.values():
                sim = self._cosine(q_vec, item["vector"])
                scored.append({"score": sim, "payload": item["payload"]})
            scored.sort(key=lambda x: x["score"], reverse=True)
            return scored[:top_k]


# 全局单例
qdrant_retriever = QdrantSchemaRetriever()
