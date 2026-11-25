"""
Schema-Map 数仓血缘图谱与 Table-Rank 引擎

================================================================================
# build_schema_map_prompt-{load_schema()->_build_adjacency()->compute_table_rank()->resolve_connected_subgraph}
调用 build_schema_map_prompt(query)
  │
  ├─► 第 1 步：拿表结构 (self._schema)
  │     先查 Redis 缓存；没命中再去 PostgreSQL 查出所有表名、字段名、物理外键。
  │     （若运行期 SQL 报字段/表不存在，handle_sql_schema_error 会自动 force_refresh=True 击穿脏缓存）
  │
  ├─► 第 2 步：算表结构 MD5 指纹，决定要不要跑“第一阶段探边与对账”
  │     ├─ 指纹没变 ──> 0ms 直接跳过第 3 步！（复用内存里现成的 self._relationships）
  │     └─ 指纹变了（或首次启动）──> 进入第 3 步
  │
  ├─► 第 3 步：【第一阶段】三步对账 + 三层漏斗找齐所有外键边 (self._relationships) 并落盘 YAML
  │     ① 剪枝死边 (_prune_stale_relationships)：读 schema_lineage.yaml 旧记录，踢掉在最新表结构里已被删/改名的死边；
  │     ② 合入第 1 层（物理外键）+ 第 2 层（SQLGlot 解析历史 SQL 里的 JOIN ON，自动校验字段存活）；
  │     ③ 合入第 3 层（对没外键的孤岛表算“数据值包含率 ≥ 80%” + 语义域/大模型二分类盖章）；
  │     ④ 按规范化四元组 (左表, 左列, 右表, 右列) 严格去重后，原子覆写回 schema_lineage.yaml。
  │
  ├─► 第 4 步：【第二阶段】把四元组变成“无向加权图” (_build_adjacency)
  │     遍历 self._relationships，组装出：
  │     - 邻接表 adj（哪张表和哪张表相连）
  │     - 边条件 edge_join_conditions（相连的两张表用哪些字段 ON，复合键自动用 AND 拼接）
  │     - 边权重 edge_weights（验证次数越多、复合键越具体，边权重越高）
  │
  ├─► 第 5 步：【第二阶段】跑个性化 PageRank 选出 Top-K 核心表 (compute_table_rank)
  │     ① 看用户的 query 命中了哪几张表（如“客户”“退货”），给它们加高初始偏置权重；
  │     ② 顺着第 4 步的无向加权图跑 PageRank 概率游走（把中间的枢纽表“订单表”也顶上来）；
  │     ③ 按得分从高到低排序，截取前 K 张表（selected = Top-K）。
  │
  └─► 第 6 步：【第二阶段】连通补桥 + 主干/备选双栏划分 (resolve_connected_subgraph)
        ① 迭代 BFS 连通补桥：检查选中表集合内部是否存在孤岛（多个连通分量）；
           若断开，在全库大图上跑多源 BFS 找到最短路径，把路径上的中间桥表（支持多跳桥）并入集合，
           反复迭代直到集合内所有可达表完全连通，并将补进来的桥表字段同步补充到上方表清单！
        ② 带权最大生成树（MST）分两栏输出（既不丢边，又防三角形环路）：
           收集补桥后集合内的全部关联边，按权重从高到低用并查集（Kruskal）划分为两栏：
           - 第一栏【🌲 默认主干连法】：N 张表选出权重最高的 N-1 条无环生成树主干边，直接连通所有表；
           - 第二栏【🔄 备选替换连法】：其余会形成三角形/环路的边（如明细级复合键边）绝不丢弃，
             放入备选栏并提示大模型“按需替换主干，严禁同时堆叠形成环形重复 JOIN”！
================================================================================
"""
# 为什么第 6 步要做成「迭代 BFS 补桥 + 主干/备选两栏分治」？
# 1. 为什么不能只补“一座桥”？（防多跳断桥）
#   如果选出的 Top-5 里有 A、B、C 三个孤岛，或者 A 和 B 之间隔了 X -> Y 两跳桥（它们刚好排在第 6、7 名落选了），
#   单次找一座桥会漏掉多跳。因此必须用 while 循环检查连通分量 (_get_components)，反复在全图跑多源 BFS，
#   直到选中集合内部变成 1 个完整的连通分量（或者确认某张表在全库里本来就是真·孤立表）。
# 2. 为什么不能“简单砍掉环路第 3 条边”，也不能“不加区分全丢给大模型”？
#   假设关联关系是一个三角形环路：
#         订单表 (fact_orders)
#         /            \
#        /              \
# 订单明细表 ────────── 退货表 (fact_returns)
#   - 如果用单起点 BFS 硬砍掉第 3 条边（比如砍掉 订单明细表 ── 退货表）：
#     当业务刚好需要按“商品明细粒度（order_id AND product_id）”查退货时，大模型看不到这条边，只能绕道订单表按 order_id 连，引发数据扇出放大 Bug！
#   - 如果把 3 条边混在一起全丢给大模型：
#     大模型会老老实实把 3 条边全写进同一条 SQL，导致 3 张表连 3 次绕成死圈。
#   - 终极解法（像手机导航一样分「主路线」与「备选替换路线」两栏）：
#     用并查集按权重选出 N-1 条无环边放进【1. 默认主干连法】，把剩下的环路边放进【2. 备选替换连法】，
#     既保证了关键细粒度边 100% 不丢失，又通过显式分栏+防环警告阻止大模型把三角形的 3 条边同时写进一条 SQL！

# 重要:这个大纲最终输出的关联不是看_relationships, _relationships决定graph里的节点边权重,,节点边权重产生生成树 决定backbone_edges和alternative_edges
# _relationships（全库四元组真相源 + verified_count）
#     ↓ _build_adjacency()
# adj（邻接表） + joined_conds（复合 ON 条件） + edge_weights（边权重）
#     ↓ compute_table_rank()
# 加权 PageRank 排序 → 选出 Top-K 核心表
#     ↓ resolve_connected_subgraph()
# ① 多源 BFS 补桥（用 adj 打通孤岛）
# ② Kruskal 最大生成树（按 edge_weights 排序 + 并查集防环）
#     → 拆出 backbone_edges（默认无环主干） + alternative_edges（备选替换边）
#     ↓ build_schema_map_prompt()
# Aider 二分控预算 → 完整渲染表字段 + 主干/备选 JOIN 路径给大模型

# 你可能觉得饶了好大一圈,一开始不是已经有关联了吗,并非,一开始拿到的是全部的关联数据,而我们需要根据用户query,得到最符合的前K个表,所以需要pagerank算法
# 所以repomap是每一次提问重新生成,而relationships,能离线生成
# 第一阶段:根据schema,同步_relationships和schema_lineage.yaml - lifespan
# 第二阶段:根据relationships,调用_build_adjacency获得关系,compute_table_rank连线,然后获取前k个,resolve_connected_subgraph做bfs+创建生成树,最终获得关系 - 每次提问模型调用工具
from __future__ import annotations

from collections import deque
from datetime import datetime
import hashlib
import json
import math
import os
from pathlib import Path
import re
import threading
from typing import Any, Dict, List, Optional, Set, Tuple
import yaml

from .postgres_client import postgres_client
from .qdrant_client import qdrant_retriever
from .redis_client import redis_client
from ..log_utils import agent_logger

LINEAGE_YAML_PATH = Path(__file__).parent / "schema_lineage.yaml"

# 业务实体语义域词典（用于在离线无 LLM Key 时对数据包含率达标的候选外键做语义盖章，防止客户ID=商品ID巧合碰撞）
_ENTITY_SEMANTIC_DOMAINS: Dict[str, Tuple[str, ...]] = {
    "order": ("order", "ord", "订单", "单号", "业务单"),
    "customer": ("customer", "cust", "buyer", "user", "客户", "买家", "用户"),
    "supplier": ("supplier", "vendor", "provider", "供应商", "合作方", "厂家"),
    "product": ("product", "prod", "sku", "part", "item", "商品", "零件", "物料"),
    "return": ("return", "refund", "退货", "退款", "售后"),
    "claim": ("claim", "索赔"),
    "inventory": ("inventory", "snapshot", "stock", "库存", "快照"),
}


class SchemaMapEngine:
    """两阶段数仓血缘引擎：第一阶段三层漏斗探边落盘（四元组严格去重），第二阶段加权 PageRank 寻路"""

    def __init__(self, lineage_path: Path = LINEAGE_YAML_PATH):
        self.lineage_path = lineage_path
        self._lock = threading.RLock()
        # "fact_quality_claims": {
        #         "description": "供应商质量索赔孤岛表（无物理外键、无历史SQL、字段异名：ord_ref_no / vendor_code）",
        #         "primary_key": "claim_id",
        #         "columns": [
        #             {"name": "claim_id",     "type": "BIGINT",        "comment": "索赔单唯一主键"},
        #             {"name": "ord_ref_no",   "type": "BIGINT",        "comment": "关联业务订单号（异名外键，未建物理约束）"},
        #             {"name": "vendor_code",  "type": "BIGINT",        "comment": "责任供应商编码（异名外键，未建物理约束）"},
        #             {"name": "claim_region", "type": "VARCHAR(32)",   "comment": "发生大区（低基数维度列）"},
        #             {"name": "claim_amount", "type": "NUMERIC(14,2)", "comment": "质量索赔扣款金额"},
        #             {"name": "claim_status", "type": "VARCHAR(32)",   "comment": "索赔状态"},
        #         ],
        #         "foreign_keys": [],  # <-- 注意：这里物理外键是空的！靠第一阶段探出来后存进 self._relationships
        #     }
        self._schema: Dict[str, Dict[str, Any]] = {}
        # 严格按规范化四元组唯一去重: (left_table, left_col, right_table, right_col) -> dict
        # 只存单向
        # ("dim_customers", "customer_id", "fact_orders", "customer_id"): {
        #         "left_table": "dim_customers",
        #         "left_col": "customer_id",
        #         "right_table": "fact_orders",
        #         "right_col": "customer_id",
        #         "source": "semantic_layer",
        #         "verified_count": 7,
        #         "inclusion_ratio": 1.0,
        #         "last_verified_at": "2026-11-22T15:33:19",
        #     }
        self._relationships: Dict[Tuple[str, str, str, str], Dict[str, Any]] = {}
        # 离线 LLM 表画像缓存: {table_name: {"business_description": str, "synonyms": list[str], "column_glossary": dict}}
        self._table_profiles: Dict[str, Dict[str, Any]] = {}
        # 历史 Golden SQL 问答记录: [{"question": str, "sql": str, "tables": list[str]}]
        self._golden_sql_records: List[Dict[str, Any]] = []
        # 表结构 MD5 指纹（替代死板的 bool 开关）：表结构未变时 0ms 跳过第一阶段；表结构一旦改变，指纹不一致自动触发三步对账更新
        self._bootstrapped_schema_hash: str = ""
        self._yaml_loaded_once: bool = False

    @staticmethod
    def _compute_schema_hash(schema: Dict[str, Dict[str, Any]]) -> str:
        """计算当前数仓表结构（表名 + 字段名 + 字段类型 + 物理外键）的 MD5 指纹"""
        try:
            payload = json.dumps(schema, sort_keys=True, ensure_ascii=False)
        except Exception:
            payload = str(sorted(schema.keys()))
        return hashlib.md5(payload.encode("utf-8")).hexdigest()

    # ============================================================
    # 规范化四元组去重器 (Canonical 4-Tuple Deduplication)
    # ============================================================

    @staticmethod
    def _canonical_4tuple(
        t1: str, c1: str, t2: str, c2: str
    ) -> Tuple[str, str, str, str]:
        """将无向边 (t1.c1 = t2.c2) 归一化为字典序唯一的四元组，彻底杜绝正反重复与同表复合键覆盖"""
        t1, c1, t2, c2 = t1.strip(), c1.strip(), t2.strip(), c2.strip()
        if (t1, c1) <= (t2, c2):
            return (t1, c1, t2, c2)
        return (t2, c2, t1, c1)

    @classmethod
    def _make_relationship_record(
        cls,
        t1: str,
        c1: str,
        t2: str,
        c2: str,
        *,
        source: str,
        verified_count: int = 1,
        inclusion_ratio: float = 1.0,
        last_verified_at: str = "",
    ) -> Tuple[Tuple[str, str, str, str], Dict[str, Any]]:
        key = cls._canonical_4tuple(t1, c1, t2, c2)
        stamp = last_verified_at or datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        return key, {
            "left_table": key[0],
            "left_col": key[1],
            "right_table": key[2],
            "right_col": key[3],
            "source": source,
            "verified_count": max(1, int(verified_count)),
            "inclusion_ratio": round(float(inclusion_ratio), 4),
            "last_verified_at": stamp,
        }

    def _upsert_edge_in_memory(
        self,
        t1: str,
        c1: str,
        t2: str,
        c2: str,
        *,
        source: str,
        verified_count: int = 1,
        inclusion_ratio: float = 1.0,
        last_verified_at: str = "",
        increment_if_exists: bool = False,
    ) -> bool:
        """向内存四元组字典插入或合并一条边；返回 True 表示新增或发生了更新"""
        if not (t1 and c1 and t2 and c2) or t1 == t2:
            return False
        key, record = self._make_relationship_record(
            t1, c1, t2, c2,
            source=source,
            verified_count=verified_count,
            inclusion_ratio=inclusion_ratio,
            last_verified_at=last_verified_at,
        )
        existing = self._relationships.get(key)
        if existing is None:
            self._relationships[key] = record
            return True
        if increment_if_exists:
            existing["verified_count"] = int(existing.get("verified_count", 1)) + 1
            existing["last_verified_at"] = record["last_verified_at"]
            return True
        return False

    # ============================================================
    # 第一阶段：三层探边漏斗 (Phase 1: Offline/Startup Graph Building)
    # ============================================================

    def load_schema(self, force_refresh: bool = False) -> Dict[str, Dict[str, Any]]:
        # 执行完这个函数后关系字典和schema_lineage都是最新状态
        with self._lock:
            if not force_refresh:
                # 1. 先看 Redis 缓存里有没有存好的表结构
                cached = redis_client.get_schema_graph_cache("warehouse_v1")
                if cached:
                    self._schema = cached
                    self._run_phase1_discovery_funnel()
                    return self._schema
            # 2. 缓存没命中（或已过期），才真正连 PostgreSQL 查 information_schema 系统表
            self._schema = postgres_client.fetch_schema_metadata()
            # 3. 查出来后存进 Redis，默认缓存 1800 秒（30 分钟）
            redis_client.set_schema_graph_cache(self._schema, "warehouse_v1")
            self._run_phase1_discovery_funnel(force_reconcile=force_refresh)
            return self._schema

    def _prune_stale_relationships(self) -> int:
        """对账第 1 步：剪枝死边（剔除最新表结构中已被删除/改名的表、列或已移除的物理外键）"""
        # 拿到最新列
        live_table_cols: Dict[str, Set[str]] = {
            t_name: {c["name"] for c in meta.get("columns", [])}
            for t_name, meta in self._schema.items()
        }
        # 拿到最新物理外键
        live_physical_fks: Set[Tuple[str, str, str, str]] = set()
        for t_name, meta in self._schema.items():
            for fk in meta.get("foreign_keys", []):
                live_physical_fks.add(
                    self._canonical_4tuple(t_name, fk["column"], fk["ref_table"], fk["ref_column"])
                )
        # 原本是物理外键,现在不是了的键
        stale_keys: List[Tuple[str, str, str, str]] = []
        for key, rel in self._relationships.items():
            lt, lc, rt, rc = key
            # 1. 表或列在最新数据库中已不存在（被 DROP 或 RENAME）
            if (
                lt not in live_table_cols
                or lc not in live_table_cols[lt]
                or rt not in live_table_cols
                or rc not in live_table_cols[rt]
            ):
                stale_keys.append(key)
                continue
            # 2. 原本是物理外键，但 DBA 在最新表结构中移除了该外键约束
            if rel.get("source") == "physical_fk" and key not in live_physical_fks:
                stale_keys.append(key)

        for k in stale_keys:
            self._relationships.pop(k, None)
            agent_logger.info(f"Pruned stale lineage edge after schema change: {k}")

        return len(stale_keys)

    def _run_phase1_discovery_funnel(self, force_reconcile: bool = False) -> None:
        """执行第一阶段三层探边漏斗 + 表结构变更三步对账（剪枝死边 -> 同步外键 -> 增量探新 -> 覆写 YAML）"""
        if not self._schema:
            # 这个函数是大模型调用工具的时候执行的,直接报错会导致崩溃,所以返回空
            # 让大模型以为没查到
            return

        current_hash = self._compute_schema_hash(self._schema)
        if (
            not force_reconcile
            and current_hash == self._bootstrapped_schema_hash
            and self._relationships
        ):
            return  # 表结构 MD5 指纹未变，0 毫秒直接放行！
        # 指纹变了,意味着表结构发生了变化,只有_schema是最新的,关系字典和schema_lineage都需要修改
        self._bootstrapped_schema_hash = current_hash

        raw_data: Dict[str, Any] = {}
        if self.lineage_path.exists():
            try:
                raw_data = yaml.safe_load(self.lineage_path.read_text(encoding="utf-8")) or {}
            except Exception as exc:
                agent_logger.warning(f"Failed to read {self.lineage_path}: {exc}")
        # 记录内存里的关系表 self._relationships 有没有发生过任何变化，从而决定最后要不要重新写一次 schema_lineage.yaml 磁盘文件。
        dirty = False

        # 0. 首次从 schema_lineage.yaml 加载持久化关系（同时自动对文件做四元组去重）
        if not self._yaml_loaded_once:
            self._yaml_loaded_once = True
            rel_list = raw_data.get("relationships", []) or []
            seen_in_file: Set[Tuple[str, str, str, str]] = set()
            for item in rel_list:
                if not isinstance(item, dict):
                    continue
                lt = str(item.get("left_table", "")).strip()
                lc = str(item.get("left_col", "")).strip()
                rt = str(item.get("right_table", "")).strip()
                rc = str(item.get("right_col", "")).strip()
                if not (lt and lc and rt and rc) or lt == rt:
                    continue
                key = self._canonical_4tuple(lt, lc, rt, rc)
                if key in seen_in_file:
                    # 防止用户写入文件已经存在的关系
                    dirty = True
                seen_in_file.add(key)
                # 装进内存字典 self._relationships
                self._upsert_edge_in_memory(
                    lt, lc, rt, rc,
                    source=str(item.get("source", "semantic_layer")),
                    verified_count=int(item.get("verified_count", 1)),
                    inclusion_ratio=float(item.get("inclusion_ratio", 1.0)),
                    last_verified_at=str(item.get("last_verified_at", "")),
                    increment_if_exists=False,
                )

        # 对账第 1 步：【剪枝死边】拿着现有关系与最新表结构对账，踢掉已被删表/删列/改名的幽灵边
        pruned_count = self._prune_stale_relationships()
        dirty = dirty or (pruned_count > 0)

        # 对账第 2 步：【合入最新物理外键 & 历史 SQL 边】
        for table_name, meta in self._schema.items():
            for fk in meta.get("foreign_keys", []):
                added = self._upsert_edge_in_memory(
                    table_name,
                    fk["column"],
                    fk["ref_table"],
                    fk["ref_column"],
                    source="physical_fk",
                    verified_count=3,
                    inclusion_ratio=1.0,
                )
                dirty = dirty or added

        raw_seeds = raw_data.get("golden_sql_seed", []) or []
        seed_sqls, normalized_golden = self._normalize_golden_sql_seeds(
            raw_seeds, set(self._schema.keys())
        )
        self._golden_sql_records = normalized_golden

        # 加载已缓存的离线表画像 table_profiles
        raw_profiles = raw_data.get("table_profiles", {})
        if isinstance(raw_profiles, dict):
            self._table_profiles.update(raw_profiles)

        if seed_sqls:
            mined_new = self.mine_join_edges_from_sql_history(
                seed_sqls, persist=False, only_new=True
            )
            dirty = dirty or (mined_new > 0)
        # 到现在为止改的都是内存里的_relationships
        # 对账第 3 步：【增量探测新表/改名新列】执行「数据值包含率初筛 + 大模型语义盖章」
        discovered_new = self.discover_hidden_foreign_keys(persist=False)
        dirty = dirty or (discovered_new > 0)

        # 对账第 4 步：【离线 LLM 自动补齐缺失表画像 + 同步灌入 Qdrant 向量数据库】
        profile_added = self._ensure_table_profiles()
        dirty = dirty or profile_added
        self._sync_qdrant_vector_index()

        # 只要发生了死边剔除、新边发现或新表画像补齐，立即原子覆写更新 schema_lineage.yaml！
        if dirty:
            self._persist_lineage_to_yaml()

    def discover_hidden_foreign_keys(
        self,
        *,
        min_inclusion_ratio: float = 0.8,
        persist: bool = True,
    ) -> int:
        """第一阶段第 3 层：数据值集合包含率初筛 (Inclusion Dependency) + 大模型/语义域二分类盖章"""
        with self._lock:
            schema = self._schema or postgres_client.fetch_schema_metadata()
            if not schema:
                return 0

            # Step 1: 锁定全库各表的“主键靶子” (Primary Key Targets)
            pk_targets: Dict[str, str] = {}
            for t_name, meta in schema.items():
                pk = meta.get("primary_key")
                col_names = [c["name"] for c in meta.get("columns", [])]
                if pk and pk in col_names:
                    pk_targets[t_name] = pk
                elif col_names:
                    pk_targets[t_name] = col_names[0]

            # 统计已存在关联的四元组，避免对已知外键重复探查
            known_4tuples = set(self._relationships.keys())
            new_discovered_count = 0

            # Step 2: 扫描各表的非主键候选外键列，计算与各表主键靶子的数据值包含率
            # 这里可以再唯一列和主键列当中计算覆盖,但是怕有些表不做唯一限制
            for child_table, meta in schema.items():
                child_pk = pk_targets.get(child_table, "")
                for col_meta in meta.get("columns", []):
                    child_col = col_meta["name"]
                    col_type = str(col_meta.get("type", "")).upper()

                    # 跳过该表自身的主键列（防止两张表的主键 1,2,3 自增碰撞），并跳过金额/浮点/日期列
                    if child_col == child_pk:
                        continue
                    if any(
                        w in child_col.lower()
                        for w in ("amount", "price", "profit", "cost", "fee", "date", "qty", "score", "status", "name", "reason", "region", "city", "tier", "phone", "category")
                    ):
                        continue
                    if any(t in col_type for t in ("NUMERIC", "DOUBLE", "FLOAT", "DATE", "TIMESTAMP")):
                        continue

                    for target_table, target_pk in pk_targets.items():
                        if child_table == target_table:
                            continue
                        candidate_key = self._canonical_4tuple(
                            child_table, child_col, target_table, target_pk
                        )
                        if candidate_key in known_4tuples:
                            continue

                        # 在 PostgreSQL 实体数仓中计算子表列值落入目标表主键集合的包含率 (若本地未起 PG 则由 postgres_client 内部自动降级)
                        # CAST(c.child_col AS VARCHAR) 的意思就是：把子表这一列的值，强制转成字符串类型，然后再和父表的值比较。
                        probe_sql = f"""
                        SELECT
                            (SELECT COUNT(DISTINCT {child_col}) FROM {child_table} WHERE {child_col} IS NOT NULL) AS child_dist,
                            COUNT(DISTINCT c.{child_col}) AS matched_dist
                        FROM {child_table} c
                        JOIN {target_table} p
                          ON CAST(c.{child_col} AS VARCHAR) = CAST(p.{target_pk} AS VARCHAR)
                        """
                        res = postgres_client.execute_readonly_query(probe_sql)
                        if res.get("error") or not res.get("rows"):
                            continue
                        row = res["rows"][0]
                        child_dist = int(row.get("child_dist") or 0)# join前字段里值的数量
                        matched_dist = int(row.get("matched_dist") or 0)# join后字段里值的数量
                        if child_dist == 0 or matched_dist == 0:
                            continue

                        inclusion_ratio = matched_dist / child_dist
                        if inclusion_ratio < min_inclusion_ratio:
                            continue

                        # Step 3: 数据值包含率达标 (>= 80%)！交由大模型或语义域规则做二分类盖章（防巧合碰撞）
                        # 光看两列里面的数字一模一样还不行，还得看这两列的名字和中文意思是不是指同一个东西，防止数字纯属巧合撞车
                        if not self._verify_candidate_semantics(
                            child_table, col_meta, target_table, target_pk, schema[target_table]
                        ):
                            agent_logger.debug(
                                f"Semantic gate vetoed coincidental match: {child_table}.{child_col} -> {target_table}.{target_pk}"
                            )
                            continue

                        # Step 4: 闯关成功！按四元组去重写入第二源
                        added = self._upsert_edge_in_memory(
                            child_table,
                            child_col,
                            target_table,
                            target_pk,
                            source="inclusion_llm_verified",
                            verified_count=2,
                            inclusion_ratio=inclusion_ratio,
                        )
                        if added:
                            known_4tuples.add(candidate_key)
                            new_discovered_count += 1
                            agent_logger.info(
                                f"Phase-1 discovered hidden FK: {child_table}.{child_col} = {target_table}.{target_pk} "
                                f"(inclusion={inclusion_ratio:.0%})"
                            )

            if new_discovered_count > 0 and persist:
                self._persist_lineage_to_yaml()
            return new_discovered_count

    @staticmethod
    def _verify_candidate_semantics(
        child_table: str,
        child_col_meta: Dict[str, Any],
        target_table: str,
        target_pk: str,
        target_table_meta: Dict[str, Any],
    ) -> bool:
        """第一阶段 Step 3：对数据值包含率达标的候选对做语义盖章（优先轻量 LLM 二分类，无 Key 时走实体语义域对齐）"""
        child_col = str(child_col_meta.get("name", ""))
        child_comment = str(child_col_meta.get("comment", ""))
        target_desc = str(target_table_meta.get("description", ""))

        # 1. 先检查实体语义域是否对齐（既快又能拦截跨实体碰撞）
        child_text = f"{child_col} {child_comment}".lower()
        target_text = f"{target_table} {target_pk} {target_desc}".lower()

        child_domains = {
            dom for dom, kws in _ENTITY_SEMANTIC_DOMAINS.items()
            if any(k in child_text for k in kws)
        }
        target_domains = {
            dom for dom, kws in _ENTITY_SEMANTIC_DOMAINS.items()
            if any(k in target_text for k in kws)
        }
        if child_domains and target_domains:
            return bool(child_domains & target_domains)

        # 2. 若配置了 DEEPSEEK_API_KEY 且语义域词典未覆盖，调用非思考模式 LLM 做 YES/NO 盖章
        if os.getenv("DEEPSEEK_API_KEY", "").strip():
            try:
                from ..config import get_llm
                llm = get_llm(thinking=False, timeout=5, max_tokens=16)
                prompt = (
                    "你是数仓建模专家。数据探查发现以下两列的数据值包含率高达 100%，"
                    "请判断它们在业务语义上是否属于真实的主外键关联（只回答 YES 或 NO）：\n"
                    f"- 子表列: {child_table}.{child_col} (注释: {child_comment})\n"
                    f"- 目标主键: {target_table}.{target_pk} (表说明: {target_desc})"
                )
                resp = llm.invoke(prompt)
                answer = str(getattr(resp, "content", "")).strip().upper()
                return "YES" in answer
            except Exception:
                pass

        return False

    def _persist_lineage_to_yaml(self) -> None:
        """将按四元组严格去重后的关系表、离线 LLM 表画像与 Golden SQL 原子写回 schema_lineage.yaml"""
        with self._lock:
            try:
                existing_seeds: List[Any] = list(self._golden_sql_records)
                existing_profiles: Dict[str, Any] = dict(self._table_profiles)
                if self.lineage_path.exists():
                    old = yaml.safe_load(self.lineage_path.read_text(encoding="utf-8")) or {}
                    if not existing_seeds:
                        existing_seeds = old.get("golden_sql_seed", []) or []
                    if not existing_profiles and isinstance(old.get("table_profiles"), dict):
                        existing_profiles = old.get("table_profiles", {})

                sorted_rels = [
                    self._relationships[k]
                    for k in sorted(self._relationships.keys())
                ]
                payload: Dict[str, Any] = {
                    "relationships": sorted_rels,
                    "table_profiles": existing_profiles,
                    "golden_sql_seed": existing_seeds,
                }
                header = (
                    "# ============================================================\n"
                    "# DataMind-Harness 自演进数仓语义层血缘契约 (Self-Evolving Semantic Lineage)\n"
                    "# 1. 严格按规范化四元组 (left_table, left_col, right_table, right_col) 唯一去重\n"
                    "# 2. 汇集三层探边漏斗成果: physical_fk / sql_ast_mined / inclusion_llm_verified\n"
                    "# 3. 缓存离线 LLM 表画像 (table_profiles) 与历史黄金 SQL 问答对，用于灌入 Qdrant 向量库\n"
                    "# ============================================================\n\n"
                )
                yaml_body = yaml.safe_dump(
                    payload,
                    allow_unicode=True,
                    sort_keys=False,
                    default_flow_style=False,
                )
                self.lineage_path.write_text(header + yaml_body, encoding="utf-8")
            except Exception as exc:
                agent_logger.warning(f"Failed to persist self-evolving lineage YAML: {exc}")

    # ============================================================
    # 离线 LLM 表画像补齐 + Table Card 构建 + Qdrant 向量库同步
    # ============================================================

    @classmethod
    def _extract_tables_from_sql(cls, sql: str, known_tables: Set[str]) -> List[str]:
        """从单条 SQL 中提取涉及的所有真实数仓表名"""
        found: Set[str] = set()
        try:
            import sqlglot
            from sqlglot import exp

            for parsed in sqlglot.parse(sql):
                if parsed is None:
                    continue
                for tbl in parsed.find_all(exp.Table):
                    if tbl.name in known_tables:
                        found.add(tbl.name)
        except Exception:
            pass

        if not found:
            for t_name in known_tables:
                if re.search(rf"\b{re.escape(t_name)}\b", sql, re.IGNORECASE):
                    found.add(t_name)
        return sorted(found)

    @classmethod
    def _normalize_golden_sql_seeds(
        cls, raw_seeds: List[Any], known_tables: Set[str]
    ) -> Tuple[List[str], List[Dict[str, Any]]]:
        """兼容纯字符串 SQL 与结构化 {question, sql, tables} 黄金问答种子"""
        sql_list: List[str] = []
        records: List[Dict[str, Any]] = []
        for item in raw_seeds:
            if isinstance(item, str) and item.strip():
                sql_str = item.strip()
                tables = cls._extract_tables_from_sql(sql_str, known_tables)
                sql_list.append(sql_str)
                records.append({"question": "", "sql": sql_str, "tables": tables})
            elif isinstance(item, dict):
                sql_str = str(item.get("sql", "")).strip()
                if not sql_str:
                    continue
                q_str = str(item.get("question", "")).strip()
                raw_tables = item.get("tables")
                if isinstance(raw_tables, list) and raw_tables:
                    tables = sorted({str(t) for t in raw_tables if str(t) in known_tables})
                else:
                    tables = cls._extract_tables_from_sql(sql_str, known_tables)
                sql_list.append(sql_str)
                records.append({"question": q_str, "sql": sql_str, "tables": tables})
        return sql_list, records

    def _ensure_table_profiles(self) -> bool:
        """离线第 1 步：对缺失画像的新表自动抽样并由 LLM（或元数据合成）补齐业务描述与同义词"""
        changed = False
        for table_name, meta in self._schema.items():
            existing = self._table_profiles.get(table_name)
            if isinstance(existing, dict) and existing.get("business_description"):
                continue

            desc = str(meta.get("description", "") or f"数仓表 {table_name}")
            col_glossary = {
                str(c["name"]): str(c.get("comment", "") or c["name"])
                for c in meta.get("columns", [])
            }
            synonyms: List[str] = []
            for dom_kws in _ENTITY_SEMANTIC_DOMAINS.values():
                if any(k in table_name.lower() or k in desc.lower() for k in dom_kws):
                    synonyms.extend(dom_kws)

            # 若配置了 LLM API Key，则抽样 3 行样本数据请大模型自动补齐表画像与业务同义词
            if os.getenv("DEEPSEEK_API_KEY", "").strip() or os.getenv("LLM_API_KEY", "").strip():
                try:
                    from pydantic import BaseModel, Field
                    from ..config import get_llm

                    class _LLMTableProfile(BaseModel):
                        business_description: str = Field(description="一句话中文业务含义")
                        synonyms: List[str] = Field(default_factory=list, description="5~10个业务同义词或常见口语问法")
                        column_glossary: Dict[str, str] = Field(default_factory=dict, description="字段名到中文含义映射")

                    sample_res = postgres_client.execute_readonly_query(
                        f"SELECT * FROM {table_name} LIMIT 3"
                    )
                    sample_rows = sample_res.get("rows", [])[:3]
                    llm = get_llm(thinking=False, timeout=8, max_tokens=512).with_structured_output(_LLMTableProfile)
                    prof = llm.invoke(
                        f"请为数仓表 `{table_name}` 生成业务语义画像。\n"
                        f"现有表注释: {desc}\n"
                        f"字段列表: {meta.get('columns', [])}\n"
                        f"样本数据(前3行): {sample_rows}"
                    )
                    if prof and prof.business_description:
                        desc = prof.business_description
                        synonyms = list(dict.fromkeys([*synonyms, *(prof.synonyms or [])]))
                        col_glossary.update(prof.column_glossary or {})
                except Exception as exc:
                    agent_logger.debug(f"Offline LLM table profiling fallback for {table_name}: {exc}")

            self._table_profiles[table_name] = {
                "business_description": desc,
                "synonyms": list(dict.fromkeys(synonyms)),
                "column_glossary": col_glossary,
            }
            changed = True
        return changed

    def _build_table_card_text(self, table_name: str, meta: Dict[str, Any]) -> Dict[str, Any]:
        """离线第 2 步：将单张表的表名 + 描述 + 离线画像同义词 + 字段注释拼成一张 Table Card"""
        prof = self._table_profiles.get(table_name, {}) or {}
        b_desc = prof.get("business_description") or meta.get("description", "")
        synonyms = prof.get("synonyms", []) or []
        glossary = prof.get("column_glossary", {}) or {}

        col_parts: List[str] = []
        for col in meta.get("columns", []):
            c_name = str(col.get("name", ""))
            c_comm = glossary.get(c_name) or str(col.get("comment", ""))
            col_parts.append(f"{c_name}({c_comm})" if c_comm else c_name)

        card_text = (
            f"表名: {table_name}\n"
            f"业务描述: {b_desc} {meta.get('description', '')}\n"
            f"业务同义词: {', '.join(str(s) for s in synonyms)}\n"
            f"核心字段: {', '.join(col_parts)}"
        )
        return {"card_text": card_text, "synonyms": synonyms}

    def _sync_qdrant_vector_index(self) -> None:
        """将所有表的 Table Cards 与历史 Golden SQL 同步写入 Qdrant 向量数据库"""
        if not self._schema:
            return
        table_cards = {
            t_name: self._build_table_card_text(t_name, meta)
            for t_name, meta in self._schema.items()
        }
        qdrant_retriever.upsert_table_cards(table_cards)
        if self._golden_sql_records:
            qdrant_retriever.upsert_golden_sqls(self._golden_sql_records)

    # ============================================================
    # 第一阶段第 2 层 & 运行时自更新：SQLGlot AST 解析（四元组去重）
    # ============================================================

    def mine_join_edges_from_sql_history(
        self,
        sql_list: List[str],
        *,
        persist: bool = True,
        only_new: bool = False,
    ) -> int:
        with self._lock:
            schema = self._schema or postgres_client.fetch_schema_metadata()
            known_tables = set(schema.keys())
            live_cols = {
                t_name: {c["name"] for c in meta.get("columns", [])}
                for t_name, meta in schema.items()
            }
            changed_count = 0

            for sql in sql_list:
                if not sql or not sql.strip():
                    continue
                edges = self._extract_joins_from_single_sql(sql, known_tables)
                for t1, col1, t2, col2 in edges:
                    if (
                        t1 == t2
                        or t1 not in known_tables
                        or t2 not in known_tables
                        or col1 not in live_cols.get(t1, set())
                        or col2 not in live_cols.get(t2, set())
                    ):
                        continue
                    changed = self._upsert_edge_in_memory(
                        t1, col1, t2, col2,
                        source="sql_ast_mined",
                        verified_count=1,
                        inclusion_ratio=1.0,
                        increment_if_exists=not only_new,
                    )
                    if changed:
                        changed_count += 1

            if changed_count > 0 and persist:
                self._persist_lineage_to_yaml()
            return changed_count

    def learn_from_executed_sql(self, sql: str, question: str = "") -> int:
        self.load_schema()
        changed = self.mine_join_edges_from_sql_history([sql], persist=False, only_new=False)
        tables = self._extract_tables_from_sql(sql, set(self._schema.keys()))
        if tables and sql.strip():
            cleaned_sql = sql.strip()
            existing_sqls = {r.get("sql", "").strip() for r in self._golden_sql_records}
            if cleaned_sql not in existing_sqls:
                self._golden_sql_records.append({
                    "question": (question or "").strip(),
                    "sql": cleaned_sql,
                    "tables": tables,
                })
                qdrant_retriever.upsert_golden_sqls(self._golden_sql_records)
                changed = max(changed, 1)
        if changed > 0:
            self._persist_lineage_to_yaml()
        return changed

    @staticmethod
    def _extract_joins_from_single_sql(
        sql: str, known_tables: Set[str]
    ) -> List[Tuple[str, str, str, str]]:
        """提取单条 SQL 中的所有 JOIN ON 字段对，并按四元组对单条 SQL 内部先去重"""
        raw_results: List[Tuple[str, str, str, str]] = []

        try:
            import sqlglot
            from sqlglot import exp

            for parsed in sqlglot.parse(sql):
                if parsed is None:
                    continue
                alias_map: Dict[str, str] = {}
                for tbl in parsed.find_all(exp.Table):
                    real_name = tbl.name
                    if real_name in known_tables:
                        alias_map[real_name] = real_name
                        if tbl.alias:
                            alias_map[tbl.alias] = real_name

                for join_node in parsed.find_all(exp.Join):
                    on_expr = join_node.args.get("on")
                    if on_expr is None:
                        continue
                    eq_nodes = [on_expr] if isinstance(on_expr, exp.EQ) else list(on_expr.find_all(exp.EQ))
                    for eq in eq_nodes:
                        left, right = eq.left, eq.right
                        if isinstance(left, exp.Column) and isinstance(right, exp.Column):
                            t1 = alias_map.get(left.table, left.table)
                            t2 = alias_map.get(right.table, right.table)
                            if t1 in known_tables and t2 in known_tables and t1 != t2:
                                raw_results.append((t1, left.name, t2, right.name))
        except ImportError:
            pass
        except Exception:
            pass

        if not raw_results:
            alias_map = {}
            table_pattern = re.compile(
                r"\b(?:FROM|JOIN)\s+([a-zA-Z_][\w]*)(?:\s+(?:AS\s+)?([a-zA-Z_][\w]*))?",
                re.IGNORECASE,
            )
            reserved = {"ON", "WHERE", "LEFT", "RIGHT", "INNER", "OUTER", "GROUP", "ORDER", "LIMIT", "JOIN"}
            for m in table_pattern.finditer(sql):
                t_name = m.group(1)
                alias = m.group(2)
                if t_name in known_tables:
                    alias_map[t_name] = t_name
                    if alias and alias.upper() not in reserved:
                        alias_map[alias] = t_name

            eq_pattern = re.compile(
                r"\b([a-zA-Z_][\w]*)\.([a-zA-Z_][\w]*)\s*=\s*([a-zA-Z_][\w]*)\.([a-zA-Z_][\w]*)"
            )
            for m in eq_pattern.finditer(sql):
                a1, c1, a2, c2 = m.group(1), m.group(2), m.group(3), m.group(4)
                t1 = alias_map.get(a1, a1)
                t2 = alias_map.get(a2, a2)
                if t1 in known_tables and t2 in known_tables and t1 != t2:
                    raw_results.append((t1, c1, t2, c2))

        # 单条 SQL 内按规范化四元组去重
        deduped: List[Tuple[str, str, str, str]] = []
        seen: Set[Tuple[str, str, str, str]] = set()
        for t1, c1, t2, c2 in raw_results:
            key = SchemaMapEngine._canonical_4tuple(t1, c1, t2, c2)
            if key not in seen:
                seen.add(key)
                deduped.append(key)
        return deduped

    # ============================================================
    # 第二阶段：在线加权 PageRank 与最短 JOIN 路径生成
    # ============================================================

    def _build_adjacency(
        self, schema: Dict[str, Dict[str, Any]]
    ) -> Tuple[Dict[str, Set[str]], Dict[Tuple[str, str], str], Dict[Tuple[str, str], float]]:
        # 邻接表,给每张表发个空集合,集合里是关联的表名
        # adj = {
        #     "dim_customers": {"fact_orders"},
        #     "fact_orders":   {"dim_customers", "fact_order_items", "fact_returns", "fact_quality_claims"},
        #     "fact_returns":  {"fact_orders", "fact_order_items", "dim_products"},
        #     ...
        # }
        adj: Dict[str, Set[str]] = {t: set() for t in schema}
        # edge_join_conditions = {
        #      1. 单字段关联的两张表（列表里只有 1 个条件）：
        #     ("dim_customers", "fact_orders"): [
        #         "dim_customers.customer_id = fact_orders.customer_id"
        #     ],
        #
        #      2. 双字段复合关联的两张表（列表里有 2 个条件！）：
        #     ("fact_order_items", "fact_returns"): [
        #         "fact_order_items.order_id = fact_returns.order_id",
        #         "fact_order_items.product_id = fact_returns.product_id",
        #     ],
        # }
        edge_join_conditions: Dict[Tuple[str, str], List[str]] = {}
        # 权重:verified_count
        # edge_weights = {
        #     ("dim_customers", "fact_orders"):     4.08,  # 验证了 7 次的单字段边
        #     ("fact_orders", "fact_returns"):      3.39,  # 验证了 3 次的单字段边
        #     ("fact_order_items", "fact_returns"): 4.39,  # 2 个复合字段累加出来的边（权重最高！）
        # }
        edge_weights: Dict[Tuple[str, str], float] = {}
        # 这个项目是普通Graph
        # 之前Aider之所以用MultiGraph 是因为那里每一个文件是一个节点,但是真正连线的是内部的函数和类
        for (lt, lc, rt, rc), rel in self._relationships.items():
            if lt not in schema or rt not in schema or lt == rt:
                continue
            adj[lt].add(rt)
            adj[rt].add(lt)
            cond = f"{lt}.{lc} = {rt}.{rc}"
            freq_boost = 1.0 + math.log1p(int(rel.get("verified_count", 1)))
            # _relationships里的是单向,为什么这里必须要conds,因为去重的粒度不同
            # 如果两张表之间有 2 个复合关联字段（比如 fact_order_items 和 fact_returns 同时靠 order_id 和 product_id 关联），
            # _relationships 里是 2 条不同的记录，但它们的 pair 都是同一个 ("fact_order_items", "fact_returns")！
            # 就是为了加频率权重以及复合关联才这么设计的
            # 双向存 Key、收集复合 JOIN 条件、累加边权重
            for pair in ((lt, rt), (rt, lt)):
                # conds是edge_join_conditions复制
                conds = edge_join_conditions.setdefault(pair, [])
                if cond not in conds:
                    conds.append(cond)
                edge_weights[pair] = edge_weights.get(pair, 1.0) + freq_boost

        joined_conds = {pair: " AND ".join(conds) for pair, conds in edge_join_conditions.items()}
        # joined_conds = {
        #     ("dim_customers", "fact_orders"): "dim_customers.customer_id = fact_orders.customer_id",
        #     ("fact_orders", "dim_customers"): "dim_customers.customer_id = fact_orders.customer_id",
        # }会有重复的
        # 最终pagerank 节点也是双向边
        return adj, joined_conds, edge_weights

    def compute_table_rank(
        self,
        query: str = "",
        seed_tables: Optional[List[str]] = None,
        damping: float = 0.85,
        iterations: int = 30,
    ) -> List[Tuple[str, float]]:
        schema = self.load_schema()
        if not schema:
            return []

        adj, _, edge_weights = self._build_adjacency(schema)
        tables = list(schema.keys())

        personalization: Dict[str, float] = {}
        lowered_q = (query or "").lower()
        seed_set = set(seed_tables or [])

        # 纯硬编码加权还是太抽象了,得改
        # 1. 离线 LLM 自动补齐“表画像”,让llm写业务描述和同义词
        # 2. 向量相似度把每张表的“表名 + 字段 + 注释 + 同义词”拼成一张 Table Card 提前算好向量；
        # 在线收到 query 时算余弦相似度 cosine_sim(query, table_card)，直接拿这个相似度分数当 PageRank 的 personalization[t]
        # 3. 相似历史 Golden SQL 召回拿当前 query 去向量库搜历史上最像的 Top-3 黄金 SQL，看它们 FROM / JOIN 了哪几张表，直接给这几张表的 personalization 加超高权重
        # 在线只编码 1 次 query 向量，同时复用于 Qdrant Table-Card 检索与 Golden-SQL Top-3 检索
        q_vec = qdrant_retriever.embed_text(lowered_q) if lowered_q else None
        table_card_sims: Dict[str, float] = (
            qdrant_retriever.search_table_cards(lowered_q, query_vector=q_vec, top_k=len(tables))
            if q_vec is not None
            else {}
        )
        top_golden_sqls: List[Dict[str, Any]] = (
            qdrant_retriever.search_golden_sqls(lowered_q, query_vector=q_vec, top_k=3)
            if q_vec is not None
            else []
        )

        # 路 3：统计 Top-3 相似历史 Golden SQL 命中的表加权分
        sql_boost_map: Dict[str, float] = {t: 0.0 for t in tables}
        for sql_hit in top_golden_sqls:
            sim = float(sql_hit.get("score", 0.0))
            if sim > 0.18:
                for hit_tbl in sql_hit.get("tables", []) or []:
                    if hit_tbl in sql_boost_map:
                        sql_boost_map[hit_tbl] += sim * 9.0

        for t, meta in schema.items():
            weight = 1.0  # 基础底分（防 0 权重）

            # 路 1：显式表名/字段名精确命中 + 离线 LLM 表画像同义词命中
            if t in seed_set or (lowered_q and t.lower() in lowered_q):
                weight += 8.0

            prof = self._table_profiles.get(t, {}) or {}
            synonyms = prof.get("synonyms", []) or []
            for syn in synonyms:
                syn_str = str(syn).strip().lower()
                if syn_str and syn_str in lowered_q:
                    weight += 3.5

            for col in meta.get("columns", []):
                c_name = str(col.get("name", "")).lower()
                if c_name and len(c_name) > 2 and c_name in lowered_q:
                    weight += 4.0

            # 路 2：Qdrant Table-Card 余弦相似度（减基线后平方放大，拉开高相关表与边缘表差距）
            card_sim = float(table_card_sims.get(t, 0.0))
            vec_score = (max(0.0, card_sim - 0.10) ** 2) * 16.0

            # 路 3：Qdrant Top-3 历史 Golden SQL 召回表加成
            sql_boost = sql_boost_map.get(t, 0.0)

            personalization[t] = weight + vec_score + sql_boost

        total_p = sum(personalization.values()) or 1.0
        # 如果用networkx不需要手动归一化,这里是因为后面实现了没装networkx时的手写幂迭代兜底分支
        personalization = {k: v / total_p for k, v in personalization.items()}

        try:
            import networkx as nx
            g = nx.Graph()
            for t in tables:
                g.add_node(t)
            for t, neighbors in adj.items():
                for nb in neighbors:
                    w = edge_weights.get((t, nb), 1.0)
                    g.add_edge(t, nb, weight=w)
            scores = nx.pagerank(g, alpha=damping, personalization=personalization, weight="weight")
            return sorted(scores.items(), key=lambda x: x[1], reverse=True)
        except ImportError:
            pass

        scores = dict(personalization)
        for _ in range(iterations):
            new_scores = {}
            for t in tables:
                incoming = 0.0
                for nb in adj.get(t, ()):
                    out_weight_sum = sum(edge_weights.get((nb, k), 1.0) for k in adj.get(nb, ())) or 1.0
                    incoming += scores[nb] * (edge_weights.get((nb, t), 1.0) / out_weight_sum)
                new_scores[t] = (1.0 - damping) * personalization[t] + damping * incoming
            total_s = sum(new_scores.values()) or 1.0
            scores = {k: v / total_s for k, v in new_scores.items()}

        return sorted(scores.items(), key=lambda x: x[1], reverse=True)

    def resolve_connected_subgraph(self, target_tables: List[str]) -> Dict[str, Any]:
        """两步连通子图与主干/备选边划分器：
        第 1 步（迭代 BFS 连通补桥）：
          检查选中表集合在诱导子图中的连通分量；若存在孤岛，在全库图上跑多源 BFS 找最短路径，
          将路径上的中间桥表加入集合，反复迭代直到集合内任意两表连通（或确认全库不可达）。
        第 2 步（带权最大生成树 MST 分栏输出）：
          收集补桥后集合内的全部关联边（绝不砍边），按权重从高到低跑 Kruskal 生成树：
          - 构成无环生成树的 N-1 条边归入 `backbone_edges`（默认主干连法）；
          - 形成三角形/环路的其余边归入 `alternative_edges`（备选替换连法，带防环提示）。
        """
        schema = self.load_schema()
        adj, edge_conds, edge_weights = self._build_adjacency(schema)
        ordered_tables: List[str] = []
        for t in target_tables:
            if t in schema and t not in ordered_tables:
                ordered_tables.append(t)

        if len(ordered_tables) < 2:
            return {
                "final_tables": ordered_tables,
                "bridge_tables": [],
                "unreachable_tables": [],
                "backbone_edges": [],
                "alternative_edges": [],
            }

        selected_set: Set[str] = set(ordered_tables)
        bridge_tables: List[str] = []
        unreachable_tables: Set[str] = set()

        def _get_components(nodes: Set[str]) -> List[Set[str]]:
            active = nodes - unreachable_tables
            visited: Set[str] = set()
            comps: List[Set[str]] = []
            for n in ordered_tables:
                if n not in active or n in visited:
                    continue
                comp: Set[str] = set()
                q = deque([n])
                visited.add(n)
                while q:
                    curr = q.popleft()
                    comp.add(curr)
                    for nb in adj.get(curr, ()):
                        if nb in active and nb not in visited:
                            visited.add(nb)
                            q.append(nb)
                comps.append(comp)
            return comps

        def _shortest_bridge_between(comp_a: Set[str], targets: Set[str]) -> List[str]:
            """在全库大图上跑多源 BFS，找从 comp_a 到 targets 任意节点的最短路径"""
            queue = deque([[start] for start in comp_a])
            visited = set(comp_a)
            while queue:
                path = queue.popleft()
                curr = path[-1]
                if curr in targets:
                    return path
                for nb in adj.get(curr, ()):
                    if nb not in visited:
                        visited.add(nb)
                        queue.append(path + [nb])
            return []

        # ============ 第 1 步：迭代 BFS 连通补桥 ============
        while True:
            comps = _get_components(selected_set)
            if len(comps) <= 1:
                break
            main_comp = comps[0]
            other_nodes = set().union(*comps[1:])
            bridge_path = _shortest_bridge_between(main_comp, other_nodes)
            if not bridge_path:
                # 剩余孤岛在全库图里也完全不可达，标记为真·全库孤立表
                unreachable_tables.update(other_nodes)
                break
            for node in bridge_path[1:-1]:
                if node not in selected_set:
                    selected_set.add(node)
                    ordered_tables.append(node)
                    bridge_tables.append(node)

        # ============ 第 2 步：收集集合内所有边，按权重 MST 划分为「主干边」与「备选替换边」 ============
        active_tables = [t for t in ordered_tables if t not in unreachable_tables]
        active_set = set(active_tables)

        candidate_edges: List[Dict[str, Any]] = []
        seen_pairs: Set[Tuple[str, str]] = set()
        for u in active_tables:
            for v in sorted(adj.get(u, ())):
                if v not in active_set:
                    continue
                t_pair = (u, v) if u <= v else (v, u)
                if t_pair in seen_pairs:
                    continue
                seen_pairs.add(t_pair)
                weight = round(float(edge_weights.get(t_pair, 1.0)), 2)
                cond = edge_conds.get(t_pair, f"{t_pair[0]}.id = {t_pair[1]}.id")
                matching_rels = [
                    r for (lt, _, rt, _), r in self._relationships.items()
                    if (lt, rt) == t_pair
                ]
                cnt = max((int(r.get("verified_count", 1)) for r in matching_rels), default=1)
                sources = sorted({str(r.get("source", "physical_fk")) for r in matching_rels})
                tag = f"[来源: {'/'.join(sources)} | 验证: {cnt}次 | 权重: {weight}]"
                candidate_edges.append({
                    "u": t_pair[0],
                    "v": t_pair[1],
                    "weight": weight,
                    "condition": cond,
                    "formatted": f"`{t_pair[0]}` JOIN `{t_pair[1]}` ON `{cond}` {tag}",
                })

        # 按权重从高到低排序，用并查集 (Union-Find) 选出无环最大生成树主干，其余归入备选替换边
        candidate_edges.sort(key=lambda e: (-e["weight"], e["u"], e["v"]))
        parent = {t: t for t in active_tables}

        def _find(x: str) -> str:
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x

        backbone_edges: List[str] = []
        alternative_edges: List[str] = []

        for edge in candidate_edges:
            ru, rv = _find(edge["u"]), _find(edge["v"])
            if ru != rv:
                parent[ru] = rv
                backbone_edges.append(edge["formatted"])
            else:
                alternative_edges.append(edge["formatted"])

        return {
            "final_tables": ordered_tables,
            "bridge_tables": bridge_tables,
            "unreachable_tables": sorted(unreachable_tables),
            "backbone_edges": backbone_edges,
            "alternative_edges": alternative_edges,
        }

    def find_join_bridges(self, target_tables: List[str]) -> List[str]:
        """兼容接口：返回主干关联边 + 备选关联边列表"""
        sub = self.resolve_connected_subgraph(target_tables)
        return sub["backbone_edges"] + sub["alternative_edges"]

    SCHEMA_ERROR_KEYWORDS = (
        "no such column",
        "no such table",
        "does not exist",
        "undefined column",
        "unknown column",
        "invalid column",
        "binder error",
        "referenced column",
    )

    def handle_sql_schema_error(self, error_msg: str) -> bool:
        """异常驱动缓存击穿自愈：当 SQL 报错提示表或字段不存在时，立即强刷 Redis 缓存并重跑三步对账"""
        lowered = (error_msg or "").lower()
        if any(kw in lowered for kw in self.SCHEMA_ERROR_KEYWORDS):
            agent_logger.warning(
                f"Detected SQL schema mismatch error ({error_msg[:120]}), forcing Redis cache invalidation & lineage reconciliation"
            )
            self.load_schema(force_refresh=True)
            return True
        return False

    def build_schema_map_prompt(
        self,
        query: str,
        top_k: int = 5,
        token_budget_chars: int = 3200,
        force_refresh: bool = False,
    ) -> Dict[str, Any]:
        # Agent 调用 inspect_schema_map(query) 工具时，唯一调用的就是这个函数。

        schema = self.load_schema(force_refresh=force_refresh)# 第一阶段：加载表结构并跑三层漏斗建边
        ranked = self.compute_table_rank(query=query)# 第二阶段：跑 PageRank 选 Top-K 核心表
        score_map = dict(ranked)

        def _render_candidate_k(k: int) -> Tuple[str, Dict[str, Any]]:
            """完整渲染前 k 张表（含补桥与完整主干/备选 JOIN 路径），绝不半路截断字符串"""
            selected_k = [t for t, _ in ranked[:k]]
            sub = self.resolve_connected_subgraph(selected_k)
            f_tables = sub["final_tables"]
            b_set = set(sub["bridge_tables"])
            b_edges = sub["backbone_edges"]
            a_edges = sub["alternative_edges"]

            md_lines = [
                "## 🗺️ 数仓血缘骨架图 (Schema-Map via Two-Phase Table-Rank)",
                f"针对查询意图「{query or '全库概览'}」，选出核心表与自动补全桥表共 {len(f_tables)} 张：",
            ]
            for rank_idx, t_name in enumerate(f_tables, 1):
                meta = schema.get(t_name, {})
                score = score_map.get(t_name, 0.0)
                bridge_tag = " [自动补全桥表]" if t_name in b_set else ""
                cols = ", ".join(
                    f"{c['name']}({c['type']})" for c in meta.get("columns", [])
                )
                md_lines.append(
                    f"{rank_idx}. **{t_name}** [TableRank={score:.3f}]{bridge_tag} — {meta.get('description', '')}\n"
                    f"   - 主键: `{meta.get('primary_key', 'id')}` | 字段: {cols}"
                )

            if b_edges:
                md_lines.append("\n### 🌲 1. 默认主干连法（无环生成树，已连通上方所有表，常规查询直接选用）：")
                for edge_str in b_edges:
                    md_lines.append(f"- {edge_str}")

            if a_edges:
                md_lines.append(
                    "\n### 🔄 2. 备选替换连法（细粒度/复合键替代边 — 需按细粒度关联时用于【替换】上方对应主干，严禁与主干同时堆叠形成三角形环路！）："
                )
                for edge_str in a_edges:
                    md_lines.append(f"- {edge_str}")

            if sub["unreachable_tables"]:
                md_lines.append(
                    f"\n⚠️ 全库孤立表提示：{sub['unreachable_tables']} 与其他表无任何外键关联。"
                )

            return "\n".join(md_lines), sub

        # 二分查找：在 [1, min(len(ranked), top_k)] 范围内试探能完整塞进 token_budget_chars 的最大表数 k
        # 宁可少选 1~2 张边缘表，也绝不一刀切砍断最底部的 JOIN ON 连表路径！
        max_k = max(1, min(len(ranked), int(top_k or 5)))
        low, high = 1, max_k
        best_rendered = ""
        best_sub: Optional[Dict[str, Any]] = None

        while low <= high:
            mid = (low + high) // 2
            candidate_text, candidate_sub = _render_candidate_k(mid)
            if len(candidate_text) <= token_budget_chars:
                best_rendered = candidate_text
                best_sub = candidate_sub
                low = mid + 1   # 还能装得下，尝试扩大 k
            else:
                high = mid - 1  # 超出预算，缩小 k

        # 若极端情况下连 k=1 都超出预算，保底取 k=1 的完整结构（不截断 JOIN 路径）
        if best_sub is None:
            best_rendered, best_sub = _render_candidate_k(1)

        final_tables = best_sub["final_tables"]
        bridge_set = set(best_sub["bridge_tables"])
        backbone_edges = best_sub["backbone_edges"]
        alternative_edges = best_sub["alternative_edges"]

        return {
            # 完整表清单
            "ranked_tables": [
                {"table": t, "score": round(score_map.get(t, 0.0), 4), "is_bridge": t in bridge_set}
                for t in final_tables
            ],
            # 第 2 步算出来的默认主干 JOIN ON 边
            "backbone_edges": backbone_edges,
            # 会形成三角形/环路的备选替换 JOIN ON 边
            "alternative_edges": alternative_edges,
            "join_paths": backbone_edges + alternative_edges,
            # 全库血缘图里当前一共沉淀了多少条有效字段关系
            "persisted_relationships_count": len(self._relationships),
            # 经过二分查找控字数后，拼好的最终 Markdown 文本
            "schema_map_markdown": best_rendered,
        }


# 全局单例
schema_map_engine = SchemaMapEngine()
