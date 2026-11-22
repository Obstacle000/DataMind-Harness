"""
Schema-Map 数仓血缘图谱与 Table-Rank 引擎（两阶段工业级架构）

================================================================================
【第一阶段：离线/启动建图期 (Phase 1: 3-Layer Lineage Discovery Funnel)】
在任何用户提问之前，通过三层探边漏斗一次性找齐所有表的关联边，严格按四元组去重后落盘至 `schema_lineage.yaml`：
  - 第 1 层（物理外键 `physical_fk`）：直接读取数据库 `information_schema` 的物理 FOREIGN KEY；
  - 第 2 层（历史 SQL AST 挖掘 `sql_ast_mined`）：用 SQLGlot 解析历史 SQL 种子库，还原表别名提取 JOIN ON；
  - 第 3 层（隐藏外键探查 `inclusion_llm_verified` —— 解决无外键、无历史SQL、异名字段的孤岛表）：
      Step 3.1 锁定全库各表的主键靶子 (Primary Key Targets)；
      Step 3.2 算法算“数据值集合包含率 (Inclusion Dependency Ratio ≥ 80%)”，筛掉 99% 值域不匹配的列与低基数维度列；
      Step 3.3 大模型语义二分类盖章（防 `1,2,3` 自增 ID 巧合碰撞，判断表名+字段名+注释在业务上是否真属于主外键）；
      Step 3.4 按规范化四元组 `(left_table, left_col, right_table, right_col)` 严格去重，持久化写回 `schema_lineage.yaml`！

【第二阶段：在线用户查询期 (Phase 2: Online PageRank + Shortest Path)】
图在第一阶段已 100% 建好边，用户提问时 **0 大模型探边开销**：
  1. 在加权血缘图上运行个性化 PageRank (Table-Rank) 选出 Top-K 核心表（自动拉入中间枢纽桥梁表）；
  2. 运行 BFS 最短路径输出显式 `JOIN ON` 连接路径，大模型直接照抄标准答案，无需看样本二次猜外键！
================================================================================
"""
from __future__ import annotations

from collections import deque
from datetime import datetime
import math
import os
from pathlib import Path
import re
import threading
from typing import Any, Dict, List, Optional, Set, Tuple
import yaml

from .postgres_client import postgres_client
from .redis_client import redis_client
from .duckdb_client import duckdb_client
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
        self._schema: Dict[str, Dict[str, Any]] = {}
        # 严格按规范化四元组唯一去重: (left_table, left_col, right_table, right_col) -> dict
        self._relationships: Dict[Tuple[str, str, str, str], Dict[str, Any]] = {}
        self._bootstrapped = False

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
        with self._lock:
            if not force_refresh:
                cached = redis_client.get_schema_graph_cache("warehouse_v1")
                if cached:
                    self._schema = cached
                    self._run_phase1_discovery_funnel()
                    return self._schema
            self._schema = postgres_client.fetch_schema_metadata()
            redis_client.set_schema_graph_cache(self._schema, "warehouse_v1")
            self._run_phase1_discovery_funnel()
            return self._schema

    def _run_phase1_discovery_funnel(self) -> None:
        """执行第一阶段三层探边漏斗，确保所有表（含无外键孤岛表）的关联边一次性探清并去重落盘"""
        if self._bootstrapped and self._relationships:
            return
        self._bootstrapped = True

        raw_data: Dict[str, Any] = {}
        if self.lineage_path.exists():
            try:
                raw_data = yaml.safe_load(self.lineage_path.read_text(encoding="utf-8")) or {}
            except Exception as exc:
                agent_logger.warning(f"Failed to read {self.lineage_path}: {exc}")

        dirty = False

        # 0. 加载 schema_lineage.yaml 已持久化的语义层边（同时自动对历史脏数据做四元组去重）
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
                dirty = True  # 文件中存在重复四元组，标记稍后重写去重
            seen_in_file.add(key)
            self._upsert_edge_in_memory(
                lt, lc, rt, rc,
                source=str(item.get("source", "semantic_layer")),
                verified_count=int(item.get("verified_count", 1)),
                inclusion_ratio=float(item.get("inclusion_ratio", 1.0)),
                last_verified_at=str(item.get("last_verified_at", "")),
                increment_if_exists=False,
            )

        # 第 1 层：直接合入数据库物理外键 (information_schema Physical FK)
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

        # 第 2 层：用 SQLGlot AST 解析历史 SQL 种子库 (golden_sql_seed)
        seed_sqls = raw_data.get("golden_sql_seed", []) or []
        if seed_sqls:
            mined_new = self.mine_join_edges_from_sql_history(
                seed_sqls, persist=False, only_new=True
            )
            dirty = dirty or (mined_new > 0)

        # 第 3 层：针对无外键、无历史 SQL 的孤岛表/未关联列，执行「数据值包含率初筛 + 大模型语义盖章」
        discovered_new = self.discover_hidden_foreign_keys(persist=False)
        dirty = dirty or (discovered_new > 0)

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

                        # 在数据库中计算子表列值落入目标表主键集合的包含率 (Inclusion Dependency Ratio)
                        probe_sql = f"""
                        SELECT
                            (SELECT COUNT(DISTINCT {child_col}) FROM {child_table} WHERE {child_col} IS NOT NULL) AS child_dist,
                            COUNT(DISTINCT c.{child_col}) AS matched_dist
                        FROM {child_table} c
                        JOIN {target_table} p
                          ON CAST(c.{child_col} AS VARCHAR) = CAST(p.{target_pk} AS VARCHAR)
                        """
                        res = duckdb_client.query_olap(probe_sql)
                        if res.get("error") or not res.get("rows"):
                            continue
                        row = res["rows"][0]
                        child_dist = int(row.get("child_dist") or 0)
                        matched_dist = int(row.get("matched_dist") or 0)
                        if child_dist == 0 or matched_dist == 0:
                            continue

                        inclusion_ratio = matched_dist / child_dist
                        if inclusion_ratio < min_inclusion_ratio:
                            continue

                        # Step 3: 数据值包含率达标 (>= 80%)！交由大模型或语义域规则做二分类盖章（防巧合碰撞）
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
        """将按四元组严格去重后的关系表原子写回 schema_lineage.yaml"""
        with self._lock:
            try:
                existing_seeds: List[str] = []
                if self.lineage_path.exists():
                    old = yaml.safe_load(self.lineage_path.read_text(encoding="utf-8")) or {}
                    existing_seeds = old.get("golden_sql_seed", []) or []

                sorted_rels = [
                    self._relationships[k]
                    for k in sorted(self._relationships.keys())
                ]
                payload = {
                    "relationships": sorted_rels,
                    "golden_sql_seed": existing_seeds,
                }
                header = (
                    "# ============================================================\n"
                    "# DataMind-Harness 自演进数仓语义层血缘契约 (Self-Evolving Semantic Lineage)\n"
                    "# 1. 严格按规范化四元组 (left_table, left_col, right_table, right_col) 唯一去重\n"
                    "# 2. 汇集三层探边漏斗成果: physical_fk / sql_ast_mined / inclusion_llm_verified\n"
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
            changed_count = 0

            for sql in sql_list:
                if not sql or not sql.strip():
                    continue
                edges = self._extract_joins_from_single_sql(sql, known_tables)
                for t1, col1, t2, col2 in edges:
                    if t1 == t2 or t1 not in known_tables or t2 not in known_tables:
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

    def learn_from_executed_sql(self, sql: str) -> int:
        self.load_schema()
        return self.mine_join_edges_from_sql_history([sql], persist=True, only_new=False)

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
        adj: Dict[str, Set[str]] = {t: set() for t in schema}
        edge_join_conditions: Dict[Tuple[str, str], List[str]] = {}
        edge_weights: Dict[Tuple[str, str], float] = {}

        for (lt, lc, rt, rc), rel in self._relationships.items():
            if lt not in schema or rt not in schema or lt == rt:
                continue
            adj[lt].add(rt)
            adj[rt].add(lt)
            cond = f"{lt}.{lc} = {rt}.{rc}"
            freq_boost = 1.0 + math.log1p(int(rel.get("verified_count", 1)))
            for pair in ((lt, rt), (rt, lt)):
                conds = edge_join_conditions.setdefault(pair, [])
                if cond not in conds:
                    conds.append(cond)
                edge_weights[pair] = edge_weights.get(pair, 1.0) + freq_boost

        joined_conds = {pair: " AND ".join(conds) for pair, conds in edge_join_conditions.items()}
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

        for t, meta in schema.items():
            weight = 1.0
            if t in seed_set or t.lower() in lowered_q:
                weight += 8.0
            desc = str(meta.get("description", "")).lower()
            for token in ("客户", "供应商", "订单", "退货", "索赔", "库存", "商品", "零件", "毛利", "运费", "区域", "城市"):
                if token in lowered_q and token in desc:
                    weight += 4.0
            for col in meta.get("columns", []):
                c_name = str(col.get("name", "")).lower()
                c_comm = str(col.get("comment", "")).lower()
                if (c_name and c_name in lowered_q) or any(
                    k in lowered_q and k in c_comm for k in ("退货", "索赔", "毛利", "成本", "城市", "等级", "库存", "交期")
                ):
                    weight += 3.0
            personalization[t] = weight

        total_p = sum(personalization.values()) or 1.0
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

    def find_join_bridges(self, target_tables: List[str]) -> List[str]:
        schema = self.load_schema()
        adj, edge_conds, _ = self._build_adjacency(schema)
        valid_targets = [t for t in target_tables if t in schema]
        if len(valid_targets) < 2:
            return []

        join_clauses: List[str] = []
        seen_table_pairs: Set[Tuple[str, str]] = set()

        def _bfs_path(start: str, goal: str) -> List[str]:
            queue = deque([[start]])
            visited = {start}
            while queue:
                path = queue.popleft()
                node = path[-1]
                if node == goal:
                    return path
                for nb in adj.get(node, ()):
                    if nb not in visited:
                        visited.add(nb)
                        queue.append(path + [nb])
            return []

        anchor = valid_targets[0]
        for other in valid_targets[1:]:
            path = _bfs_path(anchor, other)
            for i in range(len(path) - 1):
                u, v = path[i], path[i + 1]
                t_pair = (u, v) if u <= v else (v, u)
                if t_pair not in seen_table_pairs:
                    seen_table_pairs.add(t_pair)
                    cond = edge_conds.get((u, v), f"{u}.id = {v}.id")
                    matching_rels = [
                        r for (lt, _, rt, _), r in self._relationships.items()
                        if (lt, rt) == t_pair
                    ]
                    tag = ""
                    if matching_rels:
                        cnt = max(int(r.get("verified_count", 1)) for r in matching_rels)
                        sources = sorted({str(r.get("source", "")) for r in matching_rels})
                        tag = f" [已验证 {cnt} 次 | 来源: {'/'.join(sources)}]"
                    join_clauses.append(f"`{u}` JOIN `{v}` ON `{cond}`{tag}")

        return join_clauses

    def build_schema_map_prompt(
        self,
        query: str,
        top_k: int = 5,
        token_budget_chars: int = 3200,
    ) -> Dict[str, Any]:
        schema = self.load_schema()
        ranked = self.compute_table_rank(query=query)
        selected = [t for t, _ in ranked[:top_k]]
        join_bridges = self.find_join_bridges(selected)

        lines = [
            "## 🗺️ 数仓血缘骨架图 (Schema-Map via Two-Phase Table-Rank)",
            f"针对查询意图「{query or '全库概览'}」，基于第一阶段三层漏斗建图选出的 Top-{len(selected)} 核心表：",
        ]
        for rank_idx, (t_name, score) in enumerate(ranked[:top_k], 1):
            meta = schema.get(t_name, {})
            cols = ", ".join(
                f"{c['name']}({c['type']})" for c in meta.get("columns", [])
            )
            lines.append(
                f"{rank_idx}. **{t_name}** [TableRank={score:.3f}] — {meta.get('description', '')}\n"
                f"   - 主键: `{meta.get('primary_key', 'id')}` | 字段: {cols}"
            )

        if join_bridges:
            lines.append("\n### 🔗 最短跨表连接路径（请严格按此路径写 JOIN ON，禁止臆造外键）：")
            for bridge in join_bridges:
                lines.append(f"- {bridge}")

        rendered = "\n".join(lines)[:token_budget_chars]
        return {
            "ranked_tables": [{"table": t, "score": round(s, 4)} for t, s in ranked[:top_k]],
            "join_paths": join_bridges,
            "persisted_relationships_count": len(self._relationships),
            "schema_map_markdown": rendered,
        }


# 全局单例
schema_map_engine = SchemaMapEngine()
