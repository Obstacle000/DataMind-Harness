"""
Schema-Map 数仓血缘图谱与 Table-Rank 引擎（Aider RepoMap 同款图论算法迁移）

核心原理：
1. 节点 (Nodes) = 数仓中的所有表（如 fact_orders, dim_customers, fact_returns 等）
2. 边 (Edges) = 表之间的主外键约束与历史高频 JOIN 关系（如 fact_orders.customer_id = dim_customers.customer_id）
3. 个性化 PageRank (Personalized PageRank)：
   - 当用户提问（例如“华东客户退货率分析”）时，先根据关键词给命中的种子表赋予高初始游走偏置（Personalization Vector）
   - 在表关联图上运行 PageRank，算出每张表的拓扑中心度与当前问题相关度得分
4. 最短连接路径（Steiner Tree / Dijkstra Shortest Path）：
   - 自动求出所有命中表之间的最短 JOIN 桥梁（如 dim_customers -> fact_orders -> fact_returns）
   - 渲染出带显式 `JOIN ON` 契约的 Schema-Map 骨架，彻底消除大模型多表关联时的“假外键幻觉”
"""
from __future__ import annotations

from collections import deque
from typing import Any, Dict, List, Optional, Set, Tuple

from .postgres_client import postgres_client
from .redis_client import redis_client


class SchemaMapEngine:
    """基于 PageRank + 最短路径的数仓 Schema-Map 剪枝与关联寻路器"""

    def __init__(self):
        self._schema: Dict[str, Dict[str, Any]] = {}

    def load_schema(self, force_refresh: bool = False) -> Dict[str, Dict[str, Any]]:
        if not force_refresh:
            cached = redis_client.get_schema_graph_cache("warehouse_v1")
            if cached:
                self._schema = cached
                return self._schema
        self._schema = postgres_client.fetch_schema_metadata()
        redis_client.set_schema_graph_cache(self._schema, "warehouse_v1")
        return self._schema

    def _build_adjacency(
        self, schema: Dict[str, Dict[str, Any]]
    ) -> Tuple[Dict[str, Set[str]], Dict[Tuple[str, str], str]]:
        """构建无向邻接表与边上的 JOIN ON 条件字典"""
        adj: Dict[str, Set[str]] = {t: set() for t in schema}
        edge_join_conditions: Dict[Tuple[str, str], str] = {}

        for table_name, meta in schema.items():
            for fk in meta.get("foreign_keys", []):
                ref_table = fk["ref_table"]
                col = fk["column"]
                ref_col = fk["ref_column"]
                if ref_table in schema:
                    adj[table_name].add(ref_table)
                    adj[ref_table].add(table_name)
                    cond = f"{table_name}.{col} = {ref_table}.{ref_col}"
                    edge_join_conditions[(table_name, ref_table)] = cond
                    edge_join_conditions[(ref_table, table_name)] = cond

        return adj, edge_join_conditions

    def compute_table_rank(
        self,
        query: str = "",
        seed_tables: Optional[List[str]] = None,
        damping: float = 0.85,
        iterations: int = 30,
    ) -> List[Tuple[str, float]]:
        """计算个性化 PageRank (Table-Rank)，优先使用 NetworkX，无依赖时使用纯 Python 幂迭代"""
        schema = self.load_schema()
        if not schema:
            return []

        adj, _ = self._build_adjacency(schema)
        tables = list(schema.keys())
        n = len(tables)

        # 构建个性化偏置向量 (Personalization Vector)
        personalization: Dict[str, float] = {}
        lowered_q = (query or "").lower()
        seed_set = set(seed_tables or [])

        for t, meta in schema.items():
            weight = 1.0
            if t in seed_set or t.lower() in lowered_q:
                weight += 8.0
            desc = str(meta.get("description", "")).lower()
            for token in ("客户", "供应商", "订单", "退货", "库存", "商品", "零件", "毛利", "运费", "区域", "城市"):
                if token in lowered_q and token in desc:
                    weight += 4.0
            for col in meta.get("columns", []):
                c_name = str(col.get("name", "")).lower()
                c_comm = str(col.get("comment", "")).lower()
                if (c_name and c_name in lowered_q) or any(
                    k in lowered_q and k in c_comm for k in ("退货", "毛利", "成本", "城市", "等级", "库存", "交期")
                ):
                    weight += 3.0
            personalization[t] = weight

        total_p = sum(personalization.values()) or 1.0
        personalization = {k: v / total_p for k, v in personalization.items()}

        # 优先使用 NetworkX PageRank
        try:
            import networkx as nx
            g = nx.Graph()
            for t in tables:
                g.add_node(t)
            for t, neighbors in adj.items():
                for nb in neighbors:
                    g.add_edge(t, nb)
            scores = nx.pagerank(g, alpha=damping, personalization=personalization)
            return sorted(scores.items(), key=lambda x: x[1], reverse=True)
        except ImportError:
            pass

        # 纯 Python 幂迭代 Personalized PageRank 兜底
        scores = dict(personalization)
        for _ in range(iterations):
            new_scores = {}
            for t in tables:
                incoming = 0.0
                for nb in adj.get(t, ()):
                    deg = len(adj.get(nb, ())) or 1
                    incoming += scores[nb] / deg
                new_scores[t] = (1.0 - damping) * personalization[t] + damping * incoming
            total_s = sum(new_scores.values()) or 1.0
            scores = {k: v / total_s for k, v in new_scores.items()}

        return sorted(scores.items(), key=lambda x: x[1], reverse=True)

    def find_join_bridges(self, target_tables: List[str]) -> List[str]:
        """利用 BFS 最短路径在表关联图上找出连接所有目标表的中间桥梁与显式 JOIN ON 语句"""
        schema = self.load_schema()
        adj, edge_conds = self._build_adjacency(schema)
        valid_targets = [t for t in target_tables if t in schema]
        if len(valid_targets) < 2:
            return []

        join_clauses: List[str] = []
        seen_edges: Set[Tuple[str, str]] = set()

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
                edge_key = tuple(sorted((u, v)))
                if edge_key not in seen_edges:
                    seen_edges.add(edge_key)
                    cond = edge_conds.get((u, v), f"{u}.id = {v}.id")
                    join_clauses.append(f"`{u}` JOIN `{v}` ON `{cond}`")

        return join_clauses

    def build_schema_map_prompt(
        self,
        query: str,
        top_k: int = 5,
        token_budget_chars: int = 3200,
    ) -> Dict[str, Any]:
        """生成控制了 Token 预算的 Schema-Map 骨架提示词与推荐 JOIN 路径"""
        schema = self.load_schema()
        ranked = self.compute_table_rank(query=query)
        selected = [t for t, _ in ranked[:top_k]]
        join_bridges = self.find_join_bridges(selected)

        lines = [
            "## 🗺️ 数仓血缘骨架图 (Schema-Map via Table-Rank)",
            f"针对查询意图「{query or '全库概览'}」，PageRank 中心度排序选出的 Top-{len(selected)} 核心表：",
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
            "schema_map_markdown": rendered,
        }


# 全局单例
schema_map_engine = SchemaMapEngine()
