"""Schema-Map 血缘图、子图补桥与二分控预算单元测试"""
from __future__ import annotations

import pytest
from src.agent.clients.schema_map import SchemaMapEngine, schema_map_engine


@pytest.mark.schema
@pytest.mark.unit
class TestSchemaMapEngine:
    def test_canonical_4tuple_is_order_independent(self):
        """正反顺序的四元组必须归一化为完全一致的 Key"""
        forward = SchemaMapEngine._canonical_4tuple(
            "fact_orders", "customer_id", "dim_customers", "customer_id"
        )
        backward = SchemaMapEngine._canonical_4tuple(
            "dim_customers", "customer_id", "fact_orders", "customer_id"
        )
        assert forward == backward
        assert forward == ("dim_customers", "customer_id", "fact_orders", "customer_id")

    def test_binary_search_keeps_join_paths_intact(self):
        """二分查找控预算时，宁可少选表也绝不截断底部的 JOIN ON 路径"""
        result = schema_map_engine.build_schema_map_prompt(
            query="查询华东区客户的订单与退货金额",
            top_k=5,
            token_budget_chars=1800,
        )
        md = result["schema_map_markdown"]
        assert len(result["ranked_tables"]) >= 1
        assert "数仓血缘骨架图" in md
        for edge in result["backbone_edges"]:
            assert edge in md

    def test_resolve_connected_subgraph_bridges_disconnected_tables(self):
        """只传两个无直接外键的表时，多源 BFS 必须自动拉入中间桥表连通"""
        schema_map_engine.load_schema()
        sub = schema_map_engine.resolve_connected_subgraph(
            ["dim_customers", "fact_returns"]
        )
        # 客户表与退货表没有直接外键，必须通过 fact_orders 桥接
        assert "fact_orders" in sub["final_tables"]
        assert "fact_orders" in sub["bridge_tables"]
        assert len(sub["backbone_edges"]) >= 2
