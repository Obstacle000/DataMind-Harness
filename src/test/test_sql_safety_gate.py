"""SQL 安全审计（AST / 词法防注入）与 EXPLAIN 成本门禁单元测试"""
from __future__ import annotations

import pytest


@pytest.mark.security
@pytest.mark.unit
class TestSqlSafetyGate:
    @pytest.mark.parametrize(
        "sql, should_block",
        [
            ("SELECT customer_id, SUM(gross_amount) FROM fact_orders GROUP BY customer_id", False),
            ("WITH t AS (SELECT * FROM dim_customers) SELECT * FROM t LIMIT 10", False),
            ("DROP TABLE fact_orders", True),
            ("DELETE FROM fact_orders WHERE order_id = 1001", True),
            ("UPDATE dim_customers SET vip_tier = 'S'", True),
            ("SELECT 1; DROP TABLE fact_orders;", True),   # 分号堆叠注入
            ("   ", True),                                 # 空 SQL
        ],
    )
    def test_ast_and_keyword_audit(self, sql_gate, sql: str, should_block: bool):
        """合法 SELECT 放行，一切写操作与堆叠注入必须拦截"""
        violation = sql_gate._inspect_sql_safety(sql)
        if should_block:
            assert violation is not None, f"危险 SQL 未被拦截: {sql}"
        else:
            assert violation is None, f"合法 SQL 被误杀: {sql} -> {violation}"

    def test_cost_gate_blocks_full_table_scan(
        self, sql_gate, make_sql_tool_request, mock_pg_slow_query
    ):
        """通过 conftest 注入 mock_pg_slow_query，不连真实库验证 900 万行全表扫被拦"""
        req = make_sql_tool_request("SELECT * FROM fact_orders")
        blocked_msg = sql_gate._check_request(req)

        assert blocked_msg is not None
        assert blocked_msg.status == "error"
        assert "成本门禁阻断" in blocked_msg.content
        assert "9000000" in blocked_msg.content

    def test_non_sql_tool_is_bypassed(self, sql_gate, make_sql_tool_request):
        """非只读 SQL 工具直接放行，不触发拦截"""
        req = make_sql_tool_request("DROP TABLE x", tool_name="inspect_schema_map")
        assert sql_gate._check_request(req) is None
