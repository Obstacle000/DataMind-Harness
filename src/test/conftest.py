"""
pytest 全局共享夹具（Fixtures）

pytest 会自动发现同目录及子目录下的 conftest.py，
所有测试文件无需手动 import，直接在测试函数参数里声明 fixture 名称即可注入使用。
"""
from __future__ import annotations

from types import SimpleNamespace
import pytest

from src.agent.middlewares.sql_safety_gate import SqlSafetyGateMiddleware


@pytest.fixture
def sql_gate() -> SqlSafetyGateMiddleware:
    """提供一个默认 500 万行阈值的 SQL 安全门禁实例"""
    return SqlSafetyGateMiddleware(max_scan_rows=5_000_000)


@pytest.fixture
def make_sql_tool_request():
    """工厂 Fixture：快速构造模拟的 ToolCallRequest 对象"""
    def _factory(
        sql: str,
        tool_name: str = "execute_readonly_sql",
        call_id: str = "call_test_001",
    ):
        return SimpleNamespace(
            tool_call={
                "id": call_id,
                "name": tool_name,
                "args": {"sql": sql},
            }
        )
    return _factory


@pytest.fixture
def mock_pg_slow_query(monkeypatch):
    """Mock Fixture：伪造 PostgreSQL EXPLAIN 返回 900 万行全表扫描"""
    from src.agent.clients import postgres_client as pg_mod

    monkeypatch.setattr(
        pg_mod.postgres_client,
        "explain_query_cost",
        lambda sql: {"estimated_rows": 9_000_000, "exceeds_limit": True},
    )


@pytest.fixture
def isolated_agent_loader():
    """提供一个干净、隔离的 AgentLoader 实例（避免污染全局单例）"""
    from src.api_view.agent_loader import AgentLoader
    return AgentLoader()
