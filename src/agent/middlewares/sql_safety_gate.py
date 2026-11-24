"""
中间件: SQL 语法安全审计与 EXPLAIN 成本门禁 (SqlSafetyGateMiddleware)

核心职责（DataMind-Harness 特有防线）：
1. AST 语法树静态审计（优先使用 SQLGlot，无依赖时使用词法边界正则）：
   严厉拦截只读工具中的 DROP / TRUNCATE / DELETE / ALTER / UPDATE / INSERT 等越权写语句。
2. EXPLAIN 扫描行数成本门禁：
   在真实查询打到 PostgreSQL 数仓前，预估 Plan Rows；若超过阈值（如 500 万行全表扫描），
   直接返回降级 ToolMessage，强制模型补充 WHERE 时间分区或索引过滤条件。
"""
from __future__ import annotations

import re
from typing import Any, Awaitable, Callable

from langchain.agents.middleware import AgentMiddleware, ToolCallRequest
from langchain_core.messages import ToolMessage
from langgraph.types import Command

from ..clients.postgres_client import postgres_client
from ..log_utils import middleware_logger


class SqlSafetyGateMiddleware(AgentMiddleware):
    """拦截只读 SQL 工具调用，执行 AST 危险语句阻断与 EXPLAIN 成本门禁"""

    READONLY_SQL_TOOLS = {"execute_readonly_sql", "query_duckdb_olap"}
    FORBIDDEN_WRITE_KEYWORDS = (
        "DROP", "TRUNCATE", "DELETE", "UPDATE", "INSERT",
        "ALTER", "GRANT", "REVOKE", "CREATE", "REPLACE",
    )

    def __init__(self, max_scan_rows: int = 5_000_000):
        self._max_scan_rows = max_scan_rows
        self.tools = []

    @property
    def name(self) -> str:
        return "SqlSafetyGateMiddleware"

    def _inspect_sql_safety(self, sql: str) -> str | None:
        """返回违规原因字符串；返回 None 表示安全通过"""
        cleaned = (sql or "").strip()
        if not cleaned:
            return "SQL 语句不能为空。"

        # 1. 优先尝试 SQLGlot AST 解析审计
        try:
            import sqlglot
            from sqlglot import exp
            parsed_list = sqlglot.parse(cleaned)
            if len(parsed_list) > 1:
                return "安全门禁拦截：禁止在单次只读调用中拼接多条 SQL 语句（防堆叠注入）。"
            for expr in parsed_list:
                if expr is not None and not isinstance(expr, (exp.Select, exp.Union, exp.With)):
                    return f"安全门禁拦截：只读分析工具仅允许 SELECT 查询，检测到越权语句类型 {type(expr).__name__}。"
        except ImportError:
            pass
        except Exception:
            # SQLGlot 解析异常时继续走下方词法边界检查
            pass

        # 2. 词法边界与多语句检查
        statements = [s.strip() for s in cleaned.split(";") if s.strip()]
        if len(statements) > 1:
            return "安全门禁拦截：禁止在单次查询中使用分号拼接多条 SQL 语句。"

        upper_sql = cleaned.upper()
        for kw in self.FORBIDDEN_WRITE_KEYWORDS:
            if re.search(rf"\b{kw}\b", upper_sql):
                return (
                    f"安全门禁拦截：只读取数工具严禁包含 `{kw}` 操作！"
                    "若需写回业务库，必须调用受人工审批保护的 `execute_writeback_sql` 工具。"
                )
        return None

    def _check_request(self, request: ToolCallRequest) -> ToolMessage | None:
        # 主要是现在只读查询工具,只是起个名字,他的形参本身是一个sql语句,模型完全可以偷偷调用写入操作
        # 这里是在调用这两个工具之前,看一下模型的toolcall参数是不是不老实
        tool_name = request.tool_call.get("name", "")
        if tool_name not in self.READONLY_SQL_TOOLS:
            return None

        args = request.tool_call.get("args", {}) or {}
        sql = str(args.get("sql", ""))

        violation = self._inspect_sql_safety(sql)
        if violation:
            middleware_logger.warning(f"SQL safety gate blocked '{tool_name}': {violation}")
            return ToolMessage(
                content=f"[SQL 安全门禁阻断] {violation}",
                tool_call_id=request.tool_call.get("id", ""),
                status="error",
            )

        # 对 PostgreSQL 查询执行 EXPLAIN 扫描成本预检
        if tool_name == "execute_readonly_sql":
            cost_info = postgres_client.explain_query_cost(sql)
            if cost_info.get("exceeds_limit"):
                est_rows = cost_info.get("estimated_rows")
                middleware_logger.warning(
                    f"SQL cost gate blocked slow query (estimated_rows={est_rows})"
                )
                return ToolMessage(
                    content=(
                        f"[SQL 成本门禁阻断] EXPLAIN 预估扫描行数高达 {est_rows} 行，"
                        f"超出安全阈值 ({self._max_scan_rows} 行)。"
                        "请补充 WHERE 时间范围过滤、分区键或 LIMIT 条件后重试。"
                    ),
                    tool_call_id=request.tool_call.get("id", ""),
                    status="error",
                )
        return None

    def wrap_tool_call(
        self,
        request: ToolCallRequest,
        handler: Callable[[ToolCallRequest], ToolMessage | Command[Any]],
    ) -> ToolMessage | Command[Any]:
        blocked = self._check_request(request)
        if blocked is not None:
            return blocked
        return handler(request)

    async def awrap_tool_call(
        self,
        request: ToolCallRequest,
        handler: Callable[[ToolCallRequest], Awaitable[ToolMessage | Command[Any]]],
    ) -> ToolMessage | Command[Any]:
        blocked = self._check_request(request)
        if blocked is not None:
            return blocked
        return await handler(request)
