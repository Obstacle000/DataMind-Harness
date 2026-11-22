"""
 MCP 工具
"""
import json
from fastmcp import FastMCP
from ..http_base import erp_client


def example_tools(mcp: FastMCP):
    """注册库存管理工具到 MCP Server"""

    @mcp.tool()
    async def example_fn() -> str:
        """获取库存预警列表，返回当前库存量低于安全库存的所有零部件。

        Returns:
            库存预警列表JSON，每条包含库存信息和零部件详情（名称、编码、分类等）
        """
        result = await erp_client.get("/api/...")
        return json.dumps(result, ensure_ascii=False, indent=2)

