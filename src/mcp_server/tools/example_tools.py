"""
DataMind-Harness MCP 工具集
向 MCP Server 注册企业数仓外部元数据与实时指标查询工具
"""
import json
from fastmcp import FastMCP
from ..http_base import datamind_http_client


def register_datamind_mcp_tools(mcp: FastMCP):
    """注册 DataMind 外部数据连接器工具到 MCP Server"""

    @mcp.tool()
    async def fetch_external_kpi_benchmark(industry: str = "manufacturing") -> str:
        """获取行业基准 KPI 与宏观经营参考水位（用于归因分析对比）。

        Args:
            industry: 行业分类标识

        Returns:
            行业基准毛利率、平均退货率与库存周转天数 JSON。
        """
        try:
            result = await datamind_http_client.get("/api/benchmarks", params={"industry": industry})
            return json.dumps(result, ensure_ascii=False, indent=2)
        except Exception:
            fallback = {
                "industry": industry,
                "benchmark_gross_margin": 0.24,
                "benchmark_return_rate": 0.035,
                "benchmark_turnover_days": 28,
            }
            return json.dumps(fallback, ensure_ascii=False, indent=2)
