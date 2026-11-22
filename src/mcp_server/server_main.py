"""
DataMind-Harness MCP Server 入口
FastMCP 实例创建，注册外部数据连接器 tools，SSE 传输协议
"""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from fastmcp import FastMCP
from src.mcp_server.server_config import MCP_HOST, MCP_PORT
from src.mcp_server.tools.example_tools import register_datamind_mcp_tools

mcp = FastMCP(
    name="DataMind-MCP",
    instructions="DataMind-Harness 外部数据网关与行业基准指标 MCP 服务。",
)

register_datamind_mcp_tools(mcp)

if __name__ == "__main__":
    print(f"🚀 Starting DataMind-MCP Server on {MCP_HOST}:{MCP_PORT} (SSE transport)")
    mcp.run(transport="sse", host=MCP_HOST, port=MCP_PORT)
