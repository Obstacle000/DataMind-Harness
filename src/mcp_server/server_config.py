"""
DataMind-Harness MCP Server 配置模块
外部数据源服务地址 + MCP 监听配置
"""
import os
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', '.env'))

# 外部数据平台 / 元数据中心 HTTP 地址
DATAMIND_API_BASE_URL = os.getenv("DATAMIND_API_BASE_URL", "http://localhost:8081")

# MCP Server 监听配置
MCP_HOST = os.getenv("MCP_HOST", "0.0.0.0")
MCP_PORT = int(os.getenv("MCP_PORT", "9000"))

# HTTP 客户端配置
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "30"))
HTTP_MAX_CONNECTIONS = int(os.getenv("HTTP_MAX_CONNECTIONS", "20"))
