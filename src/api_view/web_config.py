"""
Web 层数据库与基础设施连接配置
包含：
1. MongoDB (Motor AsyncIOMotorClient) —— 持久化会话、display_messages、harness_traces
2. PostgreSQL / Redis / DuckDB 客户端快捷访问入口
"""
from motor.motor_asyncio import AsyncIOMotorClient
from ..agent.env_utils import get_env
from ..agent.config import MONGODB_URI, MONGODB_DB_NAME

# 全局 Motor 客户端
_client: AsyncIOMotorClient | None = None


def get_mongo_client() -> AsyncIOMotorClient:
    """获取 MongoDB 异步客户端"""
    global _client
    if _client is None:
        _client = AsyncIOMotorClient(MONGODB_URI)
    return _client


def get_db():
    """获取 MongoDB 数据库实例"""
    return get_mongo_client()[MONGODB_DB_NAME]


async def close_mongo_client():
    """关闭 MongoDB 连接"""
    global _client
    if _client:
        _client.close()
        _client = None
