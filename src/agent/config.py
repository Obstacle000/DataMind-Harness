"""
DataMind-Harness 全局配置模块
LLM、MongoDB (Store/Checkpointer)、PostgreSQL (业务数仓)、Redis (缓存与异步队列)、DuckDB (OLAP) 与 Docker 沙箱参数
"""
import os
from langchain_deepseek import ChatDeepSeek
from .env_utils import get_env, get_env_int

# ============ LLM 配置 ============
LLM_MODEL = get_env("LLM_MODEL", "deepseek-chat")
LLM_BASE_URL = get_env("LLM_BASE_URL", "https://api.deepseek.com")
LLM_API_KEY = get_env("DEEPSEEK_API_KEY", "")
LLM_TEMPERATURE = 0.1
LLM_MAX_TOKENS = 4096
LLM_REASONING_EFFORT = get_env("LLM_REASONING_EFFORT", "high")


def get_llm(*, thinking: bool = True, timeout: float | None = None,
            max_retries: int = 2, max_tokens: int = LLM_MAX_TOKENS) -> ChatDeepSeek:
    """获取 DeepSeek 模型实例。

    主 Agent 使用思考模式（复杂指标拆解与归因规划）；
    需要强制结构化输出的内部评审器（Grader）与轻量路由器使用非思考模式。
    """
    common_kwargs = {
        "model": LLM_MODEL,
        "base_url": LLM_BASE_URL,
        "api_key": LLM_API_KEY,
        "max_tokens": max_tokens,
        "max_retries": max_retries,
    }
    if timeout is not None:
        common_kwargs["timeout"] = timeout
    if thinking:
        return ChatDeepSeek(
            **common_kwargs,
            reasoning_effort=LLM_REASONING_EFFORT,
            extra_body={"thinking": {"type": "enabled"}},
        )
    return ChatDeepSeek(
        **common_kwargs,
        temperature=LLM_TEMPERATURE,
        extra_body={"thinking": {"type": "disabled"}},
    )


# ============ MongoDB 配置 (Agent 控制面：Checkpoint & 三层记忆 Store) ============
MONGODB_URI = get_env("MONGODB_URI", "mongodb://localhost:27017")
MONGODB_DB_NAME = get_env("MONGODB_DB_NAME", "datamind_harness")

# ============ PostgreSQL 配置 (企业业务数仓：OLTP/源库 & Schema-Map 血缘图谱) ============
POSTGRES_URI = get_env(
    "POSTGRES_URI",
    "postgresql://postgres:postgres@localhost:5432/datamind_warehouse",
)
POSTGRES_MAX_SCAN_ROWS = get_env_int("POSTGRES_MAX_SCAN_ROWS", 5_000_000)

# ============ Redis 配置 (SQL 语义结果缓存、Schema-Map 拓扑缓存、重任务异步队列) ============
REDIS_URL = get_env("REDIS_URL", "redis://localhost:6379/0")
SQL_CACHE_TTL_SECONDS = get_env_int("SQL_CACHE_TTL_SECONDS", 600)
SCHEMA_MAP_CACHE_TTL_SECONDS = get_env_int("SCHEMA_MAP_CACHE_TTL_SECONDS", 1800)

# ============ DuckDB 配置 (沙箱/本地内嵌向量化 OLAP 分析引擎) ============
DUCKDB_DATABASE_PATH = get_env("DUCKDB_DATABASE_PATH", ":memory:")
DUCKDB_MEMORY_LIMIT = get_env("DUCKDB_MEMORY_LIMIT", "256MB")

# ============ MCP Server 配置 ============
MCP_SERVER_URL = get_env("MCP_SERVER_URL", "http://localhost:9000")
MCP_SSE_URL = f"{MCP_SERVER_URL}/sse"

# ============ 沙箱配置 ============
SANDBOX_IMAGE = get_env("SANDBOX_IMAGE", "python:3.11-slim")
SANDBOX_WORK_DIR = "/workspace"
SANDBOX_SKILLS_DIR = "/skills"
SANDBOX_MEMORIES_DIR = "/memories"
SANDBOX_WARM_POOL_SIZE = get_env_int("SANDBOX_WARM_POOL_SIZE", 2)
SANDBOX_IDLE_TIMEOUT_MINUTES = get_env_int("SANDBOX_IDLE_TIMEOUT_MINUTES", 30)
SANDBOX_MAINTENANCE_INTERVAL_SECONDS = get_env_int(
    "SANDBOX_MAINTENANCE_INTERVAL_SECONDS", 60
)
SANDBOX_HEALTH_CHECK_INTERVAL_SECONDS = get_env_int(
    "SANDBOX_HEALTH_CHECK_INTERVAL_SECONDS", 30
)

# ============ Store 命名空间 ============
SKILLS_STORE_NAMESPACE = ("persisted-skills",)
PREFERENCES_STORE_NAMESPACE = ("user-preferences",)


def skills_store_namespace(user_id: str) -> tuple[str, str]:
    """Return the isolated Store namespace for one user's custom Skills."""
    return (*SKILLS_STORE_NAMESPACE, user_id)


# ============ Agent 调用上限与上下文配置 ============
MAX_MODEL_CALLS = get_env_int("MAX_MODEL_CALLS", 50)
MAX_TOOL_CALLS = get_env_int("MAX_TOOL_CALLS", 30)
SUMMARIZATION_THRESHOLD = 0.85  # 85% 上下文窗口时触发摘要

# ============ HITL 人工介入中断配置 ============
# 仅对高危写操作、未脱敏敏感宽表导出、重型 ETL 回写任务触发人工审批中断
INTERRUPT_ON_TOOLS = {
    "execute_writeback_sql": {"allowed_decisions": ["approve", "reject"]},
    "export_sensitive_data": {"allowed_decisions": ["approve", "reject"]},
    "trigger_etl_pipeline": {"allowed_decisions": ["approve", "reject"]},
}
