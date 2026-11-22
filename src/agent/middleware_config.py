"""
子Agent中间件工厂（DataMind-Harness 架构 — 完整中间件栈）

为 schema-engineer / python-scientist / report-narrator 子Agent 提供专用中间件配置。
"""
from pathlib import Path
from deepagents.middleware import SummarizationToolMiddleware

from .middlewares.context_injection import ContextInjectionMiddleware
from .middlewares.skills_sync import SkillsSyncMiddleware
from .middlewares.sandbox_breaker import SandboxCircuitBreakerMiddleware
from .middlewares.sql_safety_gate import SqlSafetyGateMiddleware
from .middlewares.tools_summarization import ToolsSummarizationMiddleware
from .schema import DataMindContext


SKILLS_DIR = Path(__file__).parent.parent / "skills"


def get_schema_engineer_middleware(user_context: DataMindContext | None = None) -> list:
    """获取数仓取数与血缘专家子Agent的中间件列表"""
    context = user_context or DataMindContext(username="数仓取数专家")
    return [
        SummarizationToolMiddleware(),
        ContextInjectionMiddleware(user_context=context),
        SqlSafetyGateMiddleware(),
        ToolsSummarizationMiddleware(),
    ]


def get_python_scientist_middleware(user_context: DataMindContext | None = None) -> list:
    """获取沙箱数据科学与可视化子Agent的中间件列表"""
    context = user_context or DataMindContext(username="数据科学专家")
    return [
        SummarizationToolMiddleware(),
        ContextInjectionMiddleware(user_context=context),
        SkillsSyncMiddleware(skills_dir=SKILLS_DIR),
        ToolsSummarizationMiddleware(),
        SandboxCircuitBreakerMiddleware(failure_threshold=3, recovery_timeout=60),
    ]


def get_report_narrator_middleware(user_context: DataMindContext | None = None) -> list:
    """获取商业洞察与受控写回子Agent的中间件列表"""
    context = user_context or DataMindContext(username="商业洞察专家")
    return [
        SummarizationToolMiddleware(),
        ContextInjectionMiddleware(user_context=context),
        SkillsSyncMiddleware(skills_dir=SKILLS_DIR),
        ToolsSummarizationMiddleware(),
        SandboxCircuitBreakerMiddleware(failure_threshold=3, recovery_timeout=60),
    ]


def get_all_subagent_middlewares(user_context: DataMindContext | None = None) -> dict:
    """获取所有子Agent的中间件配置"""
    return {
        "schema-engineer": get_schema_engineer_middleware(user_context),
        "python-scientist": get_python_scientist_middleware(user_context),
        "report-narrator": get_report_narrator_middleware(user_context),
    }
