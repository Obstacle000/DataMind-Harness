"""
DataMind-Harness 数据模型定义
DataMindContext、UserPreferences、ChatRequest、Phase、Plan、ReviewResult 等 Pydantic 模型
定义整个项目中流转的强类型数据契约。
"""
from typing import Optional, List, Literal, Dict
from pydantic import BaseModel, Field
from datetime import datetime
from enum import Enum


class UserPreferences(BaseModel):
    """用户偏好与动态指标字典模型（WARM 层投影）"""
    preferred_output: str = Field(default="markdown", description="首选输出格式: markdown/table/json/chart")
    preferred_chart_type: str = Field(default="line", description="首选图表类型: bar/line/pie/radar/waterfall/heatmap等")
    preferred_currency: str = Field(default="CNY", description="首选货币单位")
    preferred_language: str = Field(default="zh", description="首选语言: zh/en")
    metric_definitions: Dict[str, str] = Field(
        default_factory=dict,
        description="双时态业务指标口径字典（如 {'净利率': '(毛利-退货-运费)/销售额'}）",
    )
    recent_tables: List[str] = Field(default_factory=list, description="最近查询的数仓表名")
    recent_suppliers: List[str] = Field(default_factory=list, description="最近关注的业务实体/供应商")
    recent_queries: List[str] = Field(default_factory=list, description="最近的分析查询记录")


class DataMindContext(BaseModel):
    """DataMind 智能数据分析上下文 — 每次请求注入到 Agent state"""
    user_id: str = Field(default="default_user", description="用户ID")
    username: str = Field(default="数据分析师", description="用户名")
    preferences: dict = Field(default_factory=dict, description="用户偏好与指标口径字典")
    warm_memory: str = Field(default="", description="WARM 层记忆摘要（语义偏好 + 指标字典 + 近期情节），注入系统提示词")
    session_start: str = Field(default_factory=lambda: datetime.now().isoformat())


# 向后兼容别名（确保原有测试和中间件引用不报错）
ProcurementContext = DataMindContext


class ChatRequest(BaseModel):
    """聊天请求模型"""
    message: str = Field(..., description="用户消息内容")
    images: list[str] = Field(
        default_factory=list,
        description="可选图片列表（支持 http(s) URL、data:image/...;base64,... 或纯 Base64 字符串）",
    )
    thread_id: Optional[str] = Field(default=None, description="会话线程ID，为空则创建新会话")
    user_id: str = Field(default="default_user", description="用户ID")
    username: str = Field(default="数据分析师", description="用户名")


class ResumeRequest(BaseModel):
    """中断恢复请求模型"""
    resume: dict = Field(..., description="恢复数据，格式取决于中断类型 (approve/reject 或补充字段)")


class SSEEvent(BaseModel):
    """SSE 事件模型"""
    event: str = Field(
        ...,
        description="事件类型: token/thinking/phase/todo_update/chart_component/review_result/interrupt/done",
    )
    data: dict = Field(default_factory=dict, description="事件数据")


class ConversationRecord(BaseModel):
    """会话记录"""
    thread_id: str
    user_id: str
    title: str = "新数据分析会话"
    created_at: str = Field(default_factory=lambda: datetime.now().isoformat())
    updated_at: str = Field(default_factory=lambda: datetime.now().isoformat())


class DisplayMessage(BaseModel):
    """前端展示消息"""
    role: str = Field(..., description="角色: user/assistant/tool")
    content: str = Field(default="", description="消息内容")
    tool_calls: Optional[List[dict]] = Field(default=None, description="工具调用信息")
    source: Optional[str] = Field(
        default=None,
        description="来源: main / schema-engineer / python-scientist / report-narrator",
    )
    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat())


# ============================================================
# Harness 架构结构化状态模型（Planning → Executing → Review → Result）
# ============================================================

class Phase(str, Enum):
    """Harness 工作流阶段（图状态机）"""
    thinking = "thinking"      # 思考中（初始态）
    planning = "planning"      # Planning：生成分析与取数规划
    executing = "executing"    # Executing：调用 SQL/DuckDB/沙箱工具或子Agent执行
    reviewing = "reviewing"    # Review：数据一致性质检法官 (Grader) 校验结果
    result = "result"          # Result：结构化输出最终洞察报告与交互图表


class PlanStep(BaseModel):
    """规划中的单个步骤"""
    id: str = Field(..., description="步骤唯一标识")
    content: str = Field(..., description="步骤描述")
    status: Literal["pending", "in_progress", "completed"] = Field(
        default="pending", description="步骤状态"
    )
    result: Optional[str] = Field(default=None, description="步骤执行结果摘要")


class Plan(BaseModel):
    """Harness Planning 阶段的产物：结构化任务规划"""
    steps: List[PlanStep] = Field(default_factory=list, description="规划步骤列表")
    created_at: str = Field(default_factory=lambda: datetime.now().isoformat())
    current_step: Optional[str] = Field(default=None, description="当前执行中的步骤 id")


class ReviewResult(BaseModel):
    """Harness Review 阶段的产物：数据质检评审器对执行结果的判定"""
    verdict: Literal[
        "satisfied", "needs_revision", "failed",
        "max_iterations_reached", "grader_error",
    ] = Field(..., description="评审判定")
    explanation: str = Field(default="", description="评审结论说明")
    criteria: List[dict] = Field(default_factory=list, description="逐条评审标准判定")
    iteration: int = Field(default=0, description="评审迭代次数")
