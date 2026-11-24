# DataMind-Harness

**基于 Harness Engineering 中间件洋葱架构的企业级自愈型智能 BI & 数据科学 Agent**

DataMind-Harness 面向复杂多表企业数仓的 NL2SQL 取数、统计归因分析与交互式可视化场景，在 LangGraph / DeepAgents 之上构建了一套完整的**确定性治理底座（Harness）与自愈闭环（Loop）**：通过两阶段数仓血缘骨架图（Schema-Map）、SQL 安全与成本门禁、Rubric 质检自动返工、Docker 沙箱热替换与熔断、三层长期记忆、以及双流 SSE + HITL 人工审批机制，解决大模型在真实数仓场景下“表多连错、幻觉瞎编、高危写库失控、长对话上下文爆炸”等工程痛点。

---

## 核心特性

### 1. 两阶段数仓血缘骨架引擎（Two-Phase Schema-Map）
借鉴 Aider `RepoMap` 思想并针对企业数仓深度重构（核心实现见 `src/agent/clients/schema_map.py`）：
* **第一阶段（离线 / FastAPI `lifespan` 启动预热 + MD5 指纹 0ms 短路）**：
  * **三层关系发现漏斗**：
    1. **Layer 1（物理外键）**：读取 PostgreSQL `information_schema` 真实主键、外键与字段注释；
    2. **Layer 2（SQL AST 挖掘）**：使用 `sqlglot` 解析历史黄金 SQL 与线上执行成功的 SQL，自动沉淀高频 `JOIN ON` 关系并累加 `verified_count`；
    3. **Layer 3（隐式外键探针 + 语义校验）**：针对无物理外键、字段异名的孤岛表（如 `fact_quality_claims`），通过 SQL 包含依赖率（Inclusion Dependency Ratio ≥ 80%）+ 语义门控自动发现隐藏关联。
  * **规范化四元组去重与落盘**：按 `(left_table, left_col, right_table, right_col)` 严格去重，支持表/字段变更自动清理失效边，并原子持久化至 `schema_lineage.yaml`。
  * **SQL 报错自愈**：执行只读 SQL 触发字段/表不存在异常时，自动击穿 Redis 缓存并重跑对账。
* **第二阶段（在线查询毫秒级召回）**：
  * **加权 Personalized PageRank**：结合用户查询意图与对数平滑后的验证频次权重，选出 Top-K 核心表；
  * **多源 BFS 自动补桥**：检测选中表是否出现孤岛断链，自动从全库图中拉入最短路径桥表；
  * **Kruskal 最大生成树防环拆分**：将所有候选边拆分为 **「🌲 默认主干连法（无环生成树）」** 与 **「🔄 备选替换连法（细粒度/复合键替代边）」**，防止大模型堆叠三角形 `JOIN` 导致指标翻倍；
  * **Aider 同款二分查找控预算**：在 `token_budget_chars` 内二分寻找最大完整表数 `k`，绝不粗暴截断底部的 `JOIN ON` 连表路径。

### 2. Harness Engineering 中间件治理栈
采用洋葱模型编排全链路控制流（见 `src/agent/main_agent.py`）：
* **混合审查路由 + Rubric 自愈闭环**：
  * `HarnessPhaseMiddleware` + `ReviewPolicy`（规则 + 结构化路由器）按需注入评审标准 `rubric`，避免简单闲聊浪费质检成本；
  * `ReviewExecutionGate` 监控运行时异常与写操作写锁（防止不可逆写回操作被打回重跑）；
  * `SafeRubricMiddleware` 在 `after_agent` 阶段对回答按 Rubric 打分，不达标时带扣分理由自动打回重写，直到达标或触发 `max_iterations` 熔断。
* **SQL 安全与成本双门禁（`SqlSafetyGateMiddleware`）**：
  * 拦截只读查询工具中的一切 DDL/DML 危险语句；
  * 执行前自动跑 `EXPLAIN (FORMAT JSON)` 预估扫描行数，超阈值（默认 500 万行）拒绝全表扫描。
* **HITL 人工审批中断恢复**：
  * 对 `execute_writeback_sql`、`export_sensitive_data` 等高危写回/敏感导出工具挂载 `interrupt_on`，命中时冻结 LangGraph Checkpoint 并推送前端审批卡片，经用户批准后通过 `Command(resume=...)` 原位续跑。
* **用户级隔离沙箱 + 热替换 + 三态熔断**：
  * `SandboxManager` 维护预热容器池与运行租约（`acquire_lease` / `release_lease` 防后台巡检误杀）；
  * `SandboxBackendProxy` 保持 Agent 后端引用稳定，容器异常时由 `SandboxHealthMiddleware` 无缝重建并失效技能同步缓存；
  * `SandboxCircuitBreakerMiddleware` 提供三态熔断保护；本机无 Docker 时自动降级至 `LocalShellBackend`。

### 3. 多子图协同（SubAgents）与渐进式技能（Skills）
* **三大专职 SubAgent**（独立上下文子图，避免中间调试噪声污染主对话）：
  * `schema-engineer`：数仓血缘探索与复杂 SQL 构建；
  * `python-scientist`：沙箱内 Python / DuckDB 统计建模与异常归因；
  * `report-narrator`：商业分析报告撰写与 ECharts 交互图表编排。
* **渐进式技能体系**：内置 `metric-attribution`（核心指标波动拆解与归因分析）技能，并支持用户自定义技能持久化至 MongoDB Store。

### 4. 双流 SSE 协议、Generative UI 与全链路可观测
* **三模混合流式引擎**：`stream_mode=["messages", "values", "custom"], subgraphs=True`，统一输出 `thinking` / `phase` / `todo_update` / `tool_start` / `tool_args` / `tool_result` / `tool_end` / `chart_component` / `review_result` / `interrupt` / `token` / `error` / `done` 共 13 类 SSE 事件；
* **Generative UI**：调用 `generate_echarts_schema` 工具后直接流式下发交互式 ECharts JSON 组件；
* **隐私脱敏可观测性**：基于 `ContextVar` 请求级隔离接入 Langfuse（只报耗时、Token 与脱敏 ID，支持 `start_span` 跨迭代计时与 `async with` 上下文管理），同时将 Harness 阶段与评审轨迹持久化至 MongoDB `harness_traces`。

---

## 存储与数据架构

| 组件 | 定位与职责 | 本地降级策略 |
| :--- | :--- | :--- |
| **MongoDB** | Agent 控制面：`checkpoints`（`MongoDBSaver` 多轮状态与中断现场）、`langgraph_store`（三层记忆与用户技能）、`display_messages`、`conversations`、`harness_traces` | 核心控制面依赖 |
| **PostgreSQL** | 企业业务数仓数据源（8 张业务表，含 `dim_customers`、`fact_orders`、`fact_returns`、`fact_quality_claims` 等） | 离线时自动降级至 DuckDB |
| **DuckDB** | 进程内向量化 OLAP 分析引擎，预置完整企业数仓测试数据集 | 无 `duckdb` 包时降级至内存 SQLite |
| **Redis** | SQL SHA256 结果缓存、Schema 元数据缓存、重型异步分析任务队列 | 离线时自动降级至线程安全内存 TTL 缓存 |
| **Docker Sandbox** | 用户级隔离代码执行与技能运行环境 | 无 Docker 时自动降级至 `LocalShellBackend` |

---

## 目录结构

```text
DataMind-Harness/
├── README.md
└── src/
    ├── agent/
    │   ├── config.py                      # 全局配置（LLM / Mongo / PG / Redis / DuckDB / Sandbox）
    │   ├── main_agent.py                  # 主 Agent 装配与洋葱模型中间件编排
    │   ├── harness.py                     # HarnessPhaseState & HarnessPhaseMiddleware 状态机
    │   ├── review_policy.py               # 混合审查路由器与动态 Rubric 拼装
    │   ├── observability.py               # Langfuse 脱敏链路追踪（ContextVar / SpanTimer）
    │   ├── prompts.py                     # 系统提示词与角色设定
    │   ├── clients/
    │   │   ├── schema_map.py              # 两阶段 Schema-Map 血缘引擎
    │   │   ├── schema_lineage.yaml        # 持久化数仓血缘语义层
    │   │   ├── postgres_client.py         # PostgreSQL 元数据、EXPLAIN 与只读查询客户端
    │   │   ├── duckdb_client.py           # DuckDB 向量化 OLAP 引擎
    │   │   └── redis_client.py            # Redis 缓存与异步分析队列
    │   ├── middlewares/
    │   │   ├── sql_safety_gate.py         # SQL 安全审计与 EXPLAIN 成本门禁
    │   │   ├── review_gate.py             # ReviewExecutionGate & SafeRubricMiddleware
    │   │   ├── sandbox_health.py          # 沙箱健康巡检与热替换重建
    │   │   ├── sandbox_breaker.py         # 沙箱三态熔断器
    │   │   ├── warm_memory.py             # WARM 记忆注入中间件
    │   │   └── ...                        # 上下文注入、技能同步、工具摘要、记忆巩固等
    │   ├── backends/                      # SandboxManager / Proxy / Holder / Bootstrap
    │   ├── memory/                        # 三层记忆系统（HOT / WARM / COLD）
    │   ├── tools/
    │   │   └── datamind_tools.py          # BI 核心工具集（Schema-Map / SQL / OLAP / ECharts / 写回）
    │   └── subagents/configs/             # schema_engineer / python_scientist / report_narrator
    ├── api_view/
    │   ├── web_main.py                    # FastAPI 入口（含 lifespan Schema-Map 预热与沙箱池维护）
    │   ├── agent_loader.py                # 用户级 AgentSession 缓存与双检锁初始化
    │   ├── mongodb_store.py               # 基于 MongoDB 的 LangGraph BaseStore 实现
    │   └── api/
    │       ├── chat.py                    # SSE 流式对话、HITL 中断恢复、状态与历史接口
    │       └── history.py                 # 会话列表与历史管理接口
    ├── mcp_server/                        # MCP Server 工具服务
    └── skills/analytics/metric-attribution/ # 指标归因分析 Skill
```

---

## 快速开始

### 1. 环境准备
推荐使用 Python 3.11+，在根目录或运行环境中配置 `.env`：

```env
# LLM 配置（默认 DeepSeek）
DEEPSEEK_API_KEY=your_api_key
LLM_MODEL=deepseek-chat
LLM_BASE_URL=https://api.deepseek.com

# MongoDB（Agent 状态与记忆）
MONGODB_URI=mongodb://localhost:27017
MONGODB_DB_NAME=datamind_harness

# 可选外部依赖（未启动时自动启用内置本地降级模式）
POSTGRES_URI=postgresql://postgres:postgres@localhost:5432/datamind_warehouse
REDIS_URL=redis://localhost:6379/0

# 可选 Langfuse 链路追踪
DATAMIND_LANGFUSE_ENABLED=false
```

### 2. 启动 FastAPI 服务
```bash
uvicorn src.api_view.web_main:app --host 0.0.0.0 --port 8000 --reload
```
服务启动时，`lifespan` 会自动在后台线程完成 **Schema-Map 第一阶段预热** 与 **沙箱预热池初始化**。

### 3. 核心 HTTP 接口
* `POST /api/chat/stream`：SSE 流式对话接口（返回 `text/event-stream`）
* `POST /api/chat/{thread_id}/resume`：HITL 人工审批中断恢复接口
* `GET /api/chat/{thread_id}/state`：查询会话当前是否处于审批中断状态
* `GET /api/chat/{thread_id}/history`：拉取会话展示历史消息
* `GET /api/download/{filename}`：下载沙箱生成的分析报告或数据产物
* `GET /health`：服务健康检查
