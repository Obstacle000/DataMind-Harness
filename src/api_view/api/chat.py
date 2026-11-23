"""
核心 SSE 流式对话 + 中断检测 + 中断恢复 + 展示消息持久化

关键设计：
- 双模式流: stream_mode=["messages", "values"], subgraphs=True
- 输出格式: (namespace, mode, data) 三元组
- 中断检测在 messages 处理之前（values 流优先）
- display_messages 实时累积（用户消息 + 助手回复 + 工具调用）
- SSE 事件协议: token / tool_start / tool_args / tool_result / tool_end / interrupt / done
"""
# sse_event类型
# 正文与思考
# thinking：流开始时发 {"status": "start"} 让前端转圈；收到第 1 个正文 token 或工具调用时发 {"status": "end"} 关掉转圈。
# token：大模型逐字输出的正文内容，带 content 和来源 source（main / schema-engineer / python-scientist / report-narrator）。
# 2. 工具调用生命周期
# tool_start：开始调用某个工具，带 name 和 id。
# tool_args：工具参数的流式 JSON 分片。
# tool_result：工具执行完返回的结果（截断前 2000 字）。
# tool_end：该 id 对应的工具卡片结束运行。
# 3. Harness 治理与状态
# phase：阶段切换（thinking / planning / executing / reviewing / result 及中文 label）。
# todo_update：write_todos 任务清单有变动时，推送最新 TODO 列表和勾选状态。
# review_result：Rubric 质检评审器的打分判定结果（verdict、explanation、criteria、iteration）。
# 4. 交互与可视化
# chart_component：DataMind 新增，调用 generate_echarts_schema 后向前端直接下发交互式 ECharts 图表 JSON（Generative UI）。
# interrupt：高危工具触发人工审批挂起，通知前端弹出审批卡片（如 hitl_approval）。
# 5. 流控制与异常
# error：后端抛未捕获异常时推送错误提示。
# done：本次 SSE 流结束信号，带 thread_id 和 interrupted: True/False。
import json
import uuid
from typing import AsyncGenerator
from datetime import datetime

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from langgraph.types import Command

from ..agent_loader import agent_loader
from ...agent.schema import ChatRequest, ResumeRequest
from ...agent.log_utils import web_logger

router = APIRouter(prefix="/api/chat", tags=["chat"])


def sse_event(event: str, data: dict) -> str:
    """格式化 SSE 事件（严格遵循 event:/data: 协议）"""
    return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"


# Harness 阶段 → 前端展示文案
PHASE_LABELS = {
    "planning": "📝 规划中",
    "executing": "⚙️ 执行中",
    "reviewing": "🔍 审查中",
    "result": "📊 已完成",
    "thinking": "💭 思考中",
}


from ...agent.observability import traced_stream, traced_config, event


@traced_stream
async def stream_chat_response(
    message: str,
    thread_id: str,
    user_id: str = "default_user",
    username: str = "用户",
    resume_data: dict = None,
) -> AsyncGenerator[str, None]:
    """
    核心流式响应生成器

    双流模式（subgraphs=True 时输出为 3-tuple）：
    - (namespace, "values", data): 检测中断（interrupts 字段）
    - (namespace, "messages", data): 逐 token 输出 + 工具调用事件

    消息累积：
    - 实时将 assistant 回复追加到 display_messages
    - 流结束后持久化到 MongoDB
    """
    # 每个用户有独立 Agent（含 Proxy、中间件缓存和 SandboxManager 映射）。也就是独立session
    session = await agent_loader.get_session(user_id, username)
    config = traced_config(agent_loader.create_config(thread_id, user_id))
    if resume_data is not None:
        decisions = resume_data.get('decisions', []) if isinstance(resume_data, dict) else []
        event('hitl.resume', approved=any(d.get('type')=='approve' for d in decisions if isinstance(d,dict)),
              rejected=any(d.get('type')=='reject' for d in decisions if isinstance(d,dict)))

    # 加载已有消息（恢复场景）或初始化
    if resume_data:
        display_messages = await agent_loader.get_display_messages(thread_id)
        current_input = Command(resume=resume_data)
    else:
        display_messages = await agent_loader.get_display_messages(thread_id)
        display_messages.append({
            "id": str(uuid.uuid4()),
            "role": "user",
            "content": message,
            "timestamp": datetime.now().isoformat(),
        })
        current_input = {"messages": [{"role": "user", "content": message}]}

    # 把每次收到的文本 token 拼接成完整回复。
    assistant_content = ""
    # 记录这轮调了哪些工具（name、流式拼起来的 args、执行完填进去的 result、running → done 状态）。
    tool_calls_buffer = []
    # 流一开始发了 thinking: start 让前端转圈；等第 1 个正文 token 或工具调用到来时，发一次 thinking: end 关掉转圈，然后把开关置 True，避免后面每个字都重复发。
    thinking_emitted = False
    # 记录当前 Harness 阶段
    phase = "thinking"  # 当前阶段: thinking → planning → executing → reviewing → result
    # 只有 new_phase != last_phase 时才发 event: phase 更新前端顶部的状态胶囊。
    last_phase = "thinking"  # 上次已发射的阶段（去重）
    # last_todos_sig = None：把 todos 列表转成 JSON 字符串当“指纹”；只有 TODO 内容或勾选状态真变了（todos_sig != last_todos_sig），才发 event: todo_update。
    last_todos_sig = None  # 上次已发射的 todo 列表签名（去重）
    # 对审查路由决策 review_decision 做 JSON 指纹去重，防止同一条决策被重复记录。
    last_review_sig = None
    harness_trace: list = []  # Harness 阶段流转 trace（P3 可观测）

    # 同一用户的整个流串行运行，避免两个会话同时覆盖 /tmp、/workspace 中的文件。
    # ContextVar 让图表、文档、Skill 工具在并发用户之间取到正确的稳定 Proxy。
    from ...agent.backends.sandbox_holder import bind_sandbox, reset_sandbox
    from ...agent.backends.sandbox_manager import sandbox_manager
    from ...agent.runtime_context import bind_runtime, reset_runtime

    import time
    lock_started = time.perf_counter()
    # 防止同一个用户两个会话同时覆盖 /tmp、/workspace 中的文件
    await session.run_lock.acquire()
    event('request.lock_wait', duration_ms=(time.perf_counter()-lock_started)*1000)
    lease_acquired = sandbox_manager.acquire_lease(user_id)
    sandbox_token = bind_sandbox(user_id)
    runtime_tokens = bind_runtime(user_id, agent_loader.store)

    try:
        # 发射“深度思考”事件（前端展示思考动画）
        yield sse_event("thinking", {"status": "start"})

        async for namespace, chunk_type, chunk in session.agent.astream(
            input=current_input,
            config=config,
            stream_mode=["messages", "values", "custom"],
            subgraphs=True,
        ):
            # ===== 中断检测 + 结构化 Harness 状态读取（必须在 messages 处理之前）=====
            # 每跑完一个图节点（Super-step）、状态合并进 state 的那一刻，stream_mode="values" 就会立刻吐出整份最新 state 的 chunk。
            if chunk_type == "values":
                if isinstance(chunk, dict):
                    if not namespace and chunk.get("review_decision"):
                        decision = chunk["review_decision"]
                        event('review.decision', review=decision.get('decision')=='review')
                        signature = json.dumps(decision, ensure_ascii=False, sort_keys=True)
                        if signature != last_review_sig:
                            last_review_sig = signature
                            harness_trace.append({"type": "review_decision", **decision,
                                                  "timestamp": datetime.now().isoformat()})
                    # --- 结构化阶段流转（来自 HarnessPhaseMiddleware 写入的 state.phase）---
                    new_phase = chunk.get("phase")
                    if new_phase and new_phase != last_phase:
                        last_phase = new_phase
                        phase = new_phase
                        label = PHASE_LABELS.get(new_phase, new_phase)
                        harness_trace.append({
                            "type": "phase",
                            "phase": new_phase,
                            "label": label,
                            "timestamp": datetime.now().isoformat(),
                        })
                        yield sse_event("phase", {
                            "phase": new_phase,
                            "label": label,
                        })

                    # --- 结构化 todo 列表（来自 TodoListMiddleware 的 write_todos 状态）---
                    todos = chunk.get("todos")
                    if todos is not None:
                        # 这个chunk是tools 节点刚执行完 write_todos 工具,更新state的那一刻发出来的
                        # 所以此时harness还没改Plan
                        # 这里判断todos是否发生变动告知前端
                        todos_sig = json.dumps(todos, ensure_ascii=False, sort_keys=True)
                        if todos_sig != last_todos_sig:
                            last_todos_sig = todos_sig
                            plan_items = [
                                {
                                    "id": f"todo-{i}",
                                    "content": t.get("content", ""),
                                    "status": t.get("status", "pending"),
                                }
                                for i, t in enumerate(todos)
                            ]
                            if plan_items:
                                yield sse_event("todo_update", {
                                    "phase": "planning",
                                    "todos": plan_items,
                                })

                    # --- 中断检测 ---
                    # LangGraph emits __interrupt__; retain the legacy state key.
                    interrupts = chunk.get("__interrupt__") or chunk.get("interrupts")
                    if interrupts:
                        for interrupt_item in interrupts:
                            interrupt_value = (
                                interrupt_item.value
                                if hasattr(interrupt_item, "value")
                                else interrupt_item
                            )
                            if isinstance(interrupt_value, dict):
                                # 判断中断类型
                                if interrupt_value.get("type") == "order_info_request":
                                    yield sse_event("interrupt", {
                                        "interrupt_type": "order_info_supplement",
                                        "missing_fields": interrupt_value.get("missing_fields", []),
                                        "message": interrupt_value.get("message", ""),
                                        "extracted_data": interrupt_value.get("current_data", {}),
                                    })
                                elif "action_requests" in interrupt_value:
                                    yield sse_event("interrupt", {
                                        "interrupt_type": "hitl_approval",
                                        "tool_name": interrupt_value.get("tool_name", ""),
                                        "tool_args": interrupt_value.get("action_requests", {}),
                                        "order_data": interrupt_value.get("action_requests", {}),
                                    })
                                else:
                                    # 通用中断（interrupt_on 触发的审批）
                                    yield sse_event("interrupt", {
                                        "interrupt_type": "hitl_approval",
                                        "tool_name": interrupt_value.get("tool_name", ""),
                                        "tool_args": interrupt_value.get("tool_args", interrupt_value),
                                        "order_data": interrupt_value.get("tool_args", interrupt_value),
                                    })

                        # 保存当前累积的消息
                        if assistant_content:
                            display_messages.append({
                                "id": str(uuid.uuid4()),
                                "role": "assistant",
                                "content": assistant_content,
                                "toolCalls": tool_calls_buffer,
                                "timestamp": datetime.now().isoformat(),
                            })
                        await agent_loader.save_display_messages(thread_id, display_messages)
                        await agent_loader.save_harness_trace(thread_id, harness_trace)
                        yield sse_event("done", {"thread_id": thread_id, "interrupted": True})
                        return
                continue

            # ===== Custom 流处理（评审器等中间件的结构化事件）=====
            if chunk_type == "custom":
                if isinstance(chunk, dict):
                    ev_type = chunk.get("type", "")
                    if ev_type == "rubric_evaluation_start":
                        event('grader.start')
                        # 评审器开始 grading → 进入审查阶段
                        if phase != "reviewing":
                            phase = "reviewing"
                            last_phase = "reviewing"
                            yield sse_event("phase", {"phase": "reviewing", "label": "🔍 审查中"})
                    elif ev_type == "rubric_evaluation_end":
                        event('grader.end', satisfied=chunk.get('result')=='satisfied', iteration=chunk.get('iteration',0))
                        # 评审器产出结构化判定 → 发射 review_result 事件
                        review_data = {
                            "verdict": chunk.get("result", ""),
                            "explanation": chunk.get("explanation", ""),
                            "criteria": chunk.get("criteria", []),
                            "iteration": chunk.get("iteration", 0),
                        }
                        harness_trace.append({
                            "type": "review",
                            **review_data,
                            "timestamp": datetime.now().isoformat(),
                        })
                        yield sse_event("review_result", review_data)
                continue

            # ===== Messages 流处理 =====
            if chunk_type == "messages":
                # Routing output is internal control data, never assistant text
                # or a user-visible tool invocation.
                metadata = chunk[1] if isinstance(chunk, (tuple, list)) and len(chunk) > 1 else {}
                if isinstance(metadata, dict) and "review_router" in (metadata.get("tags") or []):
                    continue
                # chunk 格式: (message_chunk, metadata_dict)
                if isinstance(chunk, (list, tuple)) and len(chunk) >= 1:
                    token = chunk[0]
                else:
                    token = chunk

                if token is None:
                    continue

                token_type = getattr(token, "type", "")
                content = getattr(token, "content", "")
                tool_call_chunks = getattr(token, "tool_call_chunks", None)

                # AIMessage with tool_call_chunks → tool_start / tool_args
                if tool_call_chunks:
                # 此时是 Model 节点在流式吐 AIMessageChunk（大模型刚决定要调哪个工具，工具还没开始执行）
                    # 首次收到工具调用时结束 thinking 状态
                    # 为什么第 1 个 Token 来时发射 thinking: {status: "end"}？
                    # 大模型（尤其是带思考链的模型）在回答前会有几秒钟的“思考延迟”。
                    # 前端借此展示“🤔 思考中...”动效；当第 1 个真正的内容 Token 吐出时，
                    # 必须立刻关掉思考动效，切换为正文打字机模式。
                    if not thinking_emitted:
                        yield sse_event("thinking", {"status": "end"})
                        thinking_emitted = True
                    for tc in tool_call_chunks:
                        if isinstance(tc, dict):
                            # 记录tc
                            if tc.get("name"):
                                tool_calls_buffer.append({
                                    "id": tc.get("id", str(uuid.uuid4())),
                                    "name": tc["name"],
                                    "args": "",
                                    "status": "running",
                                })
                                yield sse_event("tool_start", {
                                    "name": tc["name"],
                                    "id": tc.get("id", ""),
                                })
                            if tc.get("args"):
                                # 追加到最近的 tool_call
                                if tool_calls_buffer:
                                    tool_calls_buffer[-1]["args"] += tc["args"]
                                yield sse_event("tool_args", {"args": tc["args"]})
                    continue

                # ToolMessage → tool_result / tool_end
                # 等大模型把参数吐完，LangGraph 切到 Tools 节点真正去跑这个工具（耗时几百毫秒到几秒）。
                # 工具跑完后（无论是正常查出数据，还是报错返回异常信息），Tools 节点会吐出一个 ToolMessage
                # 与if tool_call_chunks互斥,这个是大模型toolcall,下面是调用完毕
                if token_type == "tool":
                    tool_name = getattr(token, "name", "unknown")
                    tool_content = content if isinstance(content, str) else str(content)
                    # 更新 buffer 中对应工具的状态
                    for tc in tool_calls_buffer:
                        if tc["name"] == tool_name and tc["status"] == "running":
                            # 截取前 1000 字（tool_content[:1000]）更新进 tool_calls_buffer
                            # 由于chunk(tool)比
                            tc["result"] = tool_content[:1000]
                            tc["status"] = "done"
                            break

                    # 检测 TODO 工具调用，发射任务列表更新事件
                    if "todo" in tool_name.lower() or "task" in tool_name.lower():
                        try:
                            # 尝试从工具参数中提取 todo 列表
                            todo_args = None
                            for tc in tool_calls_buffer:
                                if tc["name"] == tool_name and tc.get("args"):
                                    todo_args = tc["args"]
                                    break
                            if todo_args:
                                yield sse_event("todo_update", {
                                    "tool": tool_name,
                                    "args": todo_args[:2000],
                                    "result": tool_content[:500],
                                })
                        except Exception:
                            pass

                    # 检测 Generative UI 图表组件调用，向前端流式下发交互式 ECharts Schema
                    if tool_name == "generate_echarts_schema":
                        try:
                            chart_payload = json.loads(tool_content)
                            if isinstance(chart_payload, dict) and chart_payload.get("component") == "InteractiveECharts":
                                yield sse_event("chart_component", chart_payload)
                        except Exception:
                            pass

                    yield sse_event("tool_result", {
                        "name": tool_name,
                        "content": tool_content[:2000],
                    })
                    yield sse_event("tool_end", {
                        "id": getattr(token, "tool_call_id", ""),
                    })
                    continue

                # AIMessage 纯文本 → token 事件
                if content and isinstance(content, str) and content.strip():
                    # 首次收到 token 时结束 thinking
                    if not thinking_emitted:
                        yield sse_event("thinking", {"status": "end"})
                        thinking_emitted = True
                    assistant_content += content

                    # 判断来源（通过 namespace 元组识别 DataMind 子 Agent）
                    source = "main"
                    if namespace:
                        ns_str = str(namespace)
                        if "schema-engineer" in ns_str:
                            source = "schema-engineer"
                        elif "python-scientist" in ns_str or "analyst" in ns_str:
                            source = "python-scientist"
                        elif "report-narrator" in ns_str or "order" in ns_str:
                            source = "report-narrator"
                    yield sse_event("token", {"content": content, "source": source})

        # ===== 流正常结束 =====
        if assistant_content:
            display_messages.append({
                "id": str(uuid.uuid4()),
                "role": "assistant",
                "content": assistant_content,
                "toolCalls": tool_calls_buffer if tool_calls_buffer else None,
                "timestamp": datetime.now().isoformat(),
            })

        await agent_loader.save_display_messages(thread_id, display_messages)
        await agent_loader.save_harness_trace(thread_id, harness_trace)

        # 自动生成会话标题（首条消息前20字）
        if message:
            title = message[:20] + "..." if len(message) > 20 else message
            await agent_loader.save_conversation(thread_id, user_id, title)

        yield sse_event("done", {"thread_id": thread_id, "interrupted": False})

    except Exception as e:
        web_logger.error(f"Stream error for thread {thread_id}: {e}", exc_info=True)
        yield sse_event("error", {"message": f"服务内部错误: {str(e)[:200]}"})
        yield sse_event("done", {"thread_id": thread_id, "interrupted": False})
    finally:
        reset_runtime(runtime_tokens)
        reset_sandbox(sandbox_token)
        if lease_acquired:
            sandbox_manager.release_lease(user_id)
        session.run_lock.release()


@router.post("/stream")
async def chat_stream(request: ChatRequest):
    """SSE 流式对话端点"""
    await agent_loader.initialize()

    thread_id = request.thread_id or agent_loader.generate_thread_id()
    existing_owner = await agent_loader.get_conversation_user_id(thread_id)
    if existing_owner and existing_owner != request.user_id:
        raise HTTPException(status_code=403, detail="该会话不属于当前用户")

    # 在流开始时就记录归属，确保在中断后 resume 能找到同一个用户沙箱。
    title = request.message[:20] + "..." if len(request.message) > 20 else request.message
    await agent_loader.save_conversation(thread_id, request.user_id, title)

    return StreamingResponse(
        stream_chat_response(
            message=request.message,
            thread_id=thread_id,
            user_id=request.user_id,
            username=request.username,
        ),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Thread-Id": thread_id,
            "X-Accel-Buffering": "no",
        },
    )


@router.post("/{thread_id}/resume")
async def chat_resume(thread_id: str, request: ResumeRequest):
    """中断恢复端点"""
    await agent_loader.initialize()
    user_id = await agent_loader.get_conversation_user_id(thread_id) or "default_user"

    return StreamingResponse(
        stream_chat_response(
            message="",
            thread_id=thread_id,
            user_id=user_id,
            resume_data=request.resume,
        ),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Thread-Id": thread_id,
            "X-Accel-Buffering": "no",
        },
    )


@router.get("/{thread_id}/state")
async def chat_state(thread_id: str):
    """获取 Agent 状态（是否处于中断中）"""
    await agent_loader.initialize()
    user_id = await agent_loader.get_conversation_user_id(thread_id) or "default_user"
    session = await agent_loader.get_session(user_id)
    config = agent_loader.create_config(thread_id, user_id)
    try:
        state = await session.agent.aget_state(config)
        is_interrupted = bool(state.next) if state else False
        return {"thread_id": thread_id, "interrupted": is_interrupted}
    except Exception:
        return {"thread_id": thread_id, "interrupted": False}


@router.get("/{thread_id}/history")
async def chat_history(thread_id: str):
    """获取对话消息历史"""
    messages = await agent_loader.get_display_messages(thread_id)
    return {"thread_id": thread_id, "messages": messages}
