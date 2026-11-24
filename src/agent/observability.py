"""Opt-in metadata-only Langfuse tracing. Never forwards callback payloads."""
import asyncio
import functools
import hashlib
import hmac
import os
import re
import threading
import time
from contextvars import ContextVar
from langchain_core.callbacks import BaseCallbackHandler
# 为啥要搞一个current,直接搞一个全局变量trace不行吗?
# 如果用普通全局变量，只要有两个用户同时发请求，链路追踪瞬间就串线乱套了
# 如果用普通全局变量 current_trace = None：
# 00:01：张三的请求进来，设置全局变量 current_trace = Trace(张三)；
# 00:02：张三调用大模型，遇到 await 挂起让出 CPU；
# 00:03：李四的请求进来，把同一个全局变量改写成了 current_trace = Trace(李四)；
# 00:04：张三的协程醒来，调了一句 event('grader.start') 去读 current_trace——结果读成了李四的 Trace！ 张三的 Token 消耗和报错全被挂到李四的链路树下面去了。
# ContextVar是给每一个并发请求（异步协程 Task）分配的专属储物柜
# _current里的trace对象本身已经进行了用户隔离,每一次请求协程都创建一个独立的trace进行链路追踪
_current = ContextVar('datamind_trace', default=None)
_client = None
_client_lock = threading.Lock()
# 1. 外部调用chat接口,会调用stream_chat_response的修饰器@traced_stream
# 修饰器内部创建Trace对象,初始化的时候创建链路根节点name='erp.request',以及监听器callback,并存到_current:ContextVar
# 2. 接着调用stream_chat_response,里面调用traced_config,将刚创建的监听器传入运行时配置config里(字典)\
# 这个config要在astream(input=..., config=config)的时候传给 LangGraph
# 由于MetadataCallback 继承了BaseCallbackHandler
# 在astream内部LangGraph会在正确时机调用监听器里的on_方法
# 当执行 session.agent.astream(...) 时，由于监听器LangGraph 会在各个生命周期自动触发回调：
# 遇到大模型调用时：
#   触发 on_chat_model_start：自动向 Trace 申请一个子节点 trace.start(name='model.call', generation=True)；
#   模型回答完毕触发 on_llm_end：探针自动从响应对象中扒出 usage_metadata（Prompt Tokens、Completion Tokens），上报给 Langfuse。
# 遇到工具执行时：
#   触发 on_tool_start：自动开辟子节点 trace.start(name='tool.read_skill')；
#   工具执行完触发 on_tool_end：自动闭合该工具节点。
# 3. 当 Agent 在沙箱里干活、读技能、等待锁时，代码里显式写的度量器生效
# 通过@measured("name")和event()获取当前trace,
# 接着调用trace的event(),在链路树上“开辟一个子分支”挂载到根节点
# 首先调用 self.start(name),Langfuse 客户端在内存里新建了一个名为 name 的节点
# 接着对数据脱敏,然后span.end()关闭,并将这个打包好的数据块压入 Langfuse 的内存待发队列.把沉重的网络发包工作打包扔给后台线程慢慢处理
# @traced_stream结束时调用trace.finish,关监听,关节点
# 4. 请求完毕，结算并异步上传（Flush）
# 当 SSE 流输出最后一句 yield sse_event("done", ...)，或发生报错时
# 1. @traced_stream 的 finally 块被触发
# 2. 关停所有子节点:self.callback.close()；
# 3. 计算总账：记录总耗时 (time.perf_counter() - started) * 1000 毫秒；
# 4. 根节点收口：self.root.end()；
# 5. 异步上传：Langfuse 客户端在后台线程池中，把这次请求收集到的所有节点打包压缩成一个 JSON，通过 HTTP 请求发送到公司的 Langfuse 服务器。
# 最终效果
# ▼ erp.request  [ 2.45s ] (status: completed, user: c71e89f...)
#   ├─ request.lock_wait   [ 0.12ms ]
#   ├─► agent.step         [ 2.40s ]
#   │   ├─► model.call     [ 1.80s ]  (DeepSeek-Chat | Input: 850 tokens, Output: 240 tokens)
#   │   ├─► tool.read_skill [ 4.2ms ]
#   │   │     └─ skills.read [ 3.1ms ] (success: true)
#   │   └─► tool.generate_chart [ 580ms ]
#   └─ request.first_token [ 650ms ]
def client():
    global _client
    enabled = os.getenv('DATAMIND_LANGFUSE_ENABLED', os.getenv('ERP_LANGFUSE_ENABLED', 'false'))
    if enabled.lower() != 'true': return None
    hash_key = os.getenv('DATAMIND_TRACE_HASH_KEY', os.getenv('ERP_TRACE_HASH_KEY'))
    if not all(os.getenv(k) for k in ('LANGFUSE_SECRET_KEY', 'LANGFUSE_PUBLIC_KEY', 'LANGFUSE_BASE_URL')) or not hash_key: return None
    with _client_lock:
        if _client is None:
            try:
                from langfuse import Langfuse
                if os.getenv('DATAMIND_LANGFUSE_DIRECT', os.getenv('ERP_LANGFUSE_DIRECT','false')).lower()=='true':
                    from urllib.parse import urlparse
                    host=urlparse(os.environ['LANGFUSE_BASE_URL']).hostname
                    if host: os.environ['NO_PROXY']=','.join(filter(None,[os.getenv('NO_PROXY',''),host]))
                _client = Langfuse(timeout=10, flush_at=20, flush_interval=5,
                    environment=os.getenv('DATAMIND_TRACE_ENVIRONMENT', os.getenv('ERP_TRACE_ENVIRONMENT','development')),
                    release=os.getenv('DATAMIND_TRACE_RELEASE', os.getenv('ERP_TRACE_RELEASE','unversioned')))
            except Exception: return None
    return _client


def pseudonym(value):
    key = os.getenv('DATAMIND_TRACE_HASH_KEY', os.getenv('ERP_TRACE_HASH_KEY', 'datamind-default-key'))
    return hmac.new(key.encode(), str(value).encode(), hashlib.sha256).hexdigest()[:32]

# 链路追踪器 - 底层接入的是大模型领域最主流的开源 APM 平台：Langfuse(开源版langsmith)。
# 流程:
# @traced_stream 修饰stream_chat_response 初始化Trace,调用start_observation开启根节点root,初始化回调器
#   traced_config把回调器放进运行时配置里
#   @measured/event() - 瞬时打点,调用start()不传parent_id,开辟根节点写第一层节点,span开启后更新,立马关闭soan,传入name,和自定义字段
#   期间遇到sse流,就根据需要瞬时打点
#   内部官方源码自动调用on_方法,回调我自己写的on_方法,遇到on_start(),调用start(),传parent_id,开辟非第一层子节点,遇到on_end(),调用span.end()
# 调用finish()关闭root根节点,reset恢复trace
class Trace:
    def __init__(self, user, thread, resume=False):
        self.client = client() # Langfuse实例
        self.root = None
        self.started = time.perf_counter()
        self.first_token = False
        if self.client:
            # 在单次请求开始时，初始化整条链路追踪的「树根节点（Root Span）」，并完成严格的「数据脱敏与上下文绑定」。
            try:
                from langfuse import propagate_attributes
                # 用户 ID 强力加密脱敏
                # pseudonym:它使用系统的秘钥通过 HMAC-SHA256 哈希算法把真实用户名加密成了一串 32 位的随机摘要串
                identity={'user_id':pseudonym(user),'session_id':pseudonym(thread)}
                # 属性自动下传
                # 证在当前这次请求里，后续发生的所有大模型调用、工具调用，
                # 都会自动打上这个脱敏后的 user_id 和 session_id，不需要你每次调工具都手动传参
                with propagate_attributes(**identity):
                    # 创建整条追踪链的总入口
                    # 后续所有的 model.call、skills.read 都是挂在它底下的子树枝。
                    self.root = self.client.start_observation(name='datamind.request', as_type='span')
                # 合规安全声明
                self.root.update(metadata={**identity,'resume': bool(resume), 'content_policy': 'metadata_only'})
            except Exception: self.root = None
        # 挂载 LangChain 监听器
        # 把当前这个 Trace 对象包装成 LangChain / LangGraph 的标准回调器（BaseCallbackHandler）
        self.callback = MetadataCallback(self)
    # 开辟一个子节点
    def start(self, name, parent=None, generation=False):
        if not self.root: return None
        try:
            return self.client.start_observation(name=name, as_type='generation' if generation else 'span',
                trace_context={'trace_id': self.root.trace_id, 'parent_span_id': (parent or self.root).id})
        except Exception: return None
    # 记录一个瞬间事件（即刻打点）,看内部start完后直接就end关闭span了,@measured也是瞬时打点,内部调用event,但是开启的都是根节点下第一层节点
    # 长事件只有两处,一处是traced_stream包裹的stream_chat_response,初始化Trace,创建根节点,内部会监听每一个yield出的sse事件,直到方法结束调用finish()关闭根节点
    # 还有一处就是回调器内部根据on_方法回调,此时遇到on_..._start调用start()开启子节点,此时不是根节点下第一层节点
    def event(self, name, **numbers):
        # 用event追踪的,默认都是根节点下的“第一层（一级子节点）”
        # 这里不传根节点的id,说明默认父节点就是根节点
        span = self.start(name)
        if span:
            try:
                span.update(metadata={k:v for k,v in numbers.items() if isinstance(v,(int,float,bool))})
                span.end()
            except Exception: pass
    # 整条链路的结束
    def finish(self, status):
        self.callback.close()
        if self.root:
            try:
                self.root.update(metadata={'status': status, 'duration_ms': (time.perf_counter()-self.started)*1000})
                self.root.end()
            except Exception: pass


def event(name, **numbers):
    trace = _current.get()
    # 属性是自定义的
    # 属性值只能传数值（int、float）或布尔值（bool）
    if trace: trace.event(name, **numbers)

# 计时打点
class SpanTimer:
    """可跨异步迭代手动闭合、也可直接 with / async with 的计时 Span。"""
    # 用的时候先start_span返回SpanTimer,然后with或者async with
    # 或者手动 调用finish关闭(建议)
    def __init__(self, span):
        self._span = span
        self._started = time.perf_counter()
        self._closed = False

    def finish(self, **numbers):
        if self._closed:
            return
        self._closed = True
        if self._span:
            try:
                meta = {k: v for k, v in numbers.items() if isinstance(v, (int, float, bool))}
                meta.setdefault("duration_ms", (time.perf_counter() - self._started) * 1000)
                self._span.update(metadata=meta)
                self._span.end()
            except Exception:
                pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.finish(failed=exc_type is not None)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.finish(failed=exc_type is not None)


def start_span(name, parent=None, generation=False) -> SpanTimer:
    """开启一个持续计时 Span（支持跨迭代 .finish(...) 或 async with 自动闭合）。"""
    trace = _current.get()
    span = trace.start(name, parent=parent, generation=generation) if trace else None
    return SpanTimer(span)

# def deco(func):
#     def wrapper(*args, **kwargs):
#         # 前置
#         result = func(*args, **kwargs)   # 调原函数
#         # 后置
#         return result
#     return wrapper
def measured(name):
    """Instrument internal operations without recording arguments or exceptions."""
    # 本质是一个自动化埋点打卡
    # 记录监控函数的执行耗时与成功率（APM 链路追踪）
    # 给 read() 加上 @measured('skills.read') 后，系统每次读取技能，都会自动把指标上报给链路追踪器：
    #   指标名：skills.read
    #   是否成功：success=True / False
    #   耗时：duration_ms=12.4（花了多少毫秒）
    # 运维和开发者在后台仪表盘（Dashboard）一眼就能看出：“最近 1 小时读取技能平均耗时 15ms，失败率 0%”。
    def decorate(fn):
        @functools.wraps(fn)
        def wrapped(*args, **kwargs):
            start=time.perf_counter()# 1. 记录开始时间
            try:
                result=fn(*args, **kwargs)
            except BaseException:
                # 2. 失败时，上报执行失败及耗时
                event(name, success=False, duration_ms=(time.perf_counter()-start)*1000)
                raise
            # 3. 成功时，上报执行成功及耗时
            event(name, success=True, duration_ms=(time.perf_counter()-start)*1000)
            return result
        return wrapped
    return decorate

# 让我讲讲invoke源码
# 首先上面我们把MetadataCallback放进了运行时配置config:RunnableConfig
# astream内部会调用get_callback_manager_for_config获得CallbackManager(回调管理器)实例callback_manager
# 这个类继承了BaseCallbackHandler,你可以把它理解为一个“容器”或“调度中心”
# 你的 MetadataCallback 是一个回调处理器（Handler）。
# 它的角色是被放进这个“容器”里，然后由这个管理器在合适的时机（比如链开始、LLM 结束时）负责调用。
# callback_manager一开始会调用on_chain_start,也就是我这里的on_chain_start
# 这会返回一个CallbackManagerForChainRun实例 - run_manager  ,这个你可以理解为专门为你这次链执行所生成的一个“专属子调度中心”
# 接着run_manager 会调用一个方法叫做get_child(),他会创建一个新的、以当前 run_id 作为 parent_run_id 的子管理器CallbackManager。
# 以child_config = patch_config(config, callbacks=run_manager.get_child())方式获得子配置
# 随后循环调用 step.invoke(input, 以child_config, ...)，把 child_config 传给序列里的下一步（比如一个 LLM 包装器）
# 然后看char_models文件里的stream方法,内部会用
# CallbackManager.configure(
#                 config.get("callbacks"),
#                 ...)
# 得到一个CallbackManager,这里的config就是child_config,重新配置得到一个为本次 ChatModel 调用构建一个专用的回调管理器
# 这个callbackManager,会作为子节点调用on_chat_model_start得到run_manager:CallbackManagerForLLMRun
# ...
class MetadataCallback(BaseCallbackHandler):
    """Custom callback deliberately does not pass messages, arguments or raw errors."""
    run_inline = True
    # 用spans一个字典记住所有正在运行的节点：{run_id: span}
    def __init__(self, trace): self.trace=trace; self.spans={}; self.lock=threading.RLock()
    # 下面的方法名都是固定的,必须一字不差
    # 每次大模型思考、每次工具执行，都严格遵循 “begin 登记 → 运行代码 → end 结账” 的闭环
    def begin(self, run_id, parent_run_id, name, generation=False):
        with self.lock:
            # self.spans.get(parent_run_id)：通过 parent_run_id 找出是谁触发了当前任务
            self.spans[run_id]=self.trace.start(name, self.spans.get(parent_run_id), generation)
    def end(self, run_id, failed=False, usage=None, model=None):
        with self.lock: span=self.spans.pop(run_id,None)
        if span:
            try:
                values={'metadata':{'failed':failed}}
                if usage:
                    values['usage_details']={dest:int(usage[k]) for k,dest in [('input_tokens','input'),('output_tokens','output'),('total_tokens','total')] if isinstance(usage.get(k),int)}
                if model and re.fullmatch(r'[a-zA-Z0-9._:/-]{1,80}',model): values['model']=model
                span.update(**values);span.end()
            except Exception: pass

    # 1. 步骤开始时：框架传进来 run_id="ID_001", parent_run_id=None
    def on_chain_start(self, serialized, inputs, *, run_id, parent_run_id=None, **kw):
        self.begin(run_id,parent_run_id,'agent.step')

    def on_chain_end(self, outputs, *, run_id, **kw): self.end(run_id)
    def on_chain_error(self, error, *, run_id, **kw): self.end(run_id,True)
    # 2. 调大模型时：框架传进来 run_id="ID_002", parent_run_id="ID_001"
    def on_chat_model_start(self, serialized, messages, *, run_id, parent_run_id=None, tags=None, **kw):
        self.begin(run_id,parent_run_id,'model.router' if 'review_router' in (tags or []) else 'model.call',True)
    def on_llm_start(self, serialized, prompts, *, run_id, parent_run_id=None, **kw): self.begin(run_id,parent_run_id,'model.call',True)
    def on_llm_end(self, response, *, run_id, **kw):
        usage=None;model=None
        for row in response.generations:
            for g in row:
                usage=getattr(getattr(g,'message',None),'usage_metadata',None) or usage
                model=(getattr(getattr(g,'message',None),'response_metadata',{}) or {}).get('model_name') or model
        self.end(run_id,usage=usage,model=model)
    def on_llm_error(self, error, *, run_id, **kw): self.end(run_id,True)
    # 3. 调工具时：框架传进来 run_id="ID_003", parent_run_id="ID_002"（或 "ID_001"）
    def on_tool_start(self, serialized, input_str, *, run_id, parent_run_id=None, **kw):
        # Names are code-owned in this project; no arbitrary plugin names exported.
        name=(serialized or {}).get('name','')
        allowed={
            'task','read_memory','remember','list_skills','read_skill','read_skill_resource','request_order_info',
            'inspect_schema_map','execute_readonly_sql','query_duckdb_olap','generate_echarts_schema',
            'submit_heavy_analytics_job','execute_writeback_sql','export_sensitive_data',
        }
        if name not in allowed and not (name.startswith(('part_','supplier_','inventory_','order_')) and name.replace('_','').isascii() and name.replace('_','').isalnum()): name='other'
        self.begin(run_id,parent_run_id,'tool.'+name)
    def on_tool_end(self, output, *, run_id, **kw):
        import json
        failed=getattr(output,'status',None)=='error'
        try:
            data=json.loads(getattr(output,'content',output))
            if isinstance(data,dict) and isinstance(data.get('code'),int):failed |= data['code']>=400
        except (ValueError,TypeError):pass
        self.end(run_id, failed)
    def on_tool_error(self, error, *, run_id, **kw): self.end(run_id,True)
    def close(self):
        for run_id in list(self.spans): self.end(run_id,True)


def traced_stream(fn):
    @functools.wraps(fn)
    async def wrapped(*args, **kwargs):
        import inspect
        bound=inspect.signature(fn).bind(*args,**kwargs);bound.apply_defaults()
        trace=Trace(bound.arguments['user_id'],bound.arguments['thread_id'],bound.arguments.get('resume_data') is not None)
        # 存context
        token=_current.set(trace);status='cancelled'
        try:
            async for chunk in fn(*args,**kwargs):
                # Only inspect event type/status; never send SSE data payloads.
                if 'event: error\n' in chunk: status='failed'
                if 'event: interrupt\n' in chunk or 'event: interrupted\n' in chunk: status='interrupted'
                if 'event: interrupt\n' in chunk: trace.event('hitl.interrupt')
                if 'event: token\n' in chunk and not trace.first_token:
                    trace.first_token=True;trace.event('request.first_token',duration_ms=(time.perf_counter()-trace.started)*1000)
                if 'event: done\n' in chunk and status not in ('failed','interrupted'):
                    status='interrupted' if '"interrupted": true' in chunk else 'completed'
                yield chunk
        except (asyncio.CancelledError, GeneratorExit):
            status='cancelled'
            raise
        except BaseException:
            status='failed'
            raise
        finally:
            trace.finish(status);_current.reset(token)
    return wrapped


def traced_config(config):
    trace=_current.get()
    if not trace or not trace.root: return config
    return {**config,'callbacks':[*config.get('callbacks',[]),trace.callback]}


async def shutdown():
    if _client:
        try: await asyncio.wait_for(asyncio.to_thread(_client.flush),3)
        except Exception: pass
