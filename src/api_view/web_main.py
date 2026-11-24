"""
DataMind-Harness FastAPI 应用入口
CORS 配置、路由注册、沙箱预热池维护与生命周期管理
"""
import sys
import os
import asyncio
from pathlib import Path
from contextlib import asynccontextmanager, suppress

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

from .api.chat import router as chat_router
from .api.history import router as history_router
from .web_config import close_mongo_client
from ..agent.log_utils import web_logger
from ..agent.backends.sandbox_manager import sandbox_manager
from ..agent.clients.duckdb_client import duckdb_client
from ..agent.clients.schema_map import schema_map_engine
from ..agent.config import SANDBOX_MAINTENANCE_INTERVAL_SECONDS


async def _sandbox_maintenance(stop_event: asyncio.Event) -> None:
    """后台补齐预热池、回收空闲容器。"""
    while not stop_event.is_set():
        try:
            await asyncio.to_thread(sandbox_manager.cleanup_idle_sandboxes)
            await asyncio.to_thread(sandbox_manager.ensure_warm_pool)
        except Exception as exc:
            web_logger.warning(f"Sandbox maintenance iteration failed: {exc}")

        try:
            await asyncio.wait_for(
                stop_event.wait(), timeout=SANDBOX_MAINTENANCE_INTERVAL_SECONDS
            )
        except TimeoutError:
            pass


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    web_logger.info("Starting DataMind-Harness Web Server...")
    stop_event = asyncio.Event()
    maintenance_task = None
    try:
        # 离线第一阶段预热：服务启动时在后台线程完成 Schema 元数据加载与三层漏斗血缘建图，
        # 预热完成后线上请求直接命中 MD5 指纹 0ms 短路，避免首个用户请求冷启动卡顿
        try:
            schema = await asyncio.to_thread(schema_map_engine.load_schema)
            web_logger.info(
                f"Schema-Map Phase 1 warmed up: {len(schema)} tables, "
                f"{len(schema_map_engine._relationships)} lineage relationships."
            )
        except Exception as exc:
            web_logger.warning(f"Schema-Map startup warmup skipped due to error: {exc}")

        # DuckDB 零拷贝只读挂载 PostgreSQL（若 PG 在线则直查真实数仓；离线则自动回退内置 8 张演示表）
        try:
            pg_attached = await asyncio.to_thread(duckdb_client.attach_postgres)
            web_logger.info(f"DuckDB OLAP ready (pg_attached={pg_attached})")
        except Exception as exc:
            web_logger.warning(f"DuckDB PostgreSQL attach skipped: {exc}")

        await asyncio.to_thread(sandbox_manager.ensure_warm_pool)
        maintenance_task = asyncio.create_task(
            _sandbox_maintenance(stop_event), name="sandbox-maintenance"
        )
        yield
    finally:
        web_logger.info("Shutting down DataMind-Harness Web Server...")
        stop_event.set()
        if maintenance_task is not None:
            maintenance_task.cancel()
            with suppress(asyncio.CancelledError):
                await maintenance_task
        await close_mongo_client()
        from ..agent.observability import shutdown
        await shutdown()


app = FastAPI(
    title="DataMind-Harness 智能 BI 与数据科学 Agent",
    description="基于 Harness Engineering 控制流治理架构的自愈型智能 BI、NL2SQL 与生成式可视化 API",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(chat_router)
app.include_router(history_router)

DOWNLOAD_DIR = Path(__file__).resolve().parent.parent / "download"
DOWNLOAD_DIR.mkdir(exist_ok=True)


@app.get("/api/download/{filename}")
async def download_file(filename: str):
    """提供沙箱生成文件（图表、导出数据、分析报告）的 HTTP 下载"""
    file_path = DOWNLOAD_DIR / filename
    if not file_path.exists() or not file_path.is_file():
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail=f"文件不存在: {filename}")
    return FileResponse(
        path=str(file_path),
        filename=filename,
        media_type="application/octet-stream",
    )


@app.get("/")
async def root():
    return {"message": "DataMind-Harness 智能 BI 与数据科学 Agent API", "version": "1.0.0"}


@app.get("/health")
async def health():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
