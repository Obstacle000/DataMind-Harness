"""AgentLoader 异步并发与双检锁安全单元测试"""
from __future__ import annotations

import asyncio
import time
import pytest


@pytest.mark.concurrency
@pytest.mark.unit
class TestAgentLoaderConcurrency:
    def test_same_user_concurrent_get_session_builds_once(
        self, isolated_agent_loader, monkeypatch
    ):
        """同一 user_id 并发请求 get_session：无死锁，且底层构建只执行 1 次"""
        build_count = 0

        async def fake_ensure_resources():
            return None

        def fake_build_agent(user_id: str, username: str):
            nonlocal build_count
            time.sleep(0.05)  # 模拟耗时的 Docker/Agent 构建
            build_count += 1
            return f"fake-agent-{user_id}"

        monkeypatch.setattr(isolated_agent_loader, "_ensure_resources", fake_ensure_resources)
        monkeypatch.setattr(isolated_agent_loader, "_build_agent_for_user", fake_build_agent)

        async def run():
            return await asyncio.gather(
                isolated_agent_loader.get_session("user_a", "张三"),
                isolated_agent_loader.get_session("user_a", "张三"),
            )

        s1, s2 = asyncio.run(run())
        assert s1 is s2
        assert build_count == 1

    def test_different_users_get_isolated_sessions(
        self, isolated_agent_loader, monkeypatch
    ):
        """不同 user_id 各自拥有独立的 AgentSession 与独立的 run_lock"""
        async def fake_ensure_resources():
            return None

        monkeypatch.setattr(isolated_agent_loader, "_ensure_resources", fake_ensure_resources)
        monkeypatch.setattr(
            isolated_agent_loader,
            "_build_agent_for_user",
            lambda uid, uname: f"fake-agent-{uid}",
        )

        async def run():
            return await asyncio.gather(
                isolated_agent_loader.get_session("user_a", "张三"),
                isolated_agent_loader.get_session("user_b", "李四"),
            )

        sa, sb = asyncio.run(run())
        assert sa is not sb
        assert sa.run_lock is not sb.run_lock
        assert sa.agent == "fake-agent-user_a"
        assert sb.agent == "fake-agent-user_b"
