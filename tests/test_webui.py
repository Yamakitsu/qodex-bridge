"""WebUI API / WS 端到端测试。"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import re
import shutil
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

import aiohttp

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from _common import ProcessWrapper

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
LOGGER = logging.getLogger("test_webui")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
MARKER = Path.home() / "approval_test_marker.txt"


def clean_marker() -> None:
    if os.path.exists(MARKER):
        os.remove(MARKER)


async def wait_for_webui(bridge: ProcessWrapper, timeout: float = 30.0) -> tuple[str, int]:
    """从 bridge stdout 提取 WebUI URL 与端口。"""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            line = await asyncio.wait_for(bridge.lines.get(), timeout=deadline - time.monotonic())
        except asyncio.TimeoutError:
            break
        m = re.search(r"WebUI:\s+(http://[\d\.]+:(\d+))", line)
        if m:
            return m.group(1), int(m.group(2))
    raise TimeoutError("未等到 WebUI 启动日志")


async def read_token(timeout: float = 10.0) -> str:
    path = PROJECT_ROOT / "data" / "server.token"
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            return path.read_text(encoding="utf-8").strip()
        await asyncio.sleep(0.3)
    raise TimeoutError("未生成 server.token")


async def run_tests(config_path: str) -> dict[str, Any]:
    clean_marker()
    results: dict[str, Any] = {"ok": True}

    fake = await ProcessWrapper.start(
        "fake_napcat",
        [sys.executable, str(PROJECT_ROOT / "tests" / "fake_napcat.py"), "--config", config_path],
        cwd=PROJECT_ROOT,
    )
    try:
        await fake.wait_for("server listening on 127.0.0.1:", timeout=10.0)
        await asyncio.sleep(0.5)

        bridge = await ProcessWrapper.start(
            "bridge",
            [sys.executable, "-m", "qq_codex_bridge", "--config", config_path],
            cwd=PROJECT_ROOT,
        )
        try:
            url, port = await wait_for_webui(bridge)
            token = await read_token()
            LOGGER.info("WebUI url=%s token=%s...", url, token[:8])
            results["webui_port"] = port

            base = f"http://127.0.0.1:{port}/api/v1"
            headers = {"Authorization": f"Bearer {token}"}

            async with aiohttp.ClientSession() as session:
                # 显式指定测试模型并确保处于 safe 模式
                async with session.post(f"{base}/model", headers=headers, json={"name": "gpt-5.6-luna"}) as r:
                    LOGGER.info("set model status=%s", r.status)
                async with session.post(f"{base}/mode", headers=headers, json={"mode": "safe"}) as r:
                    LOGGER.info("set safe mode status=%s", r.status)
                # 1. 无 token 401
                async with session.get(f"{base}/status") as r:
                    results["unauthorized_401"] = r.status == 401
                    LOGGER.info("no token status=%s", r.status)

                # 2. 有 token GET status 200
                async with session.get(f"{base}/status", headers=headers) as r:
                    results["status_200"] = r.status == 200
                    body = await r.json()
                    LOGGER.info("status=%s project=%s", r.status, body.get("project"))

                # 3. GET index 200
                async with session.get(f"http://127.0.0.1:{port}/", headers=headers) as r:
                    results["index_200"] = r.status == 200

                # 4. WS 连接 + prompt 收到回复
                ws_events: list[dict] = []
                async with session.ws_connect(f"{base}/ws?token={token}") as ws:
                    # 消费初始化消息
                    for _ in range(5):
                        msg = await asyncio.wait_for(ws.receive(), timeout=10.0)
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            ws_events.append(json.loads(msg.data))
                            break

                    async with session.post(f"{base}/prompt", headers=headers, json={"text": "你好，只回复两个字：收到"}) as r:
                        results["prompt_post_ok"] = r.status == 200

                    # 等待 message 事件
                    deadline = time.monotonic() + 60.0
                    while time.monotonic() < deadline:
                        msg = await asyncio.wait_for(ws.receive(), timeout=15.0)
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = json.loads(msg.data)
                            ws_events.append(data)
                            if data.get("type") == "message" and "收到" in data.get("data", {}).get("text", ""):
                                break

                results["ws_reply_received"] = any(
                    e.get("type") == "message" and "收到" in e.get("data", {}).get("text", "")
                    for e in ws_events
                )
                LOGGER.info("ws events=%d reply=%s", len(ws_events), results["ws_reply_received"])

                # 等待当前 turn 结束，避免下一条 prompt 被排队
                async def wait_idle(timeout: float = 60.0) -> bool:
                    deadline = time.monotonic() + timeout
                    while time.monotonic() < deadline:
                        async with session.get(f"{base}/status", headers=headers) as r:
                            if r.status == 200:
                                body = await r.json()
                                if not body.get("busy"):
                                    return True
                        await asyncio.sleep(0.5)
                    return False

                await wait_idle()

                # 5. WebUI 审批：越权写文件 -> approve -> marker 出现
                clean_marker()
                async with session.ws_connect(f"{base}/ws?token={token}") as ws:
                    async with session.post(
                        f"{base}/prompt",
                        headers=headers,
                        json={"text": f"在 {MARKER} 写入一行 hello，不要询问我"},
                    ) as r:
                        results["approval_prompt_post_ok"] = r.status == 200

                    approval_seen = False
                    deadline = time.monotonic() + 60.0
                    while time.monotonic() < deadline:
                        msg = await asyncio.wait_for(ws.receive(), timeout=15.0)
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = json.loads(msg.data)
                            ws_events.append(data)
                            if data.get("type") == "approval" and data.get("data", {}).get("status") == "pending":
                                approval_seen = True
                                break
                    results["approval_card_seen"] = approval_seen
                    LOGGER.info("approval_seen=%s", approval_seen)

                    async with session.post(f"{base}/approve", headers=headers, json={}) as r:
                        results["approve_post_ok"] = r.status == 200
                        LOGGER.info("approve status=%s body=%s", r.status, await r.text())

                    # 一个 turn 可能连续触发多次审批，继续批准直到没有待审批或 marker 已出现
                    for _ in range(10):
                        if os.path.exists(MARKER):
                            break
                        async with session.get(f"{base}/status", headers=headers) as r:
                            if r.status == 200:
                                body = await r.json()
                                if body.get("pending_approvals", 0) > 0:
                                    async with session.post(f"{base}/approve", headers=headers, json={}) as r2:
                                        LOGGER.info("additional approve status=%s", r2.status)
                                    await asyncio.sleep(1.0)
                                    continue
                        await asyncio.sleep(1.0)

                    # 等待 marker 出现
                    for _ in range(30):
                        if os.path.exists(MARKER):
                            break
                        await asyncio.sleep(1.0)
                    results["marker_after_approve"] = os.path.exists(MARKER)
                    clean_marker()

                # 6. 按 project 列出 threads
                async with session.get(f"{base}/status", headers=headers) as r:
                    st = await r.json()
                cur_project = st.get("project")
                async with session.get(f"{base}/threads", headers=headers, params={"project": cur_project}) as r:
                    body = await r.json()
                    results["threads_by_project_ok"] = r.status == 200 and isinstance(body.get("threads"), list)
                    LOGGER.info("threads?project=%s -> %d 条", cur_project, len(body.get("threads") or []))

                # 7. 新建 project（临时目录），验证后从 overlay 清理
                tmp_dir = tempfile.mkdtemp(prefix="webui_test_proj_")
                proj_name = "webui-test-tmp"
                async with session.post(f"{base}/projects", headers=headers, json={"name": proj_name, "path": tmp_dir}) as r:
                    body = await r.json()
                    results["create_project_ok"] = r.status == 200 and body.get("ok") is True
                    LOGGER.info("create project: %s", body)
                async with session.get(f"{base}/status", headers=headers) as r:
                    body = await r.json()
                    results["project_in_status"] = proj_name in (body.get("projects") or {})
                # 清理 overlay，避免污染真实 data/projects.json
                overlay_path = PROJECT_ROOT / "data" / "projects.json"
                try:
                    overlay = json.loads(overlay_path.read_text(encoding="utf-8"))
                    overlay.pop(proj_name, None)
                    overlay_path.write_text(json.dumps(overlay, ensure_ascii=False, indent=2), encoding="utf-8")
                except Exception as exc:
                    LOGGER.warning("清理 overlay 失败: %s", exc)
                shutil.rmtree(tmp_dir, ignore_errors=True)

                # 8. resume 当前 thread 应返回 history 字段
                cur_thread = st.get("thread")
                if cur_thread:
                    async with session.post(f"{base}/thread", headers=headers, json={"id": cur_thread}) as r:
                        body = await r.json()
                        results["thread_history_field"] = (
                            r.status == 200 and body.get("ok") is True and isinstance(body.get("history"), list)
                        )
                        LOGGER.info("resume thread ok=%s history=%d 条", body.get("ok"), len(body.get("history") or []))
                else:
                    results["thread_history_field"] = False
                    LOGGER.warning("当前无 thread，跳过 history 检查")

            return results
        finally:
            await bridge.shutdown()
    finally:
        fake.write_line("quit")
        await asyncio.sleep(0.5)
        await fake.shutdown()


async def main() -> int:
    parser = argparse.ArgumentParser(description="WebUI test")
    parser.add_argument("--config", default="config.toml")
    args = parser.parse_args()

    results = await run_tests(args.config)
    path = Path("test_webui_results.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=True, indent=2)
    LOGGER.info("results written to %s", path)
    print(json.dumps(results, ensure_ascii=True, indent=2))

    ok = all(v for v in results.values() if isinstance(v, bool))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
