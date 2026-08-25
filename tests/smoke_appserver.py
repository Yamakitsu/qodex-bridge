"""直接对 Codex app-server 做冒烟测试。"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from pathlib import Path

# 允许从仓库根目录直接运行
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from qq_codex_bridge.appserver import AppServerClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
LOGGER = logging.getLogger("smoke")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PLAYGROUND = PROJECT_ROOT / "playground"
CODEX_PATH = os.environ.get("CODEX_PATH", "codex")


async def main() -> int:
    events: list[dict] = []
    agent_texts: list[str] = []
    turn_completed = asyncio.Event()

    def on_notification(method: str, params: dict) -> None:
        events.append({"method": method, "params": params})
        if method == "item/completed":
            item = params.get("item", {})
            if item.get("type") == "agentMessage":
                text = item.get("text", "")
                if text:
                    agent_texts.append(text)
                    LOGGER.info("agentMessage: %r", text)
        if method == "turn/completed":
            turn_completed.set()

    client = AppServerClient(
        CODEX_PATH,
        on_notification=on_notification,
        on_server_request=lambda _m, _i, _p: {"decision": "decline"},
    )

    try:
        await client.start()

        LOGGER.info("创建临时 thread...")
        result = await client.call(
            "thread/start",
            ephemeral=True,
            cwd=str(PLAYGROUND.resolve()),
            approvalPolicy="never",
            sandboxPolicy={
                "type": "workspaceWrite",
                "writableRoots": [str(PLAYGROUND.resolve())],
                "networkAccess": True,
            },
        )
        thread_id = result["thread"]["id"]
        LOGGER.info("thread id=%s", thread_id)

        LOGGER.info("发送 turn...")
        await client.call(
            "turn/start",
            threadId=thread_id,
            input=[{"type": "text", "text": "只回复两个字：你好"}],
            cwd=str(PLAYGROUND.resolve()),
            approvalPolicy="never",
            sandboxPolicy={
                "type": "workspaceWrite",
                "writableRoots": [str(PLAYGROUND.resolve())],
                "networkAccess": True,
            },
        )

        LOGGER.info("等待 turn/completed...")
        await asyncio.wait_for(turn_completed.wait(), timeout=120.0)

        final_text = "".join(agent_texts)
        LOGGER.info("最终 agent 文本: %r", final_text)

        # 简单断言
        if "你好" not in final_text:
            LOGGER.error("未在回复中找到 '你好'")
            return 1

        LOGGER.info("冒烟测试通过。事件数量: %d", len(events))
        return 0
    finally:
        await client.shutdown()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
