"""第二批功能端到端测试：队列管理、/interrupt、持久化。"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from _common import ProcessWrapper, collect_replies

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
LOGGER = logging.getLogger("e2e_batch2")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
MARKER = Path.home() / "approval_test_marker.txt"


def clean_marker() -> None:
    if os.path.exists(MARKER):
        os.remove(MARKER)


async def wait_reply_contains(fake: ProcessWrapper, replies: list[str], substring: str, timeout: float = 15.0) -> bool:
    """等待 replies 中出现包含 substring 的回复。"""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        await collect_replies(fake, 1.0, replies)
        if any(substring in r for r in replies):
            return True
    return False


async def start_bridge(config_path: str) -> ProcessWrapper:
    return await ProcessWrapper.start(
        "bridge",
        [sys.executable, "-m", "qq_codex_bridge", "--config", config_path],
        cwd=PROJECT_ROOT,
    )


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

        bridge = await start_bridge(config_path)
        try:
            await fake.wait_for("桥接器已连接", timeout=10.0)
            await asyncio.sleep(0.5)

            def send(text: str) -> None:
                LOGGER.info("send: %s", text)
                fake.write_line(text)

            # 显式指定测试模型
            send("/model gpt-5.6-luna")
            await asyncio.sleep(0.5)

            replies: list[str] = []

            # 1. 队列命令测试
            LOGGER.info("=== step 1: queue commands ===")
            send("queue msg A")
            await asyncio.sleep(1.0)  # 给 Codex 启动 turn
            send("queue msg B")
            await wait_reply_contains(fake, replies, "已排队")

            replies.clear()
            send("/queue list")
            await collect_replies(fake, 3.0, replies)
            results["queue_list_shows_b"] = any("queue msg B" in r for r in replies)
            LOGGER.info("queue_list_shows_b=%s", results["queue_list_shows_b"])

            # 清理并等待 A/B 处理完
            await wait_reply_contains(fake, replies, "queue msg B")
            replies.clear()
            send("/queue clear")
            await collect_replies(fake, 3.0, replies)

            # jump：在 busy 时插队
            replies.clear()
            send("queue msg C")
            await asyncio.sleep(0.3)
            send("/queue jump urgent msg")
            await asyncio.sleep(0.2)

            replies.clear()
            send("/queue list")
            await collect_replies(fake, 3.0, replies)
            lines = [r for r in replies if r.strip().startswith(("1.", "2.", "3."))]
            results["queue_jump_first"] = len(lines) >= 1 and lines[0].startswith("1. urgent")
            LOGGER.info("queue_jump_first=%s", results["queue_jump_first"])

            replies.clear()
            send("/queue clear")
            await collect_replies(fake, 3.0, replies)

            # pop / clear：构造两条队列消息
            replies.clear()
            send("queue msg D")
            await asyncio.sleep(0.2)
            send("queue msg E")
            await asyncio.sleep(0.2)
            send("queue msg F")
            await asyncio.sleep(0.2)

            replies.clear()
            send("/queue list")
            await asyncio.sleep(0.2)
            send("/queue pop 2")
            await collect_replies(fake, 3.0, replies)
            lines = [r for r in replies if r.strip().startswith(("1.", "2.", "3."))]
            results["queue_has_two_after_d"] = len(lines) >= 2
            LOGGER.info("queue_has_two_after_d=%s", results["queue_has_two_after_d"])
            results["queue_pop_ok"] = any("已删除队列第 2 条" in r for r in replies)
            LOGGER.info("queue_pop_ok=%s", results["queue_pop_ok"])

            replies.clear()
            send("/queue clear")
            await collect_replies(fake, 3.0, replies)
            results["queue_clear_ok"] = any("已清空队列" in r for r in replies)
            LOGGER.info("queue_clear_ok=%s", results["queue_clear_ok"])

            # 2. 持久化：在 turn 进行中加入队列，立即重启
            LOGGER.info("=== step 2: queue persistence across restart ===")
            replies.clear()
            send("persist A")
            await asyncio.sleep(1.0)
            send("persist B")
            await wait_reply_contains(fake, replies, "已排队")

            LOGGER.info("stopping bridge...")
            await bridge.shutdown()
            await asyncio.sleep(1.0)

            LOGGER.info("restarting bridge...")
            bridge = await start_bridge(config_path)
            await fake.wait_for("桥接器已连接", timeout=10.0)
            await asyncio.sleep(0.5)

            replies.clear()
            send("/queue list")
            await collect_replies(fake, 3.0, replies)
            lines = [r for r in replies if r.strip().startswith(("1.", "2."))]
            results["queue_persisted"] = len(lines) >= 1 and any("persist" in r for r in lines)
            LOGGER.info("queue_persisted=%s", results["queue_persisted"])

            # 3. /interrupt 测试
            LOGGER.info("=== step 3: /interrupt while busy ===")
            send("/queue clear")
            await asyncio.sleep(0.3)
            clean_marker()

            replies.clear()
            send("please count to 100 slowly")
            await asyncio.sleep(0.5)
            before = len(replies)
            send("/interrupt interrupt processed")
            await collect_replies(fake, 25.0, replies)
            results["interrupt_reply_seen"] = len(replies) > before
            results["interrupt_marker_not_created"] = not os.path.exists(MARKER)
            LOGGER.info("interrupt_reply_seen=%s", results["interrupt_reply_seen"])

            send("/queue clear")
            clean_marker()
            return results
        finally:
            await bridge.shutdown()
    finally:
        fake.write_line("quit")
        await asyncio.sleep(0.5)
        await fake.shutdown()


async def main() -> int:
    parser = argparse.ArgumentParser(description="Batch2 e2e test")
    parser.add_argument("--config", default="config.toml")
    args = parser.parse_args()

    results = await run_tests(args.config)
    path = Path("e2e_batch2_results.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=True, indent=2)
    LOGGER.info("results written to %s", path)
    print(json.dumps(results, ensure_ascii=True, indent=2))

    ok = all(v for v in results.values() if isinstance(v, bool))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
