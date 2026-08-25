"""端到端自动化测试：fake_napcat + 真实 Codex app-server + 桥接器。

用法：
    .venv/Scripts/python tests/e2e.py [--config config.toml]
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import re
import sys
import time
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
LOGGER = logging.getLogger("e2e")

PROJECT_ROOT = Path(__file__).resolve().parent.parent


class ProcessWrapper:
    def __init__(self, name: str, proc: asyncio.subprocess.Process):
        self.name = name
        self.proc = proc
        self.lines: asyncio.Queue[str] = asyncio.Queue()
        self._tasks: list[asyncio.Task] = []

    @classmethod
    async def start(cls, name: str, cmd: list[str]) -> "ProcessWrapper":
        LOGGER.info("启动 %s: %s", name, " ".join(cmd))
        env = {**os.environ, "PYTHONIOENCODING": "utf-8"}
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            env=env,
        )
        self = cls(name, proc)
        self._tasks.append(asyncio.create_task(self._reader()))
        return self

    async def _reader(self) -> None:
        if self.proc.stdout is None:
            return
        while True:
            try:
                line = await self.proc.stdout.readline()
            except Exception:
                break
            if not line:
                break
            text = line.decode("utf-8", errors="replace").rstrip()
            if text:
                await self.lines.put(text)
                LOGGER.debug("[%s] %s", self.name, text)

    async def wait_for(self, pattern: str, timeout: float = 30.0) -> str:
        deadline = time.monotonic() + timeout
        regex = re.compile(pattern)
        while time.monotonic() < deadline:
            try:
                line = await asyncio.wait_for(self.lines.get(), timeout=deadline - time.monotonic())
            except asyncio.TimeoutError:
                break
            if regex.search(line):
                return line
        raise TimeoutError(f"等待 {self.name} 匹配 {pattern!r} 超时")

    def write_line(self, text: str) -> None:
        if self.proc.stdin is None:
            raise RuntimeError(f"{self.name} stdin 已关闭")
        self.proc.stdin.write((text + "\n").encode("utf-8"))
        asyncio.create_task(self._drain())

    async def _drain(self) -> None:
        try:
            await self.proc.stdin.drain()
        except Exception as exc:
            LOGGER.warning("drain 失败: %s", exc)

    async def shutdown(self) -> None:
        for t in self._tasks:
            t.cancel()
        if self.proc.stdin is not None:
            try:
                self.proc.stdin.close()
            except Exception:
                pass
        try:
            self.proc.terminate()
            await asyncio.wait_for(self.proc.wait(), timeout=5.0)
        except TimeoutError:
            self.proc.kill()
            await self.proc.wait()
        except Exception as exc:
            LOGGER.warning("关闭 %s 出错: %s", self.name, exc)


async def run_tests(config_path: str) -> int:
    fake = await ProcessWrapper.start("fake_napcat", [sys.executable, str(PROJECT_ROOT / "tests" / "fake_napcat.py"), "--config", config_path])
    results: dict[str, Any] = {"ok": True}
    try:
        await fake.wait_for("server listening on 127.0.0.1:", timeout=10.0)
        await asyncio.sleep(0.5)

        bridge = await ProcessWrapper.start("bridge", [sys.executable, "-m", "qq_codex_bridge", "--config", config_path])
        try:
            await fake.wait_for("桥接器已连接", timeout=10.0)
            await asyncio.sleep(0.5)

            replies: list[str] = []

            async def collect_replies(duration: float) -> None:
                deadline = time.monotonic() + duration
                while time.monotonic() < deadline:
                    try:
                        line = await asyncio.wait_for(fake.lines.get(), timeout=0.5)
                    except asyncio.TimeoutError:
                        continue
                    m = re.search(r"\[机器人回复 \d+\]: (.*)", line)
                    if m:
                        replies.append(m.group(1))

            def send(text: str) -> None:
                LOGGER.info("send: %s", text)
                fake.write_line(text)

            # 显式指定测试模型
            send("/model gpt-5.6-luna")
            await asyncio.sleep(0.5)

            # 1. 普通消息
            send("你好，请只回复两个字：收到")
            await asyncio.wait_for(collect_replies(25.0), timeout=30.0)
            results["normal_reply"] = replies[-1] if replies else None
            LOGGER.info("normal_reply ok=%s", bool(replies))

            # 2. /list
            replies.clear()
            send("/list")
            await asyncio.wait_for(collect_replies(3.0), timeout=5.0)
            results["list_reply_count"] = len(replies)
            LOGGER.info("list_reply_count=%d", len(replies))

            # 3. /model
            replies.clear()
            send("/model")
            await asyncio.wait_for(collect_replies(10.0), timeout=15.0)
            results["model_reply"] = replies[-1][:200] if replies else None
            LOGGER.info("model_reply ok=%s", bool(replies))

            # 4. /project playground
            replies.clear()
            send("/project playground")
            await asyncio.wait_for(collect_replies(8.0), timeout=10.0)
            results["project_reply"] = replies[-1][:200] if replies else None
            LOGGER.info("project_reply ok=%s", bool(replies))

            # 5. /new
            replies.clear()
            send("/new")
            await asyncio.wait_for(collect_replies(8.0), timeout=10.0)
            results["new_reply"] = replies[-1][:200] if replies else None
            LOGGER.info("new_reply ok=%s", bool(replies))

            # 6. 排队行为：连续发两条
            replies.clear()
            send("请只回复两个字：第一条")
            await asyncio.sleep(0.2)
            send("请只回复两个字：第二条")
            await asyncio.wait_for(collect_replies(35.0), timeout=40.0)
            results["queue_reply_count"] = len(replies)
            LOGGER.info("queue_reply_count=%d", len(replies))

            # 7. 触发审批（尝试文件写入）
            replies.clear()
            send("请在当前目录新建一个 test_approval.txt 文件，写入 hello approval")
            await asyncio.wait_for(collect_replies(30.0), timeout=35.0)
            approval_prompt = any("请求批准" in r for r in replies)
            results["approval_prompt_triggered"] = approval_prompt
            LOGGER.info("approval_prompt_triggered=%s", approval_prompt)
            if approval_prompt:
                replies.clear()
                send("/yes")
                await asyncio.wait_for(collect_replies(25.0), timeout=30.0)
                results["after_yes_reply"] = replies[-1][:300] if replies else None
                LOGGER.info("after_yes_reply ok=%s", bool(replies))

            LOGGER.info("e2e done")
            return 0
        finally:
            await bridge.shutdown()
    finally:
        fake.write_line("quit")
        await asyncio.sleep(0.5)
        await fake.shutdown()
        # 写入结果文件，避免终端编码问题
        with open("e2e_results.json", "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2)


def main() -> int:
    parser = argparse.ArgumentParser(description="End-to-end test")
    parser.add_argument("--config", default="config.toml")
    args = parser.parse_args()
    return asyncio.run(run_tests(args.config))


if __name__ == "__main__":
    raise SystemExit(main())
