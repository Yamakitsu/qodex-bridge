"""审批路径端到端测试：fake_napcat + 真实 Codex app-server。

通过让 Codex 在 playground 之外写文件，逼出 approval 请求，
验证 /no 拒绝、/yes 放行，以及 /mode full 不再询问。
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
from typing import Any, Callable

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import aiohttp

from qq_codex_bridge.config import load_config

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
LOGGER = logging.getLogger("approval_e2e")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
MARKER = Path.home() / "approval_test_marker.txt"


class ProcessWrapper:
    def __init__(self, name: str, proc: asyncio.subprocess.Process):
        self.name = name
        self.proc = proc
        self.lines: asyncio.Queue[str] = asyncio.Queue()
        self._tasks: list[asyncio.Task] = []

    @classmethod
    async def start(cls, name: str, cmd: list[str]) -> "ProcessWrapper":
        LOGGER.info("start %s: %s", name, " ".join(cmd))
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
            line = await self.proc.stdout.readline()
            if not line:
                break
            text = line.decode("utf-8", errors="replace").rstrip()
            if text:
                await self.lines.put(text)
                LOGGER.info("[%s] %s", self.name, text)

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
        raise TimeoutError(f"wait {self.name} for {pattern!r} timeout")

    def write_line(self, text: str) -> None:
        if self.proc.stdin is None:
            raise RuntimeError(f"{self.name} stdin closed")
        self.proc.stdin.write((text + "\n").encode("utf-8"))
        asyncio.create_task(self._drain())

    async def _drain(self) -> None:
        try:
            await self.proc.stdin.drain()
        except Exception as exc:
            LOGGER.warning("drain failed: %s", exc)

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
            LOGGER.warning("shutdown %s error: %s", self.name, exc)


def marker_exists() -> bool:
    return os.path.exists(MARKER)


def clean_marker() -> None:
    if marker_exists():
        os.remove(MARKER)


async def collect_replies(
    fake: ProcessWrapper,
    duration: float,
    replies: list[str],
) -> None:
    deadline = time.monotonic() + duration
    while time.monotonic() < deadline:
        try:
            line = await asyncio.wait_for(fake.lines.get(), timeout=0.5)
        except asyncio.TimeoutError:
            continue
        m = re.search(r"\[机器人回复 \d+\]: (.*)", line)
        if m:
            replies.append(m.group(1))


def has_approval_prompt(replies: list[str]) -> bool:
    return any("请求批准" in r or "请求额外权限" in r for r in replies)


def approval_prompts(replies: list[str]) -> list[str]:
    return [r for r in replies if "请求批准" in r or "请求额外权限" in r]


def is_permissions_prompt(text: str) -> bool:
    return "请求额外权限" in text


async def answer_approval_prompts(
    fake: ProcessWrapper,
    replies: list[str],
    send: Callable[[str], None],
    accept_permissions: bool = True,
) -> int:
    """回答当前 turn 弹出的所有审批提示，返回回答次数。

    遇到 command/file 审批统一 /yes；遇到 permissions 审批则按 accept_permissions 决定。
    若 decline permissions，回答后立刻等待 turn 结束。
    """
    answered = 0
    for _ in range(5):
        await collect_replies(fake, 15.0, replies)
        pending = approval_prompts(replies[answered:])
        if not pending:
            break
        for prompt in pending:
            answered += 1
            LOGGER.info("approval prompt (%s): %.80s", "permissions" if is_permissions_prompt(prompt) else "other", prompt)
            if is_permissions_prompt(prompt):
                send("/yes" if accept_permissions else "/no")
                if not accept_permissions:
                    await wait_for_turn_settled(fake, replies)
                    return answered
            else:
                send("/yes")
            await asyncio.sleep(2.0)
    await wait_for_turn_settled(fake, replies)
    return answered


async def read_token(timeout: float = 10.0) -> str:
    path = PROJECT_ROOT / "data" / "server.token"
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            return path.read_text(encoding="utf-8").strip()
        await asyncio.sleep(0.3)
    raise TimeoutError("未生成 server.token")


async def wait_for_webui(bridge: ProcessWrapper, timeout: float = 30.0) -> int:
    """从 bridge stdout 提取 WebUI 实际端口。"""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            line = await asyncio.wait_for(bridge.lines.get(), timeout=deadline - time.monotonic())
        except asyncio.TimeoutError:
            break
        m = re.search(r"WebUI:\s+http://[\d\.]+:(\d+)", line)
        if m:
            return int(m.group(1))
    raise TimeoutError("未等到 WebUI 启动日志")


async def wait_idle(token: str, port: int = 8765, timeout: float = 60.0) -> bool:
    """轮询 WebUI /status 直到不忙。"""
    headers = {"Authorization": f"Bearer {token}"}
    deadline = time.monotonic() + timeout
    async with aiohttp.ClientSession() as session:
        while time.monotonic() < deadline:
            try:
                async with session.get(f"http://127.0.0.1:{port}/api/v1/status", headers=headers) as r:
                    if r.status == 200:
                        body = await r.json()
                        if not body.get("busy"):
                            return True
            except Exception as exc:
                LOGGER.debug("wait_idle error: %s", exc)
            await asyncio.sleep(0.5)
    return False


async def wait_for_turn_settled(
    fake: ProcessWrapper,
    existing_replies: list[str],
    max_wait: float = 45.0,
) -> None:
    """等待约 5 秒没有新的机器人回复，认为 turn 结束。"""
    start = time.monotonic()
    last_count = len(existing_replies)
    while time.monotonic() - start < max_wait:
        await asyncio.sleep(5.0)
        await collect_replies(fake, 1.0, existing_replies)
        if len(existing_replies) == last_count:
            return
        last_count = len(existing_replies)


async def run_test(config_path: str) -> dict[str, Any]:
    clean_marker()
    results: dict[str, Any] = {"ok": True, "marker": MARKER}

    fake = await ProcessWrapper.start("fake_napcat", [sys.executable, str(PROJECT_ROOT / "tests" / "fake_napcat.py"), "--config", config_path])
    try:
        await fake.wait_for("server listening on 127.0.0.1:", timeout=10.0)
        await asyncio.sleep(0.5)

        bridge = await ProcessWrapper.start("bridge", [sys.executable, "-m", "qq_codex_bridge", "--config", config_path])
        try:
            await fake.wait_for("桥接器已连接", timeout=10.0)
            await asyncio.sleep(0.5)

            config = load_config(config_path)
            token = await read_token()
            webui_port = await wait_for_webui(bridge)

            def send(text: str) -> None:
                LOGGER.info("send: %s", text)
                fake.write_line(text)

            # 显式指定测试模型并确保 safe 模式
            send("/model gpt-5.6-luna")
            await asyncio.sleep(0.5)
            send("/mode safe")
            await asyncio.sleep(0.5)

            # ------------------ 第一次：逼出审批，/no 拒绝 ------------------
            LOGGER.info("=== step 1: trigger approval and decline ===")
            replies: list[str] = []
            send(f"在 {MARKER} 写入一行 hello，不要询问我")
            await collect_replies(fake, 20.0, replies)

            if not has_approval_prompt(replies):
                # 再等等
                await collect_replies(fake, 20.0, replies)

            results["step1_approval_prompt"] = has_approval_prompt(replies)
            LOGGER.info("approval prompt present: %s", results["step1_approval_prompt"])

            if results["step1_approval_prompt"]:
                send("/no")
                await wait_for_turn_settled(fake, replies)

            results["step1_marker_exists_after_no"] = marker_exists()
            LOGGER.info("marker exists after /no: %s", results["step1_marker_exists_after_no"])

            # ------------------ 第二次：再次触发，/yes 放行 ------------------
            LOGGER.info("=== step 2: trigger approval and accept ===")
            clean_marker()
            replies.clear()
            send(f"在 {MARKER} 写入一行 hello，不要询问我")
            await collect_replies(fake, 20.0, replies)

            results["step2_approval_prompt"] = has_approval_prompt(replies)
            LOGGER.info("approval prompt present: %s", results["step2_approval_prompt"])

            if results["step2_approval_prompt"]:
                answered = await answer_approval_prompts(fake, replies, send, accept_permissions=True)
                LOGGER.info("answered approvals: %d", answered)

            results["step2_marker_exists_after_yes"] = marker_exists()
            LOGGER.info("marker exists after /yes: %s", results["step2_marker_exists_after_yes"])

            # 等待当前 turn 真正结束，避免后续命令被排队
            await wait_idle(token, webui_port)

            # ------------------ 第三次：/mode full 不再询问 ------------------
            LOGGER.info("=== step 3: /mode full should skip approval ===")
            clean_marker()
            replies.clear()
            send("/mode full")
            await collect_replies(fake, 3.0, replies)
            send("确认")
            await collect_replies(fake, 3.0, replies)

            await wait_idle(token, webui_port)

            replies.clear()
            send(f"在 {MARKER} 写入一行 hello")
            await wait_for_turn_settled(fake, replies, max_wait=60.0)

            results["step3_full_mode_prompt_count"] = sum(1 for r in replies if "请求批准" in r or "请求额外权限" in r)
            results["step3_replies"] = replies
            results["step3_marker_exists"] = marker_exists()
            LOGGER.info("full mode approval prompts: %d, marker exists: %s, replies: %d", results["step3_full_mode_prompt_count"], results["step3_marker_exists"], len(replies))

            clean_marker()
            return results
        finally:
            await bridge.shutdown()
    finally:
        fake.write_line("quit")
        await asyncio.sleep(0.5)
        await fake.shutdown()


async def main() -> int:
    parser = argparse.ArgumentParser(description="Approval e2e test")
    parser.add_argument("--config", default="config.toml")
    args = parser.parse_args()

    results = await run_test(args.config)
    results_path = Path("approval_e2e_results.json")
    with open(results_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    LOGGER.info("results written to %s", results_path)
    # 终端可能是 GBK，打印 ASCII 转义版本避免崩溃；完整中文结果已写入文件
    print(json.dumps(results, ensure_ascii=True, indent=2))

    ok = (
        results.get("step1_approval_prompt")
        and not results.get("step1_marker_exists_after_no")
        and results.get("step2_approval_prompt")
        and results.get("step2_marker_exists_after_yes")
        and results.get("step3_full_mode_prompt_count", 999) == 0
        and results.get("step3_marker_exists")
    )
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
