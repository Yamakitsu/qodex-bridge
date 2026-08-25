"""测试公共辅助：子进程包装器。"""

from __future__ import annotations

import asyncio
import os
import re
import sys
import time
from pathlib import Path
from typing import Any


class ProcessWrapper:
    def __init__(self, name: str, proc: asyncio.subprocess.Process):
        self.name = name
        self.proc = proc
        self.lines: asyncio.Queue[str] = asyncio.Queue()
        self._tasks: list[asyncio.Task] = []

    @classmethod
    async def start(cls, name: str, cmd: list[str | Path], cwd: str | Path | None = None) -> "ProcessWrapper":
        env = {**os.environ, "PYTHONIOENCODING": "utf-8", "PYTHONUNBUFFERED": "1"}
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            env=env,
            cwd=cwd,
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
        except Exception:
            pass

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
        except Exception:
            pass


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
