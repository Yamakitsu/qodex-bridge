"""Codex app-server JSON-RPC stdio 客户端。"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from pathlib import Path
from typing import Any, Awaitable, Callable

LOGGER = logging.getLogger(__name__)

OnNotification = Callable[[str, dict[str, Any]], None]
OnServerRequest = Callable[[str, int, dict[str, Any]], Awaitable[dict[str, Any]]]
OnClose = Callable[[], None]


class AppServerClient:
    def __init__(
        self,
        codex_path: str,
        on_notification: OnNotification | None = None,
        on_server_request: OnServerRequest | None = None,
        on_close: OnClose | None = None,
    ):
        self.codex_path = str(Path(codex_path).expanduser())
        self.on_notification = on_notification
        self.on_server_request = on_server_request
        self.on_close = on_close

        self._proc: asyncio.subprocess.Process | None = None
        self._write_lock = asyncio.Lock()
        self._next_id = 1
        self._pending: dict[int, asyncio.Future] = {}
        self._tasks: list[asyncio.Task] = []
        self._closed = False

    async def start(self) -> dict[str, Any]:
        LOGGER.info("启动 Codex app-server: %s", self.codex_path)
        # Windows 需要 CREATE_NO_WINDOW 避免弹出控制台
        creationflags = 0
        if os.name == "nt":
            creationflags = getattr(asyncio.subprocess, "CREATE_NO_WINDOW", 0x08000000)

        self._proc = await asyncio.create_subprocess_exec(
            self.codex_path,
            "app-server",
            "--stdio",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            creationflags=creationflags,
        )

        self._tasks.append(asyncio.create_task(self._read_stdout(), name="stdout"))
        self._tasks.append(asyncio.create_task(self._read_stderr(), name="stderr"))
        self._tasks.append(asyncio.create_task(self._watch_proc(), name="watch"))

        init_result = await self.call(
            "initialize",
            clientInfo={
                "name": "qq_codex_bridge",
                "title": "QQ Codex Bridge",
                "version": "0.1.0",
            },
            capabilities={"experimentalApi": True},
        )
        await self.notify("initialized")
        LOGGER.info("Codex app-server 初始化完成: %s", init_result.get("userAgent", "unknown"))
        return init_result

    async def call(self, method: str, **params: Any) -> Any:
        if self._closed:
            raise RuntimeError("AppServerClient 已关闭")
        req_id = self._next_id
        self._next_id += 1
        msg = {"id": req_id, "method": method, "params": params}
        future: asyncio.Future = asyncio.get_running_loop().create_future()
        self._pending[req_id] = future
        await self._send_raw(msg)
        return await future

    async def notify(self, method: str, **params: Any) -> None:
        if self._closed:
            raise RuntimeError("AppServerClient 已关闭")
        await self._send_raw({"method": method, "params": params})

    async def _send_raw(self, msg: dict[str, Any]) -> None:
        line = json.dumps(msg, ensure_ascii=False) + "\n"
        LOGGER.debug("--> %s", line.rstrip())
        async with self._write_lock:
            if self._proc is None or self._proc.stdin is None:
                raise RuntimeError("子进程未启动或已关闭")
            self._proc.stdin.write(line.encode("utf-8"))
            await self._proc.stdin.drain()

    async def _read_stdout(self) -> None:
        if self._proc is None or self._proc.stdout is None:
            return
        try:
            while True:
                line = await self._proc.stdout.readline()
                if not line:
                    break
                text = line.decode("utf-8").strip()
                if not text:
                    continue
                LOGGER.debug("<-- %s", text)
                self._handle_message(text)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            LOGGER.exception("stdout 读取异常: %s", exc)
        finally:
            LOGGER.info("stdout 读取结束")
            self._close_pending(RuntimeError("app-server stdout 已关闭"))

    def _handle_message(self, text: str) -> None:
        try:
            msg = json.loads(text)
        except json.JSONDecodeError:
            LOGGER.warning("收到非 JSON 行: %s", text[:200])
            return

        if "id" in msg:
            # 响应
            if "result" in msg or "error" in msg:
                self._handle_response(msg)
                return
            # server -> client request
            if "method" in msg:
                asyncio.create_task(self._handle_server_request(msg))
                return

        # notification
        if "method" in msg:
            if self.on_notification:
                try:
                    self.on_notification(msg["method"], msg.get("params", {}))
                except Exception:
                    LOGGER.exception("notification 回调异常")
            return

        LOGGER.warning("无法识别的消息: %s", msg)

    def _handle_response(self, msg: dict[str, Any]) -> None:
        req_id = msg["id"]
        future = self._pending.pop(req_id, None)
        if future is None:
            LOGGER.warning("收到未知 id 的响应: %s", req_id)
            return
        if "error" in msg:
            future.set_exception(AppServerError(msg["error"]))
        else:
            future.set_result(msg.get("result"))

    async def _handle_server_request(self, msg: dict[str, Any]) -> None:
        method = msg["method"]
        req_id = msg["id"]
        params = msg.get("params", {})
        LOGGER.info("收到 server->client 请求: %s id=%s", method, req_id)
        result: dict[str, Any] = {}
        try:
            if self.on_server_request:
                result = await self.on_server_request(method, req_id, params)
            else:
                LOGGER.warning("未注册 server request 回调，默认拒绝: %s", method)
                result = {"decision": "decline"}
        except Exception as exc:
            LOGGER.exception("server request 处理异常: %s", exc)
            result = {"decision": "decline"}
        await self._send_raw({"id": req_id, "result": result})

    async def _read_stderr(self) -> None:
        if self._proc is None or self._proc.stderr is None:
            return
        try:
            while True:
                line = await self._proc.stderr.readline()
                if not line:
                    break
                text = line.decode("utf-8").rstrip()
                if text:
                    LOGGER.debug("[codex stderr] %s", text)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            LOGGER.exception("stderr 读取异常: %s", exc)

    async def _watch_proc(self) -> None:
        if self._proc is None:
            return
        try:
            await self._proc.wait()
            LOGGER.error("Codex app-server 子进程已退出，码=%s", self._proc.returncode)
        except asyncio.CancelledError:
            raise
        finally:
            if not self._closed:
                self._close_pending(RuntimeError("app-server 子进程退出"))
                if self.on_close:
                    try:
                        self.on_close()
                    except Exception:
                        LOGGER.exception("on_close 回调异常")

    def _close_pending(self, exc: BaseException) -> None:
        for future in self._pending.values():
            if not future.done():
                future.set_exception(exc)
        self._pending.clear()

    async def shutdown(self) -> None:
        if self._closed:
            return
        self._closed = True
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        if self._proc is not None and self._proc.returncode is None:
            try:
                self._proc.terminate()
                await asyncio.wait_for(self._proc.wait(), timeout=5.0)
            except TimeoutError:
                self._proc.kill()
                await self._proc.wait()
            except Exception as exc:
                LOGGER.warning("关闭子进程出错: %s", exc)
        self._close_pending(RuntimeError("AppServerClient 已关闭"))


class AppServerError(Exception):
    def __init__(self, error: dict[str, Any]):
        self.code = error.get("code")
        self.message = error.get("message", str(error))
        super().__init__(f"[{self.code}] {self.message}")
