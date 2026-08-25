"""WebUI：aiohttp + WebSocket，浅色 Claude 风格。"""

from __future__ import annotations

import asyncio
import json
import logging
import secrets
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable

from aiohttp import web

LOGGER = logging.getLogger(__name__)

MAX_MESSAGES = 200
TOKEN_PATH = Path("data/server.token")


@dataclass
class WebUIConfig:
    enabled: bool = True
    host: str = "127.0.0.1"
    port: int = 8765


class WebUI:
    def __init__(self, orchestrator: Any, config: WebUIConfig):
        self.orchestrator = orchestrator
        self.config = config
        self._token = self._load_or_generate_token()
        self._clients: set[web.WebSocketResponse] = set()
        self._messages: deque[dict[str, Any]] = deque(maxlen=MAX_MESSAGES)
        self._app = self._build_app()
        self._runner: web.AppRunner | None = None
        self._site: web.TCPSite | None = None
        self._actual_port: int | None = None

    # ------------------------------------------------------------------ #
    # Token
    # ------------------------------------------------------------------ #
    def _load_or_generate_token(self) -> str:
        TOKEN_PATH.parent.mkdir(parents=True, exist_ok=True)
        if TOKEN_PATH.exists():
            return TOKEN_PATH.read_text(encoding="utf-8").strip()
        token = secrets.token_urlsafe(32)
        TOKEN_PATH.write_text(token, encoding="utf-8")
        try:
            import os

            os.chmod(TOKEN_PATH, 0o600)
        except Exception:
            pass
        return token

    def check_token(self, request: web.Request) -> bool:
        supplied: str | None = None
        # query param (WS)
        if "token" in request.query:
            supplied = request.query["token"]
        # header
        auth = request.headers.get("Authorization", "")
        if auth.lower().startswith("bearer "):
            supplied = auth[7:].strip()
        return supplied == self._token

    # ------------------------------------------------------------------ #
    # aiohttp app
    # ------------------------------------------------------------------ #
    def _build_app(self) -> web.Application:
        app = web.Application()
        app.router.add_get("/", self._handle_index)
        app.router.add_get("/api/v1/status", self._handle_status)
        app.router.add_get("/api/v1/messages", self._handle_messages)
        app.router.add_post("/api/v1/prompt", self._handle_prompt)
        app.router.add_post("/api/v1/project", self._handle_project)
        app.router.add_post("/api/v1/projects", self._handle_projects_create)
        app.router.add_post("/api/v1/thread", self._handle_thread)
        app.router.add_post("/api/v1/model", self._handle_model)
        app.router.add_post("/api/v1/effort", self._handle_effort)
        app.router.add_post("/api/v1/mode", self._handle_mode)
        app.router.add_post("/api/v1/new", self._handle_new)
        app.router.add_post("/api/v1/stop", self._handle_stop)
        app.router.add_post("/api/v1/interrupt", self._handle_interrupt)
        app.router.add_get("/api/v1/threads", self._handle_threads)
        app.router.add_get("/api/v1/models", self._handle_models)
        app.router.add_get("/api/v1/queue", self._handle_queue)
        app.router.add_post("/api/v1/queue/pop", self._handle_queue_pop)
        app.router.add_post("/api/v1/queue/clear", self._handle_queue_clear)
        app.router.add_post("/api/v1/approve", self._handle_approve)
        app.router.add_post("/api/v1/deny", self._handle_deny)
        app.router.add_get("/api/v1/ws", self._handle_ws)
        return app

    def _auth_error(self) -> web.Response:
        return web.json_response({"error": "unauthorized"}, status=401)

    async def _handle_index(self, request: web.Request) -> web.Response:
        html_path = Path(__file__).with_name("webui.html")
        if not html_path.exists():
            return web.Response(text="webui.html not found", status=500)
        text = html_path.read_text(encoding="utf-8")
        return web.Response(text=text, content_type="text/html")

    async def _handle_status(self, request: web.Request) -> web.Response:
        if not self.check_token(request):
            return self._auth_error()
        return web.json_response(self._build_status())

    async def _handle_messages(self, request: web.Request) -> web.Response:
        if not self.check_token(request):
            return self._auth_error()
        return web.json_response(list(self._messages))

    async def _handle_prompt(self, request: web.Request) -> web.Response:
        if not self.check_token(request):
            return self._auth_error()
        try:
            data = await request.json()
            text = str(data.get("text", "")).strip()
        except Exception:
            return web.json_response({"error": "invalid json"}, status=400)
        if not text:
            return web.json_response({"error": "empty text"}, status=400)
        self.orchestrator.inject_prompt(text)
        return web.json_response({"ok": True})

    async def _handle_project(self, request: web.Request) -> web.Response:
        if not self.check_token(request):
            return self._auth_error()
        try:
            data = await request.json()
            name = str(data.get("name", "")).strip()
        except Exception:
            return web.json_response({"error": "invalid json"}, status=400)
        ok = await self.orchestrator.switch_project(name)
        return web.json_response({"ok": ok})

    async def _handle_thread(self, request: web.Request) -> web.Response:
        if not self.check_token(request):
            return self._auth_error()
        try:
            data = await request.json()
        except Exception:
            return web.json_response({"error": "invalid json"}, status=400)
        thread_id = str(data.get("id", "")).strip()
        if thread_id:
            ok, msg, history = await self.orchestrator.resume_thread(thread_id)
            return web.json_response({"ok": ok, "message": msg, "history": history})
        try:
            idx = int(data.get("index", -1))
        except Exception:
            return web.json_response({"error": "missing id or index"}, status=400)
        ok = await self.orchestrator.switch_thread(idx)
        return web.json_response({"ok": ok})

    async def _handle_model(self, request: web.Request) -> web.Response:
        if not self.check_token(request):
            return self._auth_error()
        try:
            data = await request.json()
            name = str(data.get("name", "")).strip()
        except Exception:
            return web.json_response({"error": "invalid json"}, status=400)
        ok, msg = await self.orchestrator.set_model(name)
        return web.json_response({"ok": ok, "message": msg})

    async def _handle_mode(self, request: web.Request) -> web.Response:
        if not self.check_token(request):
            return self._auth_error()
        try:
            data = await request.json()
            mode = str(data.get("mode", "")).strip()
        except Exception:
            return web.json_response({"error": "invalid json"}, status=400)
        # WebUI 前端自己弹确认框，这里直接设置
        ok, msg = await self.orchestrator.set_mode(mode)
        return web.json_response({"ok": ok, "message": msg})

    async def _handle_approve(self, request: web.Request) -> web.Response:
        if not self.check_token(request):
            return self._auth_error()
        try:
            ok = await self.orchestrator.approve()
            return web.json_response({"ok": ok})
        except Exception as exc:
            LOGGER.exception("/approve 处理异常: %s", exc)
            return web.json_response({"error": str(exc)}, status=500)

    async def _handle_deny(self, request: web.Request) -> web.Response:
        if not self.check_token(request):
            return self._auth_error()
        try:
            ok = await self.orchestrator.deny()
            return web.json_response({"ok": ok})
        except Exception as exc:
            LOGGER.exception("/deny 处理异常: %s", exc)
            return web.json_response({"error": str(exc)}, status=500)

    async def _handle_effort(self, request: web.Request) -> web.Response:
        if not self.check_token(request):
            return self._auth_error()
        try:
            data = await request.json()
            effort = str(data.get("effort", "")).strip()
        except Exception:
            return web.json_response({"error": "invalid json"}, status=400)
        ok, msg = await self.orchestrator.set_effort(effort)
        return web.json_response({"ok": ok, "message": msg})

    async def _handle_new(self, request: web.Request) -> web.Response:
        if not self.check_token(request):
            return self._auth_error()
        ok, msg = await self.orchestrator.new_thread()
        return web.json_response({"ok": ok, "message": msg})

    async def _handle_stop(self, request: web.Request) -> web.Response:
        if not self.check_token(request):
            return self._auth_error()
        ok, msg = await self.orchestrator.stop_turn()
        return web.json_response({"ok": ok, "message": msg})

    async def _handle_interrupt(self, request: web.Request) -> web.Response:
        if not self.check_token(request):
            return self._auth_error()
        try:
            data = await request.json()
            text = str(data.get("text", "")).strip()
        except Exception:
            return web.json_response({"error": "invalid json"}, status=400)
        await self.orchestrator.interrupt_with(text)
        return web.json_response({"ok": True})

    async def _handle_threads(self, request: web.Request) -> web.Response:
        if not self.check_token(request):
            return self._auth_error()
        project = request.query.get("project") or None
        try:
            threads = await self.orchestrator.list_threads(project)
            return web.json_response({"threads": threads})
        except Exception as exc:
            LOGGER.warning("/threads 失败: %s", exc)
            return web.json_response({"error": str(exc)}, status=500)

    async def _handle_projects_create(self, request: web.Request) -> web.Response:
        if not self.check_token(request):
            return self._auth_error()
        try:
            data = await request.json()
            name = str(data.get("name", ""))
            path = str(data.get("path", ""))
        except Exception:
            return web.json_response({"error": "invalid json"}, status=400)
        ok, msg = await self.orchestrator.add_project(name, path)
        return web.json_response({"ok": ok, "message": msg})

    async def _handle_models(self, request: web.Request) -> web.Response:
        if not self.check_token(request):
            return self._auth_error()
        try:
            models = await self.orchestrator.list_models()
            return web.json_response({"models": models})
        except Exception as exc:
            LOGGER.warning("/models 失败: %s", exc)
            return web.json_response({"error": str(exc)}, status=500)

    async def _handle_queue(self, request: web.Request) -> web.Response:
        if not self.check_token(request):
            return self._auth_error()
        return web.json_response({"queue": self.orchestrator.get_queue()})

    async def _handle_queue_pop(self, request: web.Request) -> web.Response:
        if not self.check_token(request):
            return self._auth_error()
        try:
            data = await request.json()
            idx = int(data.get("index", -1))
        except Exception:
            return web.json_response({"error": "invalid json"}, status=400)
        ok, msg = await self.orchestrator.queue_pop(idx)
        return web.json_response({"ok": ok, "message": msg})

    async def _handle_queue_clear(self, request: web.Request) -> web.Response:
        if not self.check_token(request):
            return self._auth_error()
        await self.orchestrator.queue_clear()
        return web.json_response({"ok": True})

    async def _handle_ws(self, request: web.Request) -> web.WebSocketResponse:
        if not self.check_token(request):
            return self._auth_error()  # type: ignore[return-value]
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        self._clients.add(ws)
        LOGGER.info("WebUI WebSocket 客户端已连接")
        # 发送当前历史与状态
        await ws.send_json({"type": "messages", "data": list(self._messages)})
        await ws.send_json({"type": "status", "data": self._build_status()})
        try:
            async for _ in ws:
                pass
        finally:
            self._clients.discard(ws)
            LOGGER.info("WebUI WebSocket 客户端已断开")
        return ws

    # ------------------------------------------------------------------ #
    # 运行
    # ------------------------------------------------------------------ #
    async def run(self) -> None:
        self._runner = web.AppRunner(self._app)
        await self._runner.setup()

        port = self.config.port
        while True:
            try:
                self._site = web.TCPSite(self._runner, self.config.host, port)
                await self._site.start()
                self._actual_port = port
                break
            except OSError as exc:
                # 10048: Address already in use; 10013: WSAEACCES / Permission denied
                if exc.errno in (10048, 10013, 13):
                    LOGGER.warning("端口 %d 被占用，尝试 %d", port, port + 1)
                    port += 1
                else:
                    raise

        url = f"http://{self.config.host}:{self._actual_port}/#token={self._token}"
        LOGGER.info("WebUI 已启动: %s", url)
        print(f"WebUI: {url}")

    async def shutdown(self) -> None:
        for ws in list(self._clients):
            await ws.close()
        self._clients.clear()
        if self._runner is not None:
            await self._runner.cleanup()

    # ------------------------------------------------------------------ #
    # 与 Orchestrator 交互
    # ------------------------------------------------------------------ #
    def on_message(self, kind: str, source: str, text: str, **kwargs: Any) -> None:
        """Orchestrator 调用此方法来推送消息/事件到 WebUI。"""
        msg = {
            "id": secrets.token_hex(8),
            "ts": datetime.now(timezone.utc).isoformat(),
            "kind": kind,
            "source": source,
            "text": text,
            "metadata": kwargs,
        }
        self._messages.append(msg)
        self._broadcast("message", msg)

    def on_status_change(self) -> None:
        status = self._build_status()
        self._broadcast("status", status)

    def on_turn(self, status: str, turn_id: str | None = None) -> None:
        self._broadcast("turn", {"status": status, "turn_id": turn_id})

    def on_delta(self, delta: str) -> None:
        if delta:
            self._broadcast("delta", {"text": delta})

    def on_reasoning_delta(self, delta: str) -> None:
        if delta:
            self._broadcast("reasoning_delta", {"text": delta})

    def on_approval(self, status: str, kind: str | None = None, text: str = "", **extra: Any) -> None:
        payload = {"status": status, "kind": kind, "text": text}
        payload.update(extra)
        self._broadcast("approval", payload)

    def _build_status(self) -> dict[str, Any]:
        onebot_connected = False
        try:
            ws = self.orchestrator.onebot._ws
            onebot_connected = ws is not None and ws.close_code is None
        except Exception:
            pass
        appserver_alive = False
        try:
            proc = self.orchestrator.appserver._proc
            appserver_alive = proc is not None and proc.returncode is None
        except Exception:
            pass
        pending = None
        if self.orchestrator._pending_approvals:
            head = self.orchestrator._pending_approvals[0]
            pending = {
                "kind": head.kind,
                "text": head.prompt_text,
                "request_id": head.request_id,
            }
        return {
            "project": self.orchestrator._current_project_name(),
            "thread": self.orchestrator._current_thread_id,
            "model": self.orchestrator.state.model,
            "effort": self.orchestrator.state.effort,
            "mode": self.orchestrator.state.mode,
            "busy": self.orchestrator._busy,
            "queue_length": len(self.orchestrator._queue),
            "queue": [m.text for m in self.orchestrator._queue],
            "pending_approvals": len(self.orchestrator._pending_approvals),
            "pending_approval": pending,
            "onebot_connected": onebot_connected,
            "appserver_alive": appserver_alive,
            "webui_port": self._actual_port,
            "projects": dict(self.orchestrator.config.projects),
            "approval_timeout_sec": self.orchestrator.config.approval_timeout_sec,
        }

    def _broadcast(self, event_type: str, data: Any) -> None:
        payload = json.dumps({"type": event_type, "data": data}, ensure_ascii=False)
        for ws in list(self._clients):
            try:
                asyncio.create_task(ws.send_str(payload))
            except Exception as exc:
                LOGGER.warning("WS 广播失败: %s", exc)
