"""交互式假 NapCat（OneBot 11 正向 WS server）。

用法：
    .venv/Scripts/python tests/fake_napcat.py [--config config.toml]

终端输入的每一行会作为 QQ 用户消息推送给桥接器。
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import websockets
import websockets.server

from qq_codex_bridge.config import load_config

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
LOGGER = logging.getLogger("fake_napcat")


class FakeNapCat:
    def __init__(self, config_path: str, host: str = "127.0.0.1", port: int | None = None):
        self.config = load_config(config_path)
        self.host = host
        if port is None:
            parsed = urlparse(self.config.ws_url)
            port = parsed.port or 3001
        self.port = port
        self.user_id = str(next(iter(self.config.whitelist), "123456789"))
        self.clients: set[websockets.server.WebSocketServerProtocol] = set()
        self.stop_event = asyncio.Event()

    async def run(self) -> None:
        server = await websockets.serve(self._handle_client, self.host, self.port)
        LOGGER.info("假 NapCat 已监听 ws://%s:%d", self.host, self.port)
        LOGGER.info("模拟 QQ 用户: %s", self.user_id)
        LOGGER.info("在终端输入消息并按回车即可发送给桥接器，输入 quit 退出。")

        input_task = asyncio.create_task(self._read_terminal())
        stop_task = asyncio.create_task(self.stop_event.wait())
        await asyncio.wait([input_task, stop_task], return_when=asyncio.FIRST_COMPLETED)

        server.close()
        await server.wait_closed()

    async def _handle_client(self, ws: websockets.server.WebSocketServerProtocol) -> None:
        self.clients.add(ws)
        LOGGER.info("桥接器已连接")
        try:
            async for raw in ws:
                text = raw if isinstance(raw, str) else raw.decode("utf-8")
                try:
                    msg = json.loads(text)
                except json.JSONDecodeError:
                    LOGGER.warning("收到非 JSON: %s", text[:200])
                    continue
                await self._handle_action(ws, msg)
        except websockets.exceptions.ConnectionClosed as exc:
            LOGGER.info("桥接器断开: %s", exc)
        finally:
            self.clients.discard(ws)

    async def _handle_action(self, ws: websockets.server.WebSocketServerProtocol, msg: dict[str, Any]) -> None:
        action = msg.get("action")
        echo = msg.get("echo")
        params = msg.get("params", {})

        if action == "send_private_msg":
            user_id = params.get("user_id")
            message = str(params.get("message", ""))
            try:
                for line in message.splitlines() or [""]:
                    print(f"\n[机器人回复 {user_id}]: {line}\n> ", end="", flush=True)
            except Exception as exc:
                LOGGER.exception("打印机器人回复失败: %s", exc)
            ack = {"echo": echo, "retcode": 0, "status": "ok"}
        else:
            LOGGER.warning("未知 action: %s", action)
            ack = {"echo": echo, "retcode": 1404, "status": "unknown action"}

        await ws.send(json.dumps(ack, ensure_ascii=False))

    async def _read_terminal(self) -> None:
        loop = asyncio.get_running_loop()
        while not self.stop_event.is_set():
            try:
                # Windows 上绕过 sys.stdin 的文本编码，强制按 UTF-8 读取
                raw = await loop.run_in_executor(None, sys.stdin.buffer.readline)
            except EOFError:
                break
            if not raw:
                await asyncio.sleep(0.1)
                continue
            try:
                line = raw.decode("utf-8")
            except UnicodeDecodeError:
                line = raw.decode("utf-8", errors="replace")
            text = line.rstrip("\n\r")
            if text.lower() == "quit":
                self.stop_event.set()
                break
            if not text:
                print("> ", end="", flush=True)
                continue
            event = {
                "post_type": "message",
                "message_type": "private",
                "sub_type": "friend",
                "user_id": int(self.user_id),
                "sender": {"user_id": int(self.user_id)},
                "raw_message": text,
                "message": [{"type": "text", "data": {"text": text}}],
            }
            payload = json.dumps(event, ensure_ascii=False)
            if self.clients:
                for ws in list(self.clients):
                    try:
                        await ws.send(payload)
                    except Exception as exc:
                        LOGGER.warning("发送事件失败: %s", exc)
                LOGGER.info("已推送事件: %s", text[:60])
            else:
                LOGGER.warning("没有桥接器连接，消息未发送")
            print("> ", end="", flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Fake NapCat for testing")
    parser.add_argument("--config", default="config.toml")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=None)
    args = parser.parse_args()
    fake = FakeNapCat(args.config, args.host, args.port)
    asyncio.run(fake.run())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
