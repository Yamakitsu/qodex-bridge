"""OneBot 11 正向 WebSocket 客户端。"""

from __future__ import annotations

import asyncio
import json
import logging
import random
from typing import Any, Callable

import websockets

LOGGER = logging.getLogger(__name__)

OnPrivateMessage = Callable[[str, str], None]
OnClose = Callable[[], None]


class OneBotClient:
    def __init__(
        self,
        ws_url: str,
        access_token: str | None,
        on_private_message: OnPrivateMessage | None = None,
        on_close: OnClose | None = None,
    ):
        self.ws_url = ws_url
        self.access_token = access_token
        self.on_private_message = on_private_message
        self.on_close = on_close

        self._ws: websockets.WebSocketClientProtocol | None = None
        self._tasks: list[asyncio.Task] = []
        self._closed = False
        self._echo_counter = 0
        self._pending_acks: dict[str, asyncio.Future] = {}

    async def start(self) -> None:
        self._tasks.append(asyncio.create_task(self._connect_loop(), name="connect"))

    async def _connect_loop(self) -> None:
        delay = 1.0
        while not self._closed:
            try:
                headers = {}
                if self.access_token:
                    headers["Authorization"] = f"Bearer {self.access_token}"
                LOGGER.info("连接 NapCat: %s", self.ws_url)
                self._ws = await websockets.connect(self.ws_url, additional_headers=headers)
                LOGGER.info("NapCat 已连接")
                delay = 1.0
                await self._read_loop()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                LOGGER.warning("NapCat 连接异常: %s", exc)
            finally:
                self._ws = None

            if self._closed:
                break

            # 指数退避 + 抖动
            jitter = random.uniform(0, 1)
            sleep = min(delay + jitter, 30.0)
            LOGGER.info("%s 秒后重连 NapCat", round(sleep, 1))
            await asyncio.sleep(sleep)
            delay = min(delay * 2, 30.0)

        LOGGER.info("NapCat 连接循环结束")
        if self.on_close:
            try:
                self.on_close()
            except Exception:
                LOGGER.exception("on_close 回调异常")

    async def _read_loop(self) -> None:
        if self._ws is None:
            return
        try:
            async for raw in self._ws:
                if isinstance(raw, bytes):
                    text = raw.decode("utf-8")
                else:
                    text = raw
                try:
                    msg = json.loads(text)
                except json.JSONDecodeError:
                    LOGGER.warning("收到非 JSON 消息: %s", text[:200])
                    continue
                self._handle_message(msg)
        except websockets.exceptions.ConnectionClosed as exc:
            LOGGER.warning("NapCat 连接关闭: %s", exc)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            LOGGER.exception("NapCat 读取异常: %s", exc)

    def _handle_message(self, msg: dict[str, Any]) -> None:
        # action 响应
        if "echo" in msg:
            echo = msg["echo"]
            future = self._pending_acks.pop(echo, None)
            if future is not None and not future.done():
                future.set_result(msg)
            if "status" in msg or "retcode" in msg:
                LOGGER.debug("action ack: %s", msg)
            return

        # 事件
        post_type = msg.get("post_type")
        if post_type == "message":
            message_type = msg.get("message_type")
            sub_type = msg.get("sub_type")
            if message_type == "private" and sub_type == "friend":
                user_id = str(msg.get("sender", {}).get("user_id", msg.get("user_id")))
                raw_message = msg.get("raw_message", "")
                LOGGER.info("收到私聊消息 %s: %s", user_id, raw_message)
                if self.on_private_message:
                    try:
                        self.on_private_message(user_id, raw_message)
                    except Exception:
                        LOGGER.exception("私聊消息处理异常")

    def send_private_msg(self, user_id: str, message: str) -> asyncio.Future | None:
        if self._ws is None or self._ws.close_code is not None:
            LOGGER.warning("NapCat 未连接，无法发送消息给 %s", user_id)
            return None
        self._echo_counter += 1
        echo = f"send_{self._echo_counter}"
        payload = {
            "action": "send_private_msg",
            "params": {"user_id": user_id, "message": message},
            "echo": echo,
        }
        future: asyncio.Future = asyncio.get_running_loop().create_future()
        self._pending_acks[echo] = future

        async def _send() -> None:
            try:
                await self._ws.send(json.dumps(payload, ensure_ascii=False))
                LOGGER.debug("发送给 %s: %s", user_id, message[:80])
                if not future.done():
                    future.set_result(None)
            except Exception as exc:
                LOGGER.exception("发送失败: %s", exc)
                if not future.done():
                    future.set_exception(exc)

        asyncio.create_task(_send())
        return future

    async def shutdown(self) -> None:
        self._closed = True
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        if self._ws is not None:
            await self._ws.close()
