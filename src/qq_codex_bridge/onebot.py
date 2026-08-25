"""OneBot 11 正向 WebSocket 客户端。"""

from __future__ import annotations

import asyncio
import html
import json
import logging
import random
import re
from dataclasses import dataclass, field
from typing import Any, Callable

import websockets

LOGGER = logging.getLogger(__name__)

CQ_CODE_RE = re.compile(r"\[CQ:([^,\]]+)((?:,[^\]]*)?)\]")


@dataclass(slots=True)
class IncomingAttachment:
    """OneBot 消息段中的可归档附件。"""

    kind: str
    file_name: str
    url: str | None = None
    file_id: str | None = None
    file_hash: str | None = None
    local_path: str | None = None


@dataclass(slots=True)
class IncomingPrivateMessage:
    user_id: str
    text: str
    message_id: str | None = None
    reply_to_message_id: str | None = None
    attachments: list[IncomingAttachment] = field(default_factory=list)
    chat_type: str = "private"  # private | group
    group_id: str | None = None
    mentioned_bot: bool = True


OnPrivateMessage = Callable[[IncomingPrivateMessage], None]
OnClose = Callable[[], None]


def _parse_cq_message(raw_message: str) -> tuple[str, str | None, list[IncomingAttachment]]:
    text_parts: list[str] = []
    attachments: list[IncomingAttachment] = []
    reply_to_message_id: str | None = None
    position = 0
    for match in CQ_CODE_RE.finditer(raw_message):
        text_parts.append(html.unescape(raw_message[position : match.start()]))
        position = match.end()
        segment_type = match.group(1)
        data: dict[str, str] = {}
        raw_args = match.group(2).lstrip(",")
        for pair in raw_args.split(",") if raw_args else []:
            key, separator, value = pair.partition("=")
            if separator:
                data[key] = html.unescape(value)
        if segment_type == "reply":
            reply_id = data.get("id") or data.get("message_id")
            if reply_id is not None:
                reply_to_message_id = reply_id
        elif segment_type in {"image", "file", "video", "record"}:
            attachments.append(
                IncomingAttachment(
                    kind=segment_type,
                    file_name=(
                        data.get("name")
                        or data.get("file")
                        or data.get("file_id")
                        or segment_type
                    ),
                    url=data.get("url"),
                    file_id=data.get("file_id") or data.get("id"),
                    file_hash=data.get("file_hash") or data.get("hash"),
                    local_path=data.get("path"),
                )
            )
    text_parts.append(html.unescape(raw_message[position:]))
    return "".join(text_parts).strip(), reply_to_message_id, attachments


def parse_message(msg: dict[str, Any]) -> IncomingPrivateMessage:
    """把 OneBot 11 私聊/群聊事件解析为统一消息。"""

    user_id = str(msg.get("sender", {}).get("user_id", msg.get("user_id")))
    message_id_value = msg.get("message_id")
    message_id = str(message_id_value) if message_id_value is not None else None
    text_parts: list[str] = []
    attachments: list[IncomingAttachment] = []
    reply_to_message_id: str | None = None
    message_type = str(msg.get("message_type") or "private")
    group_id_value = msg.get("group_id")
    group_id = str(group_id_value) if group_id_value is not None else None
    self_id = str(msg.get("self_id")) if msg.get("self_id") is not None else None
    mentioned_bot = message_type == "private"
    segments = msg.get("message")

    if isinstance(segments, list):
        for segment in segments:
            if not isinstance(segment, dict):
                continue
            segment_type = str(segment.get("type") or "")
            data = segment.get("data") if isinstance(segment.get("data"), dict) else {}
            if segment_type == "text":
                text_parts.append(str(data.get("text") or ""))
                continue
            if segment_type == "reply":
                reply_id = data.get("id") or data.get("message_id")
                if reply_id is not None:
                    reply_to_message_id = str(reply_id)
                continue
            if segment_type == "at":
                qq = str(data.get("qq") or "")
                if self_id is not None and qq == self_id:
                    mentioned_bot = True
                continue
            if segment_type not in {"image", "file", "video", "record"}:
                continue

            file_name = str(
                data.get("name") or data.get("file") or data.get("file_id") or segment_type
            )
            file_id_value = data.get("file_id") or data.get("id")
            file_hash_value = data.get("file_hash") or data.get("hash")
            attachments.append(
                IncomingAttachment(
                    kind=segment_type,
                    file_name=file_name,
                    url=str(data.get("url")) if data.get("url") else None,
                    file_id=str(file_id_value) if file_id_value is not None else None,
                    file_hash=str(file_hash_value) if file_hash_value is not None else None,
                    local_path=str(data.get("path")) if data.get("path") else None,
                )
            )

    text = "".join(text_parts).strip()
    if not isinstance(segments, list):
        raw_message = str(msg.get("raw_message") or segments or "")
        text, reply_to_message_id, attachments = _parse_cq_message(raw_message)
        if message_type == "group" and self_id is not None:
            mentioned_bot = bool(
                re.search(rf"\[CQ:at,[^\]]*qq={re.escape(self_id)}(?:,|\])", raw_message)
            )

    return IncomingPrivateMessage(
        user_id=user_id,
        text=text,
        message_id=message_id,
        reply_to_message_id=reply_to_message_id,
        attachments=attachments,
        chat_type="group" if message_type == "group" else "private",
        group_id=group_id,
        mentioned_bot=mentioned_bot,
    )


def parse_private_message(msg: dict[str, Any]) -> IncomingPrivateMessage:
    """兼容旧调用名；返回统一的私聊/群聊消息对象。"""

    return parse_message(msg)


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
            if (message_type == "private" and sub_type == "friend") or message_type == "group":
                incoming = parse_message(msg)
                LOGGER.info(
                    "收到%s消息 scope=%s sender=%s: text=%r attachments=%d reply=%s",
                    "群聊" if incoming.chat_type == "group" else "私聊",
                    incoming.group_id or incoming.user_id,
                    incoming.user_id,
                    incoming.text[:120], len(incoming.attachments), incoming.reply_to_message_id,
                )
                if self.on_private_message:
                    try:
                        self.on_private_message(incoming)
                    except Exception:
                        LOGGER.exception("QQ 消息处理异常")

    def send_private_msg(self, user_id: str, message: str) -> asyncio.Future | None:
        """发送严格的 OneBot text segment，避免把正文解析成 CQ/富文本。"""

        return self.request_action(
            "send_private_msg",
            {
                "user_id": user_id,
                "message": [{"type": "text", "data": {"text": message}}],
            },
            echo_prefix="send",
        )

    def send_group_msg(self, group_id: str, message: str) -> asyncio.Future | None:
        """向群发送严格的 OneBot text segment。"""

        return self.request_action(
            "send_group_msg",
            {
                "group_id": group_id,
                "message": [{"type": "text", "data": {"text": message}}],
            },
            echo_prefix="send_group",
        )

    def request_action(
        self,
        action: str,
        params: dict[str, Any],
        *,
        echo_prefix: str = "action",
    ) -> asyncio.Future | None:
        if self._ws is None or self._ws.close_code is not None:
            LOGGER.warning("NapCat 未连接，无法调用 action=%s", action)
            return None
        self._echo_counter += 1
        echo = f"{echo_prefix}_{self._echo_counter}"
        payload = {
            "action": action,
            "params": params,
            "echo": echo,
        }
        future: asyncio.Future = asyncio.get_running_loop().create_future()
        self._pending_acks[echo] = future
        future.add_done_callback(lambda _done: self._pending_acks.pop(echo, None))

        async def _send() -> None:
            try:
                await self._ws.send(json.dumps(payload, ensure_ascii=False))
                LOGGER.debug("已发送 OneBot action=%s echo=%s", action, echo)
            except Exception as exc:
                LOGGER.exception("发送失败: %s", exc)
                self._pending_acks.pop(echo, None)
                if not future.done():
                    future.set_exception(exc)

        asyncio.create_task(_send())
        return future

    async def call_action(
        self,
        action: str,
        params: dict[str, Any],
        *,
        timeout: float = 10.0,
    ) -> dict[str, Any]:
        future = self.request_action(action, params)
        if future is None:
            raise RuntimeError("NapCat 未连接")
        response = await asyncio.wait_for(future, timeout=timeout)
        if response.get("status") not in (None, "ok") or response.get("retcode") not in (None, 0):
            raise RuntimeError(f"OneBot action {action} 失败: {response}")
        data = response.get("data")
        return data if isinstance(data, dict) else {}

    async def shutdown(self) -> None:
        self._closed = True
        for future in self._pending_acks.values():
            if not future.done():
                future.cancel()
        self._pending_acks.clear()
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        if self._ws is not None:
            await self._ws.close()
