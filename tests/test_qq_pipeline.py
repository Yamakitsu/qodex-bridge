"""QQ 入站附件、引用图片、纯文本出站和工具事件过滤测试。"""

from __future__ import annotations

import asyncio
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from qq_codex_bridge.config import Config
from qq_codex_bridge.onebot import (
    IncomingAttachment,
    IncomingPrivateMessage,
    OneBotClient,
    parse_private_message,
)
from qq_codex_bridge.orchestrator import Orchestrator, qq_plain_text


class FakeWebUI:
    def __init__(self) -> None:
        self.messages: list[tuple[str, str, str, dict]] = []

    def on_message(self, kind: str, source: str, text: str, **kwargs: object) -> None:
        self.messages.append((kind, source, text, kwargs))


class IncomingMessageTests(unittest.TestCase):
    def test_parse_image_text_and_reply_segments(self) -> None:
        incoming = parse_private_message(
            {
                "user_id": 123,
                "message_id": 88,
                "message": [
                    {"type": "reply", "data": {"id": "77"}},
                    {"type": "image", "data": {"file": "photo.png", "url": "https://example.test/p.png"}},
                    {"type": "text", "data": {"text": " 看一下 "}},
                ],
            }
        )
        self.assertEqual(incoming.user_id, "123")
        self.assertEqual(incoming.message_id, "88")
        self.assertEqual(incoming.reply_to_message_id, "77")
        self.assertEqual(incoming.text, "看一下")
        self.assertEqual(incoming.attachments[0].file_name, "photo.png")

    def test_parse_cq_string_keeps_standalone_image_text_empty(self) -> None:
        incoming = parse_private_message(
            {
                "user_id": 123,
                "message_id": 99,
                "message": "[CQ:reply,id=77][CQ:image,file=photo.png,url=https://example.test/p.png]",
                "raw_message": "[CQ:reply,id=77][CQ:image,file=photo.png,url=https://example.test/p.png]",
            }
        )
        self.assertEqual(incoming.text, "")
        self.assertEqual(incoming.reply_to_message_id, "77")
        self.assertEqual(len(incoming.attachments), 1)
        self.assertEqual(incoming.attachments[0].url, "https://example.test/p.png")

    def test_markdown_is_downgraded_to_plain_text(self) -> None:
        source = "# 标题\n**重点**和[链接](https://example.test)\n```python\nprint(1)\n```"
        self.assertEqual(qq_plain_text(source), "标题\n重点和链接（https://example.test）\n\nprint(1)")


class QQPipelineTests(unittest.IsolatedAsyncioTestCase):
    async def test_standalone_image_is_silent_then_quoted_text_uses_archived_path(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            project = Path(temp_dir) / "project"
            project.mkdir()
            source = Path(temp_dir) / "source.png"
            source.write_bytes(b"png-data")
            config = Config(
                whitelist={"123"},
                projects={"test": str(project)},
                default_project="test",
                codex_path="codex",
            )
            orchestrator = Orchestrator(config, state_path=Path(temp_dir) / "state.json")
            orchestrator.webui = FakeWebUI()
            orchestrator._process_input = AsyncMock()  # type: ignore[method-assign]

            image = IncomingPrivateMessage(
                user_id="123",
                text="",
                message_id="image-message",
                attachments=[
                    IncomingAttachment(kind="image", file_name="photo.png", local_path=str(source))
                ],
            )
            await orchestrator._handle_private_message(image)
            orchestrator._process_input.assert_not_awaited()

            archived = list((project / "attachments").rglob("*.png"))
            self.assertEqual(len(archived), 1)
            self.assertRegex(archived[0].name, r"^\d{6}-\d{3}_photo\.png$")

            reply = IncomingPrivateMessage(
                user_id="123",
                text="这张图里是什么？",
                message_id="reply-message",
                reply_to_message_id="image-message",
            )
            await orchestrator._handle_private_message(reply)
            orchestrator._process_input.assert_awaited_once()
            user_id, prompt = orchestrator._process_input.await_args.args
            self.assertEqual(user_id, "123")
            self.assertIn("这张图里是什么？", prompt)
            self.assertIn(str(archived[0].resolve()), prompt)

    async def test_command_completion_goes_to_webui_not_qq(self) -> None:
        config = Config(whitelist={"123"}, projects={"test": "."}, default_project="test")
        orchestrator = Orchestrator(config)
        orchestrator.webui = FakeWebUI()
        orchestrator.onebot.send_private_msg = MagicMock()  # type: ignore[method-assign]

        await orchestrator._send_command_brief(
            "123",
            {"command": "python test.py", "cwd": "D:\\work", "status": "completed", "exitCode": 0},
        )

        orchestrator.onebot.send_private_msg.assert_not_called()
        self.assertEqual(len(orchestrator.webui.messages), 1)
        self.assertIn("python test.py", orchestrator.webui.messages[0][2])

    async def test_send_text_keeps_markdown_in_webui_but_sends_plain_qq_text(self) -> None:
        config = Config(whitelist={"123"}, projects={"test": "."}, default_project="test")
        orchestrator = Orchestrator(config)
        orchestrator.webui = FakeWebUI()
        ack = asyncio.get_running_loop().create_future()
        ack.set_result({"status": "ok", "retcode": 0})
        orchestrator.onebot.send_private_msg = MagicMock(return_value=ack)  # type: ignore[method-assign]

        await orchestrator._send_text("123", "# 标题\n**重点**", msg_type="agent")

        orchestrator.onebot.send_private_msg.assert_called_once_with("123", "标题\n重点")
        self.assertEqual(orchestrator.webui.messages[0][2], "# 标题\n**重点**")

    async def test_onebot_uses_text_segment_for_outgoing_message(self) -> None:
        class FakeSocket:
            close_code = None

            def __init__(self) -> None:
                self.sent: list[str] = []

            async def send(self, payload: str) -> None:
                self.sent.append(payload)

        client = OneBotClient("ws://unused", None)
        socket = FakeSocket()
        client._ws = socket  # type: ignore[assignment]
        future = client.send_private_msg("123", "[CQ:image,file=not-an-image]")
        self.assertIsNotNone(future)
        await asyncio.sleep(0)
        payload = json.loads(socket.sent[0])
        self.assertEqual(
            payload["params"]["message"],
            [{"type": "text", "data": {"text": "[CQ:image,file=not-an-image]"}}],
        )
        client._handle_message({"echo": payload["echo"], "status": "ok", "retcode": 0})
        await future


if __name__ == "__main__":
    unittest.main()
