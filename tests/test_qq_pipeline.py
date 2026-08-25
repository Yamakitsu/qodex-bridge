"""QQ 入站附件、引用图片、纯文本出站和工具事件过滤测试。"""

from __future__ import annotations

import asyncio
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from qq_codex_bridge.config import Config, RoutingConfig
from qq_codex_bridge.onebot import (
    IncomingAttachment,
    IncomingPrivateMessage,
    OneBotClient,
    parse_message,
    parse_private_message,
)
from qq_codex_bridge.orchestrator import Orchestrator, RouteContext, qq_plain_text


class FakeWebUI:
    def __init__(self) -> None:
        self.messages: list[tuple[str, str, str, dict]] = []

    def on_message(self, kind: str, source: str, text: str, **kwargs: object) -> None:
        self.messages.append((kind, source, text, kwargs))


class IncomingMessageTests(unittest.TestCase):
    def test_parse_group_message_requires_at_to_mark_bot_mentioned(self) -> None:
        incoming = parse_message(
            {
                "post_type": "message",
                "message_type": "group",
                "group_id": 9988,
                "self_id": 42,
                "sender": {"user_id": 123},
                "message": [
                    {"type": "at", "data": {"qq": "42"}},
                    {"type": "text", "data": {"text": " 你好 "}},
                ],
            }
        )
        self.assertEqual(incoming.chat_type, "group")
        self.assertEqual(incoming.group_id, "9988")
        self.assertEqual(incoming.user_id, "123")
        self.assertTrue(incoming.mentioned_bot)
        self.assertEqual(incoming.text, "你好")

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
    async def test_group_ingress_requires_group_whitelist_and_bot_mention(self) -> None:
        config = Config(group_whitelist={"9"})
        orchestrator = Orchestrator(config)
        orchestrator._on_private_message(
            IncomingPrivateMessage(
                user_id="2", text="未提及", chat_type="group", group_id="9", mentioned_bot=False
            )
        )
        self.assertTrue(orchestrator._incoming_messages.empty())

        orchestrator._on_private_message(
            IncomingPrivateMessage(
                user_id="2", text="提及", chat_type="group", group_id="9", mentioned_bot=True
            )
        )
        accepted = orchestrator._incoming_messages.get_nowait()
        self.assertEqual(accepted.group_id, "9")

    async def test_routing_policy_full_only_for_admin_private(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            project = Path(temp_dir) / "project"
            project.mkdir()
            config = Config(
                admins={"1"},
                whitelist={"1", "2"},
                group_whitelist={"9"},
                projects={"test": str(project)},
                default_project="test",
                default_model="gpt-5.6-luna",
            )

            async def collect(context: RouteContext) -> dict:
                orchestrator = Orchestrator(
                    config, state_path=Path(temp_dir) / f"{context.scope_key.replace(':', '_')}.json"
                )
                orchestrator._activate_context(context)
                orchestrator._current_thread_id = "thread-1"
                orchestrator._thread_loaded = True
                orchestrator.appserver.call = AsyncMock(
                    return_value={"turn": {"id": "turn-1"}}
                )  # type: ignore[method-assign]
                await orchestrator._start_turn(context, "你好")
                return orchestrator.appserver.call.await_args.kwargs

            private_admin = await collect(RouteContext("private:1", "1"))
            self.assertEqual(private_admin["sandboxPolicy"], {"type": "dangerFullAccess"})
            self.assertEqual(private_admin["approvalPolicy"], "never")

            group_admin = await collect(RouteContext("group:9", "1", "group", "9"))
            self.assertEqual(group_admin["sandboxPolicy"]["type"], "workspaceWrite")
            self.assertEqual(group_admin["sandboxPolicy"]["writableRoots"], [str(project)])
            self.assertTrue(group_admin["sandboxPolicy"]["networkAccess"])
            self.assertEqual(group_admin["approvalPolicy"], "never")

            private_user = await collect(RouteContext("private:2", "2"))
            self.assertEqual(private_user["sandboxPolicy"]["type"], "workspaceWrite")

    async def test_non_private_admin_approval_request_fails_closed(self) -> None:
        config = Config(admins={"1"}, projects={"test": "."}, default_project="test")
        orchestrator = Orchestrator(config)
        orchestrator._activate_context(RouteContext("group:9", "1", "group", "9"))
        result = await orchestrator._on_server_request(
            "item/commandExecution/requestApproval",
            7,
            {"command": "outside-project"},
        )
        self.assertEqual(result, {"decision": "decline"})
        self.assertFalse(orchestrator._pending_approvals)

    async def test_admin_group_can_switch_project_but_cannot_switch_mode(self) -> None:
        config = Config(
            admins={"1"},
            group_whitelist={"9"},
            projects={"a": ".", "b": "."},
            default_project="a",
        )
        orchestrator = Orchestrator(config)
        context = RouteContext("group:9", "1", "group", "9")
        self.assertTrue(orchestrator._command_allowed(context, "project"))
        self.assertFalse(orchestrator._command_allowed(context, "mode"))

    async def test_admin_managed_project_creation_stays_under_configured_root(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "root"
            config = Config(routing=RoutingConfig(project_root=str(root)))
            orchestrator = Orchestrator(config, state_path=root / "state.json")
            with patch(
                "qq_codex_bridge.orchestrator.projects_mod.load_overlay", return_value={}
            ), patch(
                "qq_codex_bridge.orchestrator.projects_mod.save_overlay"
            ):
                ok, _ = orchestrator._create_managed_project("角色一")
                escaped, _ = orchestrator._create_managed_project("../escape")

            self.assertTrue(ok)
            self.assertFalse(escaped)
            self.assertEqual(
                Path(config.projects["角色一"]),
                (root / "projects" / "custom" / "角色一").resolve(),
            )
            self.assertTrue((Path(config.projects["角色一"]) / "AGENTS.md").exists())

    async def test_auto_projects_copy_default_agents_per_scope(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "QQ-Codex-bot"
            template = root / "templates" / "default" / "AGENTS.md"
            template.parent.mkdir(parents=True)
            template.write_text("默认人格", encoding="utf-8")
            config = Config(
                routing=RoutingConfig(project_root=str(root)),
                default_model="gpt-5.6-luna",
            )
            orchestrator = Orchestrator(config, state_path=root / "state.json")

            user_state = orchestrator._state_for_context(RouteContext("private:2", "2"))
            group_state = orchestrator._state_for_context(
                RouteContext("group:9", "3", "group", "9")
            )

            user_project = Path(config.projects[user_state.project])
            group_project = Path(config.projects[group_state.project])
            self.assertNotEqual(user_project, group_project)
            self.assertEqual((user_project / "AGENTS.md").read_text(encoding="utf-8"), "默认人格")
            self.assertEqual((group_project / "AGENTS.md").read_text(encoding="utf-8"), "默认人格")

    async def test_thread_state_is_isolated_and_restored_per_scope(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "state.json"
            config = Config(projects={"test": "."}, default_project="test")
            orchestrator = Orchestrator(config, state_path=state_path)
            user = RouteContext("private:2", "2")
            group = RouteContext("group:9", "3", "group", "9")

            orchestrator._activate_context(user)
            orchestrator._current_thread_id = "user-thread"
            orchestrator._save_state()
            orchestrator._activate_context(group)
            orchestrator._current_thread_id = "group-thread"
            orchestrator._save_state()
            orchestrator._activate_context(user)

            self.assertEqual(orchestrator._current_thread_id, "user-thread")
            reloaded = Orchestrator(config, state_path=state_path)
            reloaded._activate_context(group)
            self.assertEqual(reloaded._current_thread_id, "group-thread")

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
            context, prompt = orchestrator._process_input.await_args.args
            self.assertEqual(context.scope_key, "private:123")
            self.assertEqual(context.sender_id, "123")
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

    async def test_send_text_routes_group_reply_to_group_action(self) -> None:
        config = Config(group_whitelist={"9"}, projects={"test": "."}, default_project="test")
        orchestrator = Orchestrator(config)
        ack = asyncio.get_running_loop().create_future()
        ack.set_result({"status": "ok", "retcode": 0})
        orchestrator.onebot.send_group_msg = MagicMock(return_value=ack)  # type: ignore[method-assign]
        orchestrator.onebot.send_private_msg = MagicMock()  # type: ignore[method-assign]

        await orchestrator._send_text(RouteContext("group:9", "2", "group", "9"), "群回复")

        orchestrator.onebot.send_group_msg.assert_called_once_with("9", "群回复")
        orchestrator.onebot.send_private_msg.assert_not_called()


if __name__ == "__main__":
    unittest.main()
