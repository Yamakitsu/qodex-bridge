"""消息路由、命令分发、Codex 事件处理。"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import shutil
from collections import OrderedDict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import aiohttp

from . import commands
from . import projects as projects_mod
from . import state as state_mod
from .appserver import AppServerClient
from .config import Config
from .onebot import (
    IncomingAttachment,
    IncomingPrivateMessage,
    OneBotClient,
    parse_private_message,
)

LOGGER = logging.getLogger(__name__)

MAX_QQ_MSG_LEN = 1500
MAX_FAIL_OUTPUT_LEN = 500
MAX_ATTACHMENT_BYTES = 100 * 1024 * 1024
MAX_ATTACHMENT_MESSAGE_INDEX = 200
FULL_CONFIRM_TIMEOUT_SEC = 30.0
WEBUI_USER_ID = "__webui__"


def qq_plain_text(text: str) -> str:
    """把常见 Markdown 标记降级为 QQ 纯文本；WebUI 仍保留原文。"""

    plain = text.replace("\r\n", "\n")
    plain = re.sub(r"(?m)^\s*```[^\n]*$", "", plain)
    plain = re.sub(r"!\[([^\]]*)\]\(([^)]+)\)", r"图片：\1（\2）", plain)
    plain = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r"\1（\2）", plain)
    plain = re.sub(r"(?m)^\s{0,3}#{1,6}\s+", "", plain)
    plain = re.sub(r"(?m)^\s{0,3}>\s?", "", plain)
    plain = re.sub(r"(?<!\\)(\*\*|__)(.+?)\1", r"\2", plain)
    plain = re.sub(r"`([^`\n]+)`", r"\1", plain)
    plain = re.sub(r"(?m)^\s*([-*_])(?:\s*\1){2,}\s*$", "", plain)
    plain = re.sub(r"\n{3,}", "\n\n", plain)
    return plain.strip()


@dataclass
class ApprovalRequest:
    request_id: int
    kind: str  # command | file | permissions
    params: dict[str, Any]
    decision_future: asyncio.Future = field(default_factory=lambda: asyncio.get_running_loop().create_future())
    timeout_task: asyncio.Task | None = None
    prompt_text: str = ""  # 发给用户的审批提示原文，供 WebUI 重连时恢复卡片


@dataclass
class QueuedMessage:
    context: "RouteContext"
    text: str

    @property
    def user_id(self) -> str:
        return self.context.sender_id


@dataclass(frozen=True)
class RouteContext:
    """一条 QQ/WebUI 消息的身份、回复目标和会话作用域。"""

    scope_key: str
    sender_id: str
    chat_type: str = "private"  # private | group | webui
    group_id: str | None = None

    @property
    def is_webui(self) -> bool:
        return self.chat_type == "webui"

    def is_admin(self, config: Config) -> bool:
        return self.is_webui or self.sender_id in config.admins

    def is_admin_private(self, config: Config) -> bool:
        return self.is_webui or (self.chat_type == "private" and self.sender_id in config.admins)

    def to_dict(self) -> dict[str, str | None]:
        return {
            "scope_key": self.scope_key,
            "sender_id": self.sender_id,
            "chat_type": self.chat_type,
            "group_id": self.group_id,
        }

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "RouteContext":
        sender_id = str(raw.get("sender_id") or raw.get("user_id") or "")
        chat_type = str(raw.get("chat_type") or "private")
        group_id = str(raw["group_id"]) if raw.get("group_id") is not None else None
        scope_key = str(
            raw.get("scope_key")
            or (f"group:{group_id}" if chat_type == "group" and group_id else f"private:{sender_id}")
        )
        return cls(scope_key, sender_id, chat_type, group_id)


WEBUI_CONTEXT = RouteContext("webui", WEBUI_USER_ID, "webui")


class Orchestrator:
    def __init__(self, config: Config, state_path: str | Path | None = None):
        self.config = config
        self.state_path = state_path
        self._registry = state_mod.load_registry(state_path)
        self._active_context = WEBUI_CONTEXT
        self.state = self._registry.sessions.setdefault(
            WEBUI_CONTEXT.scope_key,
            state_mod.State(project=config.default_project, model=config.default_model),
        )

        self.appserver = AppServerClient(
            config.codex_path,
            on_notification=self._on_notification,
            on_server_request=self._on_server_request,
            on_close=self._request_shutdown,
        )
        self.onebot = OneBotClient(
            config.ws_url,
            config.access_token,
            on_private_message=self._on_private_message,
            on_close=self._request_shutdown,
        )
        self.webui: Any | None = None

        self._busy = False
        self._current_thread_id: str | None = None
        self._current_turn_id: str | None = None
        self._current_user_id: RouteContext | None = None
        self._thread_loaded = False
        self._queue: deque[QueuedMessage] = deque(
            QueuedMessage(context=RouteContext.from_dict(q), text=str(q.get("text") or ""))
            for q in self._registry.queue
            if isinstance(q, dict)
        )
        self._pending_approvals: deque[ApprovalRequest] = deque()
        self._pending_interrupt_msg: QueuedMessage | None = None
        self._shutdown_event = asyncio.Event()
        self._input_lock = asyncio.Lock()
        self._incoming_messages: asyncio.Queue[IncomingPrivateMessage] = asyncio.Queue()
        self._incoming_worker: asyncio.Task | None = None
        self._attachment_message_index: OrderedDict[str, list[str]] = OrderedDict()

    # ------------------------------------------------------------------ #
    # 生命周期
    # ------------------------------------------------------------------ #
    async def start(self) -> None:
        await self.appserver.start()
        self._incoming_worker = asyncio.create_task(
            self._consume_private_messages(), name="qq-incoming"
        )
        await self.onebot.start()
        # 恢复当前 thread_id
        self._current_thread_id = self.state.thread_id
        LOGGER.info("桥接器已启动，当前 project=%s thread=%s mode=%s", self.state.project, self.state.thread_id, self.state.mode)

    async def run(self) -> None:
        await self.start()
        await self._shutdown_event.wait()

    async def shutdown(self) -> None:
        LOGGER.info("正在关闭桥接器...")
        if self._incoming_worker is not None:
            self._incoming_worker.cancel()
            await asyncio.gather(self._incoming_worker, return_exceptions=True)
            self._incoming_worker = None
        # 取消所有待审批
        for approval in self._pending_approvals:
            if approval.timeout_task is not None:
                approval.timeout_task.cancel()
            if not approval.decision_future.done():
                approval.decision_future.set_exception(RuntimeError("桥接器关闭"))
        self._pending_approvals.clear()
        await self.onebot.shutdown()
        await self.appserver.shutdown()

    def _request_shutdown(self) -> None:
        self._shutdown_event.set()

    def _save_state(self) -> None:
        self.state.thread_id = self._current_thread_id
        self._registry.sessions[self._active_context.scope_key] = self.state
        self._registry.queue = [
            {**m.context.to_dict(), "text": m.text} for m in self._queue
        ]
        state_mod.save_registry(self._registry, self.state_path)

    def _activate_context(self, context: RouteContext) -> None:
        """切换当前串行执行槽到指定会话，并恢复该会话自己的 thread。"""

        if context.scope_key == self._active_context.scope_key:
            if not self._busy:
                self._active_context = context
            return
        if self._busy:
            raise RuntimeError("当前 turn 执行期间不能切换到其他会话")
        self._save_state()
        self._active_context = context
        self.state = self._registry.sessions.setdefault(
            context.scope_key,
            state_mod.State(
                project=self._default_project_for_context(context),
                model=self._default_model_for_context(context),
                mode="full" if context.is_admin_private(self.config) else "safe",
            ),
        )
        if not context.is_admin_private(self.config):
            self.state.mode = "safe"
            self.state.pending_full_confirm_until = None
        self._current_thread_id = self.state.thread_id
        self._current_turn_id = None
        self._thread_loaded = False

    def _default_project_for_context(self, context: RouteContext) -> str | None:
        if context.is_webui or context.is_admin_private(self.config):
            return self.config.default_project
        return self._ensure_auto_project(context)

    def _default_model_for_context(self, context: RouteContext) -> str | None:
        if context.is_admin_private(self.config):
            return self.config.default_model
        return self.config.routing.public_model

    def _ensure_auto_project(self, context: RouteContext) -> str | None:
        routing = self.config.routing
        if not routing.auto_create_projects or not routing.project_root:
            return self.config.default_project

        root = Path(routing.project_root).resolve()
        if context.chat_type == "group":
            if not context.group_id or not re.fullmatch(r"\d{1,32}", context.group_id):
                raise ValueError("无效 QQ 群号，拒绝创建 project")
            folder = root / "projects" / "groups" / f"g_{context.group_id}"
            project_name = f"qq_group_{context.group_id}"
        else:
            if not re.fullmatch(r"\d{1,32}", context.sender_id):
                raise ValueError("无效 QQ 号，拒绝创建 project")
            folder = root / "projects" / "private" / f"u_{context.sender_id}"
            project_name = f"qq_user_{context.sender_id}"
        folder.mkdir(parents=True, exist_ok=True)

        agents_target = folder / "AGENTS.md"
        if not agents_target.exists():
            template = (
                Path(routing.default_agents_file)
                if routing.default_agents_file
                else root / "templates" / "default" / "AGENTS.md"
            )
            if not template.exists():
                template.parent.mkdir(parents=True, exist_ok=True)
                template.write_text(
                    "# 默认 QQ 助手\n\n请以清晰、友善、可靠的方式回答用户问题。\n",
                    encoding="utf-8",
                )
            shutil.copyfile(template, agents_target)

        resolved_folder = str(folder.resolve())
        existing = self.config.projects.get(project_name)
        if existing and str(Path(existing).resolve()) != resolved_folder:
            raise RuntimeError(f"自动 project 名称冲突: {project_name}")
        self.config.projects[project_name] = resolved_folder
        return project_name

    def register_webui(self, webui: Any) -> None:
        self.webui = webui

    def _notify_status_change(self) -> None:
        if self.webui is not None:
            self.webui.on_status_change()

    # ------------------------------------------------------------------ #
    # 消息入口（QQ / WebUI 共用）
    # ------------------------------------------------------------------ #
    def _on_private_message(self, message: IncomingPrivateMessage) -> None:
        if message.chat_type == "group":
            if not message.group_id or message.group_id not in self.config.group_whitelist:
                LOGGER.info("群 %s 不在群白名单，忽略", message.group_id)
                return
            if not message.mentioned_bot:
                LOGGER.debug("群 %s 未 @机器人，忽略", message.group_id)
                return
        elif message.user_id not in self.config.whitelist and message.user_id not in self.config.admins:
            LOGGER.info("QQ %s 不在私聊白名单，忽略", message.user_id)
            return
        self._incoming_messages.put_nowait(message)

    async def _consume_private_messages(self) -> None:
        """串行归档 QQ 附件，保证“发图后立即引用”不会发生竞态。"""

        while True:
            message = await self._incoming_messages.get()
            try:
                await self._handle_private_message(message)
            except asyncio.CancelledError:
                raise
            except Exception:
                LOGGER.exception("QQ 私聊消息处理失败")
            finally:
                self._incoming_messages.task_done()

    async def _handle_private_message(self, message: IncomingPrivateMessage) -> None:
        context = self._context_from_message(message)
        project_cwd = self._cwd_for_context(context)
        archived_paths = await self._archive_attachments(
            message.user_id, message.attachments, project_cwd
        )
        if message.message_id and archived_paths:
            self._remember_message_attachments(message.message_id, archived_paths)

        referenced_paths: list[str] = []
        if message.reply_to_message_id:
            referenced_paths = list(
                self._attachment_message_index.get(message.reply_to_message_id, [])
            )
            if not referenced_paths:
                referenced_paths = await self._load_replied_attachments(
                    message.user_id, message.reply_to_message_id, project_cwd
                )

        all_paths = list(dict.fromkeys([*referenced_paths, *archived_paths]))
        display_parts: list[str] = []
        if message.text:
            display_parts.append(message.text)
        if all_paths:
            display_parts.append("附件：\n" + "\n".join(all_paths))
        display_text = "\n\n".join(display_parts) or "附件消息"
        if self.webui is not None:
            self.webui.on_message("in", f"QQ {message.user_id}", display_text)

        # 图片/附件单独发送时只归档，不启动 Codex，也不向 QQ 回话。
        if not message.text:
            LOGGER.info("QQ %s 的纯附件消息已静默归档", message.user_id)
            return

        prompt = message.text
        if all_paths:
            prompt += "\n\n用户通过 QQ 上传或引用的附件已归档到以下绝对路径，请按用户要求读取：\n"
            prompt += "\n".join(all_paths)
        await self._process_input(context, prompt)

    @staticmethod
    def _context_from_message(message: IncomingPrivateMessage) -> RouteContext:
        if message.chat_type == "group" and message.group_id:
            return RouteContext(
                scope_key=f"group:{message.group_id}",
                sender_id=message.user_id,
                chat_type="group",
                group_id=message.group_id,
            )
        return RouteContext(
            scope_key=f"private:{message.user_id}",
            sender_id=message.user_id,
            chat_type="private",
        )

    def _state_for_context(self, context: RouteContext) -> state_mod.State:
        return self._registry.sessions.setdefault(
            context.scope_key,
            state_mod.State(
                project=self._default_project_for_context(context),
                model=self._default_model_for_context(context),
                mode="full" if context.is_admin_private(self.config) else "safe",
            ),
        )

    def _cwd_for_context(self, context: RouteContext) -> str:
        state = self._state_for_context(context)
        return self.config.projects.get(state.project, str(Path.cwd()))

    def _remember_message_attachments(self, message_id: str, paths: list[str]) -> None:
        self._attachment_message_index[message_id] = list(paths)
        self._attachment_message_index.move_to_end(message_id)
        while len(self._attachment_message_index) > MAX_ATTACHMENT_MESSAGE_INDEX:
            self._attachment_message_index.popitem(last=False)

    async def _load_replied_attachments(
        self, user_id: str, message_id: str, project_cwd: str
    ) -> list[str]:
        """引用跨进程或超出内存索引时，通过 OneBot get_msg 回查原附件。"""

        try:
            data = await self.onebot.call_action("get_msg", {"message_id": message_id})
            event = dict(data)
            event.setdefault("user_id", user_id)
            event.setdefault("sender", {"user_id": user_id})
            event.setdefault("message_id", message_id)
            original = parse_private_message(event)
            paths = await self._archive_attachments(user_id, original.attachments, project_cwd)
            if paths:
                self._remember_message_attachments(message_id, paths)
            return paths
        except Exception as exc:
            LOGGER.warning("回查引用消息 %s 的附件失败: %s", message_id, exc)
            return []

    async def _archive_attachments(
        self,
        user_id: str,
        attachments: list[IncomingAttachment],
        project_cwd: str | None = None,
    ) -> list[str]:
        paths: list[str] = []
        for attachment in attachments:
            try:
                archived = await self._archive_attachment(user_id, attachment, project_cwd)
            except Exception as exc:
                LOGGER.warning("附件归档失败 (%s): %s", attachment.file_name, exc)
                continue
            paths.append(str(archived.resolve()))
        return paths

    async def _archive_attachment(
        self, user_id: str, attachment: IncomingAttachment, project_cwd: str | None = None
    ) -> Path:
        now = datetime.now().astimezone()
        attachment_root = Path(project_cwd or self._current_cwd()) / "attachments" / now.strftime("%Y-%m-%d")
        attachment_root.mkdir(parents=True, exist_ok=True)

        safe_name = Path(attachment.file_name.replace("\\", "/")).name
        safe_name = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", safe_name).strip(" .")
        if not safe_name:
            safe_name = attachment.kind
        if not Path(safe_name).suffix:
            default_suffix = {"image": ".jpg", "record": ".amr", "video": ".mp4"}.get(
                attachment.kind, ".bin"
            )
            safe_name += default_suffix
        timestamp = now.strftime("%H%M%S-%f")[:-3]
        target = attachment_root / f"{timestamp}_{safe_name}"
        duplicate = 1
        while target.exists():
            target = attachment_root / f"{timestamp}_{duplicate}_{safe_name}"
            duplicate += 1

        local_path = attachment.local_path
        url = attachment.url
        if local_path and Path(local_path).is_file():
            shutil.copy2(local_path, target)
            return target

        if not url:
            lookup_data: dict[str, Any] = {}
            if attachment.kind == "file" and attachment.file_id:
                params: dict[str, Any] = {"user_id": user_id, "file_id": attachment.file_id}
                if attachment.file_hash:
                    params["file_hash"] = attachment.file_hash
                lookup_data = await self.onebot.call_action("get_private_file_url", params)
            elif attachment.kind == "image":
                lookup_data = await self.onebot.call_action("get_image", {"file": attachment.file_name})
            elif attachment.kind == "record":
                lookup_data = await self.onebot.call_action(
                    "get_record",
                    {"file": attachment.file_name, "out_format": "mp3"},
                )
            url = str(
                lookup_data.get("url")
                or lookup_data.get("file_url")
                or lookup_data.get("download_url")
                or ""
            )
            lookup_path = lookup_data.get("file") or lookup_data.get("path")
            if lookup_path and Path(str(lookup_path)).is_file():
                shutil.copy2(str(lookup_path), target)
                return target

        if not url or not url.lower().startswith(("http://", "https://")):
            raise RuntimeError("OneBot 未提供可下载的 HTTP(S) 地址")

        timeout = aiohttp.ClientTimeout(total=60)
        temp_target = target.with_name(target.name + ".part")
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url) as response:
                    response.raise_for_status()
                    size = 0
                    with temp_target.open("wb") as output:
                        async for chunk in response.content.iter_chunked(1024 * 256):
                            size += len(chunk)
                            if size > MAX_ATTACHMENT_BYTES:
                                raise RuntimeError("附件超过 100 MiB 限制")
                            output.write(chunk)
            temp_target.replace(target)
            return target
        finally:
            if temp_target.exists():
                temp_target.unlink()

    async def _process_input(self, user_id: str | RouteContext, text: str) -> None:
        """处理来自 QQ 或 WebUI 的输入。"""
        context = user_id if isinstance(user_id, RouteContext) else RouteContext(
            f"private:{user_id}", str(user_id), "private"
        )
        command, args = commands.parse(text)

        # 优先处理审批应答
        if self._pending_approvals and command in ("yes", "no"):
            if not context.is_admin_private(self.config) or context.scope_key != self._active_context.scope_key:
                await self._send_text(context, "只有当前管理员私聊可以处理审批。")
                return
            asyncio.create_task(self._handle_approval_reply(context, command == "yes"))
            return

        # app-server 当前仍串行工作。跨会话的命令也必须排队，避免操作到别人的状态。
        if self._busy and not (
            context.scope_key == self._active_context.scope_key and command in ("stop", "interrupt")
        ):
            async with self._input_lock:
                self._queue.append(QueuedMessage(context, text))
                self._save_state()
                self._notify_status_change()
                position = len(self._queue)
            await self._send_text(context, f"Codex 正在处理中，已排队（第 {position} 位）。")
            return

        self._activate_context(context)

        if command:
            if not self._command_allowed(context, command):
                await self._send_text(context, "当前身份不能使用该命令。发送 /list 查看可用命令。")
                return
            await self._handle_command(context, command, args)
            return

        # 普通消息
        await self._handle_user_input(context, text)

    def _command_allowed(self, context: RouteContext, command: str) -> bool:
        public_commands = {"list", "status", "new", "stop", "interrupt", "effort"}
        if context.is_admin(self.config):
            # full 只存在于管理员私聊/WebUI；群聊不能通过命令改变执行沙箱。
            return command != "mode" or context.is_admin_private(self.config)
        return command in public_commands

    def inject_prompt(self, text: str) -> None:
        """WebUI 调用：把消息注入 orchestrator 管线。"""
        asyncio.create_task(self._process_input(WEBUI_CONTEXT, text))

    async def switch_project(self, name: str) -> bool:
        if self._busy and self._active_context.scope_key != WEBUI_CONTEXT.scope_key:
            return False
        self._activate_context(WEBUI_CONTEXT)
        await self._cmd_project(WEBUI_CONTEXT, name)
        return True

    async def switch_thread(self, idx: int) -> bool:
        if self._busy and self._active_context.scope_key != WEBUI_CONTEXT.scope_key:
            return False
        self._activate_context(WEBUI_CONTEXT)
        await self._cmd_thread(WEBUI_CONTEXT, str(idx))
        return True

    async def switch_model(self, name: str) -> bool:
        if self._busy and self._active_context.scope_key != WEBUI_CONTEXT.scope_key:
            return False
        self._activate_context(WEBUI_CONTEXT)
        await self._cmd_model(WEBUI_CONTEXT, name)
        return True

    async def switch_mode(self, mode: str) -> bool:
        if self._busy and self._active_context.scope_key != WEBUI_CONTEXT.scope_key:
            return False
        self._activate_context(WEBUI_CONTEXT)
        await self._cmd_mode(WEBUI_CONTEXT, mode)
        return True

    async def approve(self) -> bool:
        if not self._pending_approvals:
            return False
        await self._resolve_head_approval(accepted=True)
        return True

    async def deny(self) -> bool:
        if not self._pending_approvals:
            return False
        await self._resolve_head_approval(accepted=False)
        return True

    # ------------------------------------------------------------------ #
    # WebUI 直连方法（结构化返回，不走 QQ 命令文本）
    # ------------------------------------------------------------------ #
    async def list_threads(self, project: str | None = None) -> list[dict[str, Any]]:
        if project is not None:
            cwd = self.config.projects.get(project)
            if cwd is None:
                return []
        else:
            cwd = self._current_cwd()
        result = await self.appserver.call("thread/list", cwd=cwd, limit=50)
        return result.get("threads") or result.get("data") or []

    async def add_project(self, name: str, path: str) -> tuple[bool, str]:
        name = name.strip()
        path = path.strip()
        if not name:
            return False, "project 名称不能为空"
        if name in self.config.projects:
            return False, f"project 已存在: {name}"
        if not path:
            return False, "路径不能为空"
        p = Path(path).expanduser()
        if not p.is_dir():
            return False, f"目录不存在: {path}"
        resolved = str(p.resolve())
        self.config.projects[name] = resolved
        overlay = projects_mod.load_overlay()
        overlay[name] = resolved
        try:
            projects_mod.save_overlay(overlay)
        except Exception as exc:
            del self.config.projects[name]
            return False, f"保存失败: {exc}"
        self._notify_status_change()
        return True, f"已添加 project: {name}"

    async def list_models(self) -> list[dict[str, Any]]:
        result = await self.appserver.call("model/list")
        return result.get("models") or result.get("data") or []

    async def resume_thread(self, thread_id: str) -> tuple[bool, str, list[dict[str, str]]]:
        if self._busy:
            return False, "当前 turn 执行中，不能切换 thread", []
        try:
            result = await self.appserver.call("thread/resume", threadId=thread_id)
        except Exception as exc:
            return False, f"切换 thread 失败: {exc}", []
        self._current_thread_id = thread_id
        self._thread_loaded = True
        self.state.thread_id = thread_id
        self._save_state()
        self._notify_status_change()
        thread = result.get("thread") or {}
        return True, thread_id, self._extract_history(thread)

    @staticmethod
    def _extract_history(thread: dict[str, Any]) -> list[dict[str, str]]:
        """从 thread.turns 提取简化对话历史（仅用户/助手文本消息）。"""
        history: list[dict[str, str]] = []
        for turn in thread.get("turns") or []:
            for item in turn.get("items") or []:
                itype = item.get("type")
                if itype == "userMessage":
                    parts = [
                        c.get("text", "")
                        for c in item.get("content") or []
                        if isinstance(c, dict) and c.get("type") == "text"
                    ]
                    text = "\n".join(p for p in parts if p)
                    if text:
                        history.append({"kind": "in", "text": text})
                elif itype == "agentMessage":
                    text = item.get("text") or ""
                    if text:
                        history.append({"kind": "out", "text": text})
        return history

    async def new_thread(self) -> tuple[bool, str]:
        if self._busy:
            return False, "当前 turn 执行中，不能新建 thread"
        try:
            params: dict[str, Any] = {"cwd": self._current_cwd()}
            if self.state.model:
                params["model"] = self.state.model
            if self.state.effort:
                params["effort"] = self.state.effort
            result = await self.appserver.call("thread/start", **params)
        except Exception as exc:
            return False, f"新建 thread 失败: {exc}"
        thread = result.get("thread", {})
        self._current_thread_id = thread.get("id")
        self._thread_loaded = True
        self.state.thread_id = self._current_thread_id
        self._save_state()
        self._notify_status_change()
        return True, str(self._current_thread_id)

    async def set_model(self, name: str) -> tuple[bool, str]:
        name = name.strip()
        if not name:
            self.state.model = None
            self._save_state()
            self._notify_status_change()
            return True, "已恢复默认模型"
        try:
            models = await self.list_models()
            known = {str(m.get("id") or m.get("name")) for m in models}
        except Exception:
            known = set()
        if known and name not in known:
            return False, f"未知模型: {name}"
        self.state.model = name
        self._save_state()
        self._notify_status_change()
        return True, f"已切换模型到: {name}"

    async def set_effort(self, effort: str) -> tuple[bool, str]:
        effort = effort.strip()
        if not effort:
            self.state.effort = None
            self._save_state()
            self._notify_status_change()
            return True, "已恢复默认 effort"
        try:
            models = await self.list_models()
        except Exception as exc:
            return False, f"无法校验 effort 档位: {exc}"
        current = None
        for m in models:
            if str(m.get("id") or m.get("name")) == (self.state.model or ""):
                current = m
                break
        if current is None:
            for m in models:
                if m.get("isDefault"):
                    current = m
                    break
            if current is None and models:
                current = models[0]
        valid = [
            e.get("reasoningEffort") if isinstance(e, dict) else e
            for e in (current or {}).get("supportedReasoningEfforts") or []
        ]
        if valid and effort not in valid:
            return False, f"无效档位: {effort}，可用: {', '.join(valid)}"
        self.state.effort = effort
        self._save_state()
        self._notify_status_change()
        return True, f"已切换 effort 到: {effort}"

    async def set_mode(self, mode: str) -> tuple[bool, str]:
        """WebUI 直接设置模式（前端已做二次确认）。"""
        if mode not in ("safe", "full"):
            return False, "mode 必须是 safe 或 full"
        self.state.mode = mode
        self.state.pending_full_confirm_until = None
        self._save_state()
        self._notify_status_change()
        desc = "workspaceWrite，越权操作需审批" if mode == "safe" else "dangerFullAccess，不再审批"
        return True, f"已切换到 {mode} 模式（{desc}）"

    def get_queue(self) -> list[str]:
        return [m.text for m in self._queue]

    async def queue_pop(self, idx: int) -> tuple[bool, str]:
        async with self._input_lock:
            if idx < 0 or idx >= len(self._queue):
                return False, "序号无效"
            removed = self._queue[idx]
            del self._queue[idx]
            self._save_state()
            self._notify_status_change()
        return True, removed.text

    async def queue_clear(self) -> None:
        async with self._input_lock:
            self._queue.clear()
            self._save_state()
            self._notify_status_change()

    async def stop_turn(self) -> tuple[bool, str]:
        if not self._current_turn_id or not self._current_thread_id:
            return False, "当前没有执行中的 turn"
        try:
            await self.appserver.call(
                "turn/interrupt",
                threadId=self._current_thread_id,
                turnId=self._current_turn_id,
            )
        except Exception as exc:
            return False, f"中断失败: {exc}"
        return True, "已请求中断当前 turn"

    async def interrupt_with(self, text: str) -> None:
        await self._cmd_interrupt(WEBUI_USER_ID, text)

    def _approve_result(self, approval: ApprovalRequest) -> dict[str, Any]:
        if approval.kind == "permissions":
            return {"scope": "turn", "permissions": approval.params.get("permissions") or {}}
        return {"decision": "accept"}

    def _decline_result(self, approval: ApprovalRequest) -> dict[str, Any]:
        if approval.kind == "permissions":
            return {"scope": "turn", "permissions": {}}
        return {"decision": "decline"}

    async def _resolve_head_approval(self, accepted: bool) -> None:
        head = self._pending_approvals[0]
        result = self._approve_result(head) if accepted else self._decline_result(head)
        reason = "用户批准" if accepted else "用户拒绝"
        await self._resolve_approval(head, result, reason=reason)

    # ------------------------------------------------------------------ #
    # 普通输入 / 队列
    # ------------------------------------------------------------------ #
    async def _handle_user_input(self, user_id: RouteContext, text: str) -> None:
        # 处理 full 模式二次确认
        if user_id.is_admin_private(self.config) and state_mod.is_pending_full_confirm(
            self.state, FULL_CONFIRM_TIMEOUT_SEC
        ):
            if text.strip() == "确认":
                self.state.mode = "full"
                self.state.pending_full_confirm_until = None
                self._save_state()
                await self._send_text(user_id, "已切换到 full 模式（dangerFullAccess，不审批）。")
            else:
                self.state.pending_full_confirm_until = None
                self._save_state()
                await self._send_text(user_id, "未收到确认，已取消模式切换。")
            return

        async with self._input_lock:
            if self._busy:
                self._queue.append(QueuedMessage(user_id, text))
                self._save_state()
                self._notify_status_change()
                await self._send_text(user_id, f"Codex 正在处理中，已排队（第 {len(self._queue)} 位）。")
                return
            self._busy = True
            self._notify_status_change()

        try:
            await self._start_turn(user_id, text)
        except Exception:
            self._busy = False
            self._notify_status_change()
            raise

    async def _start_turn(self, user_id: RouteContext, text: str) -> None:
        self._activate_context(user_id)
        try:
            await self._ensure_thread()
        except Exception as exc:
            self._busy = False
            self._notify_status_change()
            LOGGER.exception("确保 thread 失败: %s", exc)
            await self._send_text(user_id, f"无法加载 thread: {exc}")
            return

        if not self._current_thread_id:
            self._busy = False
            self._notify_status_change()
            await self._send_text(user_id, "当前没有可用 thread，请检查 project 配置。")
            return

        params: dict[str, Any] = {
            "threadId": self._current_thread_id,
            "input": [{"type": "text", "text": text}],
            "cwd": self._current_cwd(),
        }
        if self.state.model:
            params["model"] = self.state.model
        if self.state.effort:
            params["effort"] = self.state.effort

        full_allowed = user_id.is_admin_private(self.config) and self.state.mode == "full"
        if full_allowed:
            params["approvalPolicy"] = "never"
            params["sandboxPolicy"] = {"type": "dangerFullAccess"}
        else:
            writable_roots = [self._current_cwd()]
            if user_id.is_admin_private(self.config):
                for root in self.config.extra_writable_roots:
                    if root not in writable_roots:
                        writable_roots.append(root)
            # 只有管理员私聊的 safe 模式可以交互审批；其余会话失败即关闭。
            params["approvalPolicy"] = (
                "on-request" if user_id.is_admin_private(self.config) else "never"
            )
            params["sandboxPolicy"] = {
                "type": "workspaceWrite",
                "writableRoots": writable_roots,
                "networkAccess": True,
            }

        try:
            result = await self.appserver.call("turn/start", **params)
            turn = result.get("turn", {})
            self._current_turn_id = turn.get("id")
            self._current_user_id = user_id
            self._busy = True
            self._notify_status_change()
            LOGGER.info("turn 已启动 %s", self._current_turn_id)
        except Exception as exc:
            self._busy = False
            self._notify_status_change()
            LOGGER.exception("turn/start 失败: %s", exc)
            await self._send_text(user_id, f"turn/start 失败: {exc}")

    async def _ensure_thread(self) -> None:
        """保证 current_thread_id 可用。"""
        if self._thread_loaded and self._current_thread_id:
            return

        project_cwd = self._current_cwd()

        if self._current_thread_id:
            try:
                result = await self.appserver.call("thread/resume", threadId=self._current_thread_id)
                thread = result.get("thread") or {}
                thread_cwd = thread.get("cwd") or thread.get("path")
                if thread_cwd and str(Path(thread_cwd).resolve()) != str(Path(project_cwd).resolve()):
                    raise RuntimeError("thread 不属于当前 project")
                self._thread_loaded = True
                LOGGER.info("恢复 thread %s", self._current_thread_id)
                return
            except Exception as exc:
                LOGGER.warning("恢复 thread %s 失败: %s，将新建", self._current_thread_id, exc)
                self._current_thread_id = None
                self.state.thread_id = None

        params: dict[str, Any] = {"cwd": project_cwd}
        if self.state.model:
            params["model"] = self.state.model
        if self.state.effort:
            params["effort"] = self.state.effort

        result = await self.appserver.call("thread/start", **params)
        thread = result.get("thread", {})
        self._current_thread_id = thread.get("id")
        self._thread_loaded = True
        self.state.thread_id = self._current_thread_id
        self._save_state()
        LOGGER.info("新建 thread %s", self._current_thread_id)

    def _current_cwd(self) -> str:
        project = self.state.project or self.config.default_project
        return self.config.projects.get(project, str(Path.cwd()))

    def _current_project_name(self) -> str:
        return self.state.project or self.config.default_project or "未设置"

    # ------------------------------------------------------------------ #
    # 命令处理
    # ------------------------------------------------------------------ #
    async def _handle_command(self, user_id: RouteContext, command: str, args: str) -> None:
        if command == "list":
            lines = [
                commands.help_text(
                    admin=user_id.is_admin(self.config),
                    admin_private=user_id.is_admin_private(self.config),
                ),
                "",
                f"当前 project: {self._current_project_name()}",
                f"当前 thread: {self._current_thread_id or '无'}",
                f"当前 model: {self.state.model or '默认'}",
                f"当前 effort: {self.state.effort or '默认'}",
                f"当前 mode: {self.state.mode}",
                f"队列长度: {len(self._queue)}",
            ]
            await self._send_text(user_id, "\n".join(lines))
            return

        if command == "status":
            if self._pending_approvals:
                status = "待审批"
            elif self._busy:
                status = "执行中"
            else:
                status = "空闲"
            lines = [
                f"状态: {status}",
                f"project: {self._current_project_name()}",
                f"thread: {self._current_thread_id or '无'}",
                f"turn: {self._current_turn_id or '无'}",
                f"model: {self.state.model or '默认'}",
                f"effort: {self.state.effort or '默认'}",
                f"mode: {self.state.mode}",
                f"队列: {len(self._queue)}",
            ]
            await self._send_text(user_id, "\n".join(lines))
            return

        if command == "new":
            try:
                params: dict[str, Any] = {"cwd": self._current_cwd()}
                if self.state.model:
                    params["model"] = self.state.model
                if self.state.effort:
                    params["effort"] = self.state.effort
                result = await self.appserver.call("thread/start", **params)
                thread = result.get("thread", {})
                self._current_thread_id = thread.get("id")
                self._thread_loaded = True
                self.state.thread_id = self._current_thread_id
                self._save_state()
                await self._send_text(user_id, f"已新建 thread: {self._current_thread_id}")
            except Exception as exc:
                LOGGER.exception("新建 thread 失败")
                await self._send_text(user_id, f"新建 thread 失败: {exc}")
            return

        if command == "stop":
            if not self._current_turn_id or not self._current_thread_id:
                await self._send_text(user_id, "当前没有执行中的 turn。")
                return
            try:
                await self.appserver.call(
                    "turn/interrupt",
                    threadId=self._current_thread_id,
                    turnId=self._current_turn_id,
                )
                await self._send_text(user_id, "已请求中断当前 turn。")
            except Exception as exc:
                LOGGER.exception("turn/interrupt 失败")
                await self._send_text(user_id, f"中断失败: {exc}")
            return

        if command == "project":
            await self._cmd_project(user_id, args.strip())
            return

        if command == "thread":
            await self._cmd_thread(user_id, args.strip())
            return

        if command == "model":
            await self._cmd_model(user_id, args.strip())
            return

        if command == "effort":
            await self._cmd_effort(user_id, args.strip())
            return

        if command == "mode":
            await self._cmd_mode(user_id, args.strip())
            return

        if command == "queue":
            await self._cmd_queue(user_id, args.strip())
            return

        if command == "interrupt":
            await self._cmd_interrupt(user_id, args.strip())
            return

        if command in ("yes", "no"):
            await self._send_text(user_id, "当前没有待审批操作。")
            return

        await self._send_text(user_id, commands.unknown_command())

    async def _cmd_project(self, user_id: str, name: str) -> None:
        if not name:
            lines = ["可用 projects："] + [f"- {k}: {v}" for k, v in self.config.projects.items()]
            lines.append(f"当前: {self._current_project_name()}")
            await self._send_text(user_id, "\n".join(lines))
            return

        if name.lower().startswith("create "):
            project_name = name.split(None, 1)[1].strip()
            ok, message = self._create_managed_project(project_name)
            await self._send_text(user_id, message)
            if ok:
                self._notify_status_change()
            return

        if name not in self.config.projects:
            await self._send_text(user_id, f"未知 project: {name}。发送 /project 查看列表。")
            return

        self.state.project = name
        self._current_thread_id = None
        self._thread_loaded = False
        self.state.thread_id = None

        # 尝试恢复该 project 下最近 thread
        cwd = self.config.projects[name]
        thread_id = await self._find_latest_thread(cwd)
        if thread_id:
            try:
                await self.appserver.call("thread/resume", threadId=thread_id)
                self._current_thread_id = thread_id
                self._thread_loaded = True
                self.state.thread_id = thread_id
                await self._send_text(user_id, f"已切换 project 到 {name}，并恢复 thread {thread_id}")
            except Exception as exc:
                LOGGER.warning("恢复 project %s 的 thread %s 失败: %s", name, thread_id, exc)
                self._current_thread_id = None
                self._thread_loaded = False
                self.state.thread_id = None
                await self._send_text(user_id, f"已切换 project 到 {name}，历史 thread 无法恢复，下条消息将自动新建。")
        else:
            await self._send_text(user_id, f"已切换 project 到 {name}，下条消息将自动新建 thread。")

        self._save_state()
        self._notify_status_change()

    def _create_managed_project(self, name: str) -> tuple[bool, str]:
        if not re.fullmatch(r"[\w\-]{1,64}", name, flags=re.UNICODE):
            return False, "project 名称只能包含文字、数字、下划线或连字符，最长 64 字符。"
        if name in self.config.projects:
            return False, f"project 已存在: {name}"
        root_value = self.config.routing.project_root
        if not root_value:
            return False, "未配置 routing.project_root，无法从 QQ 创建 project。"
        root = Path(root_value).resolve()
        target = (root / "projects" / "custom" / name).resolve()
        expected_parent = (root / "projects" / "custom").resolve()
        if target.parent != expected_parent:
            return False, "project 路径校验失败。"
        try:
            target.mkdir(parents=True, exist_ok=False)
        except FileExistsError:
            return False, f"目录已存在但尚未登记为 project: {target}"
        except OSError as exc:
            return False, f"创建 project 目录失败: {exc}"
        template = (
            Path(self.config.routing.default_agents_file)
            if self.config.routing.default_agents_file
            else root / "templates" / "default" / "AGENTS.md"
        )
        if not template.exists():
            template.parent.mkdir(parents=True, exist_ok=True)
            template.write_text(
                "# 默认 QQ 助手\n\n请以清晰、友善、可靠的方式回答用户问题。\n",
                encoding="utf-8",
            )
        shutil.copyfile(template, target / "AGENTS.md")
        self.config.projects[name] = str(target)
        overlay = projects_mod.load_overlay()
        overlay[name] = str(target)
        try:
            projects_mod.save_overlay(overlay)
        except Exception as exc:
            self.config.projects.pop(name, None)
            return False, f"project 已创建但登记失败: {exc}"
        return True, f"已创建 project: {name}（{target}）"

    async def _find_latest_thread(self, cwd: str) -> str | None:
        try:
            result = await self.appserver.call("thread/list", cwd=cwd, limit=50)
            threads = result.get("threads") or result.get("data") or []
            # 过滤 cwd 匹配（服务器可能已过滤）
            expected = str(Path(cwd).resolve())
            matched = [
                t
                for t in threads
                if t.get("cwd") or t.get("path")
                if str(Path(t.get("cwd") or t.get("path")).resolve()) == expected
            ]
            if not matched:
                return None
            # 取最近更新
            key = lambda t: t.get("updatedAt") or t.get("createdAt") or 0
            latest = max(matched, key=key)
            return latest.get("id")
        except Exception as exc:
            LOGGER.warning("thread/list 失败: %s", exc)
            return None

    async def _cmd_thread(self, user_id: str, arg: str) -> None:
        cwd = self._current_cwd()
        try:
            result = await self.appserver.call("thread/list", cwd=cwd, limit=50)
            threads = result.get("threads") or result.get("data") or []
            expected = str(Path(cwd).resolve())
            threads = [
                thread
                for thread in threads
                if thread.get("cwd") or thread.get("path")
                if str(Path(thread.get("cwd") or thread.get("path")).resolve()) == expected
            ]
        except Exception as exc:
            await self._send_text(user_id, f"无法列出 thread: {exc}")
            return

        if not arg:
            if not threads:
                await self._send_text(user_id, "当前 project 下没有 thread。")
                return
            lines = ["当前 project 线程："]
            for i, t in enumerate(threads, 1):
                title = t.get("preview") or t.get("id")[:16]
                ts = t.get("updatedAt") or t.get("createdAt") or 0
                lines.append(f"{i}. {title} ({ts})")
            await self._send_text(user_id, "\n".join(lines))
            return

        try:
            idx = int(arg) - 1
            if idx < 0 or idx >= len(threads):
                raise ValueError
        except ValueError:
            await self._send_text(user_id, "序号无效。发送 /thread 查看列表。")
            return

        thread = threads[idx]
        thread_id = thread.get("id")
        try:
            await self.appserver.call("thread/resume", threadId=thread_id)
            self._current_thread_id = thread_id
            self._thread_loaded = True
            self.state.thread_id = thread_id
            self._save_state()
            self._notify_status_change()
            await self._send_text(user_id, f"已切换到 thread: {thread_id}")
        except Exception as exc:
            await self._send_text(user_id, f"切换 thread 失败: {exc}")

    async def _cmd_model(self, user_id: str, name: str) -> None:
        if not name:
            try:
                result = await self.appserver.call("model/list")
                models = result.get("models") or result.get("data") or []
                lines = ["可用模型："]
                for m in models:
                    lines.append(f"- {m.get('id') or m.get('name')}")
                lines.append(f"当前: {self.state.model or '默认'}")
                await self._send_text(user_id, "\n".join(lines))
            except Exception as exc:
                await self._send_text(user_id, f"无法列出模型: {exc}")
            return

        self.state.model = name
        self._save_state()
        self._notify_status_change()
        await self._send_text(user_id, f"已切换模型到: {name}")

    async def _cmd_effort(self, user_id: str, arg: str) -> None:
        try:
            result = await self.appserver.call("model/list")
            models = result.get("models") or result.get("data") or []
        except Exception as exc:
            await self._send_text(user_id, f"无法列出模型: {exc}")
            return

        current_model_id = self.state.model
        model = None
        for m in models:
            mid = m.get("id") or m.get("name")
            if mid == current_model_id:
                model = m
                break
        if model is None:
            # 未指定模型时取默认或第一个
            for m in models:
                if m.get("isDefault"):
                    model = m
                    break
            if model is None and models:
                model = models[0]

        if model is None:
            await self._send_text(user_id, "当前没有可用模型信息。")
            return

        efforts = model.get("supportedReasoningEfforts") or []
        effort_values = [e.get("reasoningEffort") if isinstance(e, dict) else e for e in efforts]

        if not arg:
            if not effort_values:
                await self._send_text(user_id, f"当前模型 {model.get('id') or model.get('name')} 没有可选 effort 档位。")
                return
            lines = [f"当前模型: {model.get('id') or model.get('name')}", "可用 effort 档位："]
            for ev in effort_values:
                marker = " ✓" if self.state.effort == ev else ""
                lines.append(f"- {ev}{marker}")
            lines.append(f"当前: {self.state.effort or '默认'}")
            await self._send_text(user_id, "\n".join(lines))
            return

        if arg not in effort_values:
            await self._send_text(
                user_id,
                f"无效档位: {arg}。可用档位: {', '.join(effort_values) or '无'}",
            )
            return

        self.state.effort = arg
        self._save_state()
        self._notify_status_change()
        await self._send_text(user_id, f"已切换 effort 到: {arg}")

    async def _cmd_mode(self, user_id: str, arg: str) -> None:
        if not arg:
            await self._send_text(user_id, f"当前模式: {self.state.mode}")
            return

        if arg == "safe":
            self.state.mode = "safe"
            self.state.pending_full_confirm_until = None
            self._save_state()
            self._notify_status_change()
            await self._send_text(user_id, "已切换到 safe 模式（workspaceWrite，需要审批）。")
            return

        if arg == "full":
            state_mod.set_pending_full_confirm(self.state)
            self._save_state()
            self._notify_status_change()
            await self._send_text(user_id, "请发送「确认」以切换到 full 模式（dangerFullAccess，不审批，30秒内有效）。")
            return

        await self._send_text(user_id, "用法: /mode safe 或 /mode full")

    async def _cmd_queue(self, user_id: str, args: str) -> None:
        parts = args.split(None, 1)
        sub = parts[0].lower() if parts else "list"
        rest = parts[1] if len(parts) > 1 else ""

        if sub in ("list", ""):
            async with self._input_lock:
                queue_snapshot = list(self._queue)
            if not queue_snapshot:
                await self._send_text(user_id, "当前队列为空。")
                return
            lines = ["当前排队消息："]
            for i, m in enumerate(queue_snapshot, 1):
                preview = m.text[:30] + "..." if len(m.text) > 30 else m.text
                lines.append(f"{i}. {preview}")
            await self._send_text(user_id, "\n".join(lines))
            return

        if sub == "jump":
            if not rest:
                await self._send_text(user_id, "用法: /queue jump <消息>")
                return
            async with self._input_lock:
                busy = self._busy
                if busy:
                    self._queue.appendleft(QueuedMessage(user_id, rest))
                    self._save_state()
                    self._notify_status_change()
            if busy:
                await self._send_text(user_id, "已插队到队首（第 1 位），当前 turn 结束后优先处理。")
            else:
                async with self._input_lock:
                    self._busy = True
                    self._notify_status_change()
                try:
                    await self._start_turn(user_id, rest)
                except Exception:
                    self._busy = False
                    self._notify_status_change()
                    raise
            return

        if sub == "pop":
            async with self._input_lock:
                try:
                    idx = int(rest) - 1
                    if idx < 0 or idx >= len(self._queue):
                        raise ValueError
                except ValueError:
                    await self._send_text(user_id, "序号无效。发送 /queue 查看列表。")
                    return
                removed = self._queue[idx]
                del self._queue[idx]
                self._save_state()
                self._notify_status_change()
            await self._send_text(user_id, f"已删除队列第 {idx + 1} 条：{removed.text[:30]}...")
            return

        if sub == "clear":
            async with self._input_lock:
                self._queue.clear()
                self._save_state()
                self._notify_status_change()
            await self._send_text(user_id, "已清空队列。")
            return

        await self._send_text(user_id, "用法: /queue list|jump|pop|clear")

    async def _cmd_interrupt(self, user_id: str, args: str) -> None:
        async with self._input_lock:
            has_turn = bool(self._current_turn_id and self._current_thread_id)
        if not has_turn:
            if args:
                async with self._input_lock:
                    self._busy = True
                    self._notify_status_change()
                try:
                    await self._start_turn(user_id, args)
                except Exception:
                    self._busy = False
                    self._notify_status_change()
                    raise
            else:
                await self._send_text(user_id, "当前没有执行中的 turn。")
            return

        try:
            await self.appserver.call(
                "turn/interrupt",
                threadId=self._current_thread_id,
                turnId=self._current_turn_id,
            )
            async with self._input_lock:
                if args:
                    self._pending_interrupt_msg = QueuedMessage(user_id, args)
            if args:
                await self._send_text(user_id, "已请求中断，当前 turn 结束后立即处理指定消息。")
            else:
                await self._send_text(user_id, "已请求中断当前 turn。")
        except Exception as exc:
            LOGGER.exception("turn/interrupt 失败")
            await self._send_text(user_id, f"中断失败: {exc}")

    # ------------------------------------------------------------------ #
    # Codex 事件
    # ------------------------------------------------------------------ #
    def _on_notification(self, method: str, params: dict[str, Any]) -> None:
        LOGGER.debug("事件: %s", method)
        if method == "turn/started":
            turn = params.get("turn", {})
            self._current_turn_id = turn.get("id")
            self._busy = True
            LOGGER.info("turn 开始: %s", self._current_turn_id)
            if self.webui is not None:
                self.webui.on_turn("started", self._current_turn_id)
                self.webui.on_status_change()
            return

        if method == "turn/completed":
            LOGGER.info("turn 完成: %s", params.get("turn", {}).get("id"))
            asyncio.create_task(self._on_turn_completed(params))
            return

        if method == "item/agentMessage/delta":
            if self.webui is not None:
                self.webui.on_delta(str(params.get("delta") or ""))
            return

        if method == "item/reasoning/textDelta":
            if self.webui is not None:
                self.webui.on_reasoning_delta(str(params.get("delta") or ""))
            return

        if method == "item/started":
            if self.webui is not None:
                item = params.get("item", {})
                if item.get("type") == "commandExecution":
                    cmd = str(item.get("command") or "").splitlines()[0][:120]
                    self.webui.on_message("tool", "Codex", f"⚙️ 执行命令: {cmd}", msg_type="tool_start")
            return

        if method == "item/completed":
            asyncio.create_task(self._on_item_completed(params.get("item", {})))
            return

    async def _on_item_completed(self, item: dict[str, Any]) -> None:
        item_type = item.get("type")
        LOGGER.info("item/completed type=%s id=%s", item_type, item.get("id"))
        user_id = self._current_user_id
        if not user_id:
            return

        if item_type == "agentMessage":
            text = item.get("text") or ""
            if text:
                await self._send_text(user_id, text, msg_type="agent")
            return

        if item_type == "commandExecution":
            await self._send_command_brief(user_id, item)
            return

        if item_type == "fileChange":
            await self._send_file_change_brief(user_id, item)
            return

    async def _send_command_brief(self, user_id: str | RouteContext, item: dict[str, Any]) -> None:
        command = item.get("command", "")
        cmd_text = str(command).splitlines()[0] if command else "未知命令"
        if len(cmd_text) > 80:
            cmd_text = cmd_text[:80] + "..."
        cwd = item.get("cwd", "")
        status = item.get("status", "unknown")
        exit_code = item.get("exitCode")
        success = status == "completed" and (exit_code is None or exit_code == 0)
        marker = "成功" if success else "失败"
        lines = [f"⚙️ 命令: {cmd_text}", f"目录: {cwd}", f"结果: {marker}"]
        if not success:
            output = item.get("aggregatedOutput") or ""
            tail = output[-MAX_FAIL_OUTPUT_LEN:] if output else ""
            if tail:
                lines.append(f"输出尾部:\n{tail}")
        # 命令执行过程只保留在 WebUI；QQ 只接收需要用户处理的审批和最终回复。
        if self.webui is not None:
            context = self._normalize_context(user_id)
            source = "WebUI" if context.is_webui else "QQ"
            self.webui.on_message("tool", source, "\n".join(lines), msg_type="tool")

    async def _send_file_change_brief(self, user_id: str, item: dict[str, Any]) -> None:
        changes = item.get("changes", [])
        status = item.get("status", "unknown")
        if status == "completed":
            marker = "已应用"
        elif status == "declined":
            marker = "已拒绝"
        else:
            marker = f"失败 ({status})"
        paths = [c.get("path", "?") for c in changes]
        text = f"✏️ 文件变更 {marker}\n" + "\n".join(paths[:10])
        if len(paths) > 10:
            text += f"\n... 等 {len(paths)} 个文件"
        await self._send_text(user_id, text, msg_type="tool")

    async def _on_turn_completed(self, params: dict[str, Any]) -> None:
        async with self._input_lock:
            self._busy = True  # 即将处理下一条，保持 busy
            self._notify_status_change()
        turn = params.get("turn", {})
        user_id = self._current_user_id

        if self.webui is not None:
            self.webui.on_turn("completed", turn.get("id"))

        # 失败兜底
        if turn.get("status") == "failed":
            error = turn.get("error", {})
            msg = error.get("message") or "turn 执行失败"
            if user_id:
                await self._send_text(user_id, f"❌ {msg}")

        # 如果没有 agentMessage 但最终 message 存在，兜底发送
        final_text = turn.get("finalAgentMessage", {}).get("text") if isinstance(turn.get("finalAgentMessage"), dict) else None
        if final_text and user_id:
            await self._send_text(user_id, final_text)

        self._current_turn_id = None
        self._current_user_id = None

        # 优先处理 /interrupt 指定的消息
        if self._pending_interrupt_msg is not None:
            msg = self._pending_interrupt_msg
            self._pending_interrupt_msg = None
            try:
                await self._start_turn(msg.context, msg.text)
            except Exception:
                self._busy = False
                self._notify_status_change()
                raise
            return

        await self._drain_queue()

    async def _drain_queue(self) -> None:
        """依次恢复排队消息所属会话；排队的 / 命令仍按命令处理。"""

        while True:
            async with self._input_lock:
                if self._queue:
                    next_msg = self._queue.popleft()
                    self._save_state()
                else:
                    next_msg = None
                self._busy = False
                self._notify_status_change()
            if next_msg is None:
                return
            await self._send_text(next_msg.context, "继续处理下一条消息...")
            await self._process_input(next_msg.context, next_msg.text)
            if self._busy:
                return

    # ------------------------------------------------------------------ #
    # Server -> Client 请求（审批等）
    # ------------------------------------------------------------------ #
    async def _on_server_request(self, method: str, request_id: int, params: dict[str, Any]) -> dict[str, Any]:
        LOGGER.info("server->client 请求: %s id=%s", method, request_id)
        if method in ("item/commandExecution/requestApproval", "item/fileChange/requestApproval", "item/permissions/requestApproval"):
            if method == "item/permissions/requestApproval":
                kind = "permissions"
            else:
                kind = "command" if "commandExecution" in method else "file"
            approval = ApprovalRequest(request_id=request_id, kind=kind, params=params)
            if not self._active_context.is_admin_private(self.config):
                LOGGER.warning(
                    "非管理员私聊会话请求越过沙箱，自动拒绝: scope=%s kind=%s",
                    self._active_context.scope_key,
                    kind,
                )
                return self._decline_result(approval)
            approval.timeout_task = asyncio.create_task(self._approval_timeout(approval))
            self._pending_approvals.append(approval)
            if len(self._pending_approvals) == 1:
                await self._prompt_approval(approval)
            try:
                result = await approval.decision_future
            except asyncio.CancelledError:
                return self._decline_result(approval)
            return result

        if method == "mcpServer/elicitation/request":
            return {"action": "decline", "content": None}

        if method == "currentTime/read":
            return {"currentTimeAt": int(datetime.now(timezone.utc).timestamp())}

        LOGGER.warning("未处理的 server->client 请求 %s id=%s，默认拒绝", method, request_id)
        return {"decision": "decline"}

    async def _approval_timeout(self, approval: ApprovalRequest) -> None:
        try:
            await asyncio.sleep(self.config.approval_timeout_sec)
        except asyncio.CancelledError:
            return
        if approval in self._pending_approvals:
            await self._resolve_approval(approval, self._decline_result(approval), reason="超时自动拒绝")

    def _format_permissions(self, permissions: dict[str, Any]) -> str:
        lines: list[str] = []
        perm = permissions or {}
        if not perm:
            lines.append("权限内容: 未说明")
            return "\n".join(lines)

        for key, value in perm.items():
            key_lower = key.lower()
            if key_lower == "network" and isinstance(value, dict):
                enabled = value.get("enabled")
                lines.append(f"网络访问: {'是' if enabled else '否'}")
            elif key_lower == "filesystem" and isinstance(value, dict):
                writes = value.get("write") or []
                if writes:
                    paths = []
                    for item in writes:
                        if isinstance(item, dict):
                            paths.append(str(item.get("path") or item))
                        else:
                            paths.append(str(item))
                    lines.append("文件写入权限:")
                    for p in paths[:10]:
                        lines.append(f"  - {p}")
                    if len(paths) > 10:
                        lines.append(f"  ... 等 {len(paths)} 个路径")
                reads = value.get("read") or []
                if reads:
                    paths = []
                    for item in reads:
                        if isinstance(item, dict):
                            paths.append(str(item.get("path") or item))
                        else:
                            paths.append(str(item))
                    lines.append("文件读取权限:")
                    for p in paths[:10]:
                        lines.append(f"  - {p}")
                    if len(paths) > 10:
                        lines.append(f"  ... 等 {len(paths)} 个路径")
            else:
                snippet = json.dumps(value, ensure_ascii=False)[:120]
                lines.append(f"{key}: {snippet}")
        return "\n".join(lines)

    async def _prompt_approval(self, approval: ApprovalRequest) -> None:
        user_id = self._current_user_id
        if not user_id:
            return
        params = approval.params
        reason = params.get("reason") or "未说明"
        cwd = params.get("cwd") or self._current_cwd()
        if approval.kind == "command":
            command = params.get("command", "")
            cmd_text = str(command).splitlines()[0] if command else "未知命令"
            if len(cmd_text) > 80:
                cmd_text = cmd_text[:80] + "..."
            text = (
                f"⚠️ Codex 请求批准\n"
                f"$ {cmd_text}\n"
                f"目录: {cwd}\n"
                f"原因: {reason}\n"
                f"回复 /yes 批准，/no 拒绝（{self.config.approval_timeout_sec}秒无响应自动拒绝）"
            )
        elif approval.kind == "file":
            changes = params.get("changes", [])
            text = (
                f"⚠️ Codex 请求批准文件变更\n"
                f"文件数: {len(changes)}\n"
                f"目录: {cwd}\n"
                f"原因: {reason}\n"
                f"回复 /yes 批准，/no 拒绝（{self.config.approval_timeout_sec}秒无响应自动拒绝）"
            )
        else:
            if approval.kind == "permissions":
                LOGGER.info("permissions request params: %s", params)
            permissions_text = self._format_permissions(params.get("permissions", {}))
            text = (
                f"⚠️ Codex 请求额外权限\n"
                f"{permissions_text}\n"
                f"原因: {reason}\n"
                f"回复 /yes 批准，/no 拒绝（{self.config.approval_timeout_sec}秒无响应自动拒绝）"
            )
        approval.prompt_text = text
        await self._send_text(user_id, text)
        if self.webui is not None:
            self.webui.on_approval(
                "pending",
                kind=approval.kind,
                text=text,
                request_id=approval.request_id,
                timeout_sec=self.config.approval_timeout_sec,
            )
            self.webui.on_status_change()

    async def _handle_approval_reply(self, user_id: str, accepted: bool) -> None:
        if not self._pending_approvals:
            await self._send_text(user_id, "当前没有待审批操作。")
            return
        await self._resolve_head_approval(accepted)

    async def _resolve_approval(self, approval: ApprovalRequest, result: dict[str, Any], reason: str) -> None:
        try:
            self._pending_approvals.remove(approval)
        except ValueError:
            return
        if approval.timeout_task is not None:
            approval.timeout_task.cancel()
        if not approval.decision_future.done():
            approval.decision_future.set_result(result)
        user_id = self._current_user_id
        if user_id:
            await self._send_text(user_id, f"审批已{reason}。")
        if self.webui is not None:
            self.webui.on_approval("resolved", kind=approval.kind, text=reason)
            self.webui.on_status_change()
        # 处理下一个
        if self._pending_approvals:
            await self._prompt_approval(self._pending_approvals[0])

    # ------------------------------------------------------------------ #
    # QQ 发送工具
    # ------------------------------------------------------------------ #
    @staticmethod
    def _normalize_context(user_id: str | RouteContext) -> RouteContext:
        if isinstance(user_id, RouteContext):
            return user_id
        if user_id == WEBUI_USER_ID:
            return WEBUI_CONTEXT
        return RouteContext(f"private:{user_id}", str(user_id), "private")

    async def _send_text(
        self, user_id: str | RouteContext, text: str, msg_type: str | None = None
    ) -> None:
        """向 QQ 发送消息，并同步到 WebUI。内部用户（如 __webui__）只走 WebUI。

        msg_type 仅供 WebUI 渲染区分：agent（最终回复）/ tool（命令、文件变更简报）/
        system（命令回执等）。None 按 system 处理。
        """
        context = self._normalize_context(user_id)
        if self.webui is not None:
            source = "WebUI" if context.is_webui else (
                f"QQ群 {context.group_id}" if context.chat_type == "group" else f"QQ {context.sender_id}"
            )
            self.webui.on_message(
                "system" if context.is_webui else "out",
                source,
                text,
                msg_type=msg_type or "system",
            )

        if context.is_webui:
            return

        text = qq_plain_text(text)
        segments = []
        for i in range(0, len(text), MAX_QQ_MSG_LEN):
            segments.append(text[i : i + MAX_QQ_MSG_LEN])
        if not segments:
            return
        for index, seg in enumerate(segments):
            future = (
                self.onebot.send_group_msg(context.group_id, seg)
                if context.chat_type == "group" and context.group_id
                else self.onebot.send_private_msg(context.sender_id, seg)
            )
            if future is not None:
                try:
                    await asyncio.wait_for(future, timeout=10.0)
                except Exception as exc:
                    LOGGER.warning("发送消息 ack 异常: %s", exc)
            else:
                await asyncio.sleep(0.5)
            if index < len(segments) - 1:
                await asyncio.sleep(0.5)
