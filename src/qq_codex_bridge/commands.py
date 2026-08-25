"""命令解析与响应文本。"""

from __future__ import annotations


def parse(text: str) -> tuple[str | None, str]:
    """解析 / 命令。返回 (command, args)；不是命令返回 (None, text)。"""
    text = text.strip()
    if not text.startswith("/"):
        return None, text
    parts = text[1:].split(None, 1)
    command = parts[0].lower()
    args = parts[1] if len(parts) > 1 else ""
    return command, args


def help_text() -> str:
    return (
        "可用命令：\n"
        "/list - 命令清单与当前状态\n"
        "/new - 开启新 thread\n"
        "/stop - 中断当前 turn\n"
        "/interrupt <消息> - 中断当前 turn 并立即处理这条消息；无消息时等同 /stop\n"
        "/status - 当前运行状态\n"
        "/project - 列出项目；/project <名> 切换\n"
        "/thread - 列出当前项目线程；/thread <序号> 切换\n"
        "/model - 列出模型；/model <名> 切换\n"
        "/effort - 列出当前模型的 reasoning effort 档位；/effort <档位> 切换\n"
        "/mode - 当前模式；/mode safe|full 切换（full 需二次确认）\n"
        "/queue - 列出排队消息\n"
        "/queue jump <消息> - 插队到队首\n"
        "/queue pop <序号> - 删除指定排队消息\n"
        "/queue clear - 清空队列\n"
        "/yes /no - 审批应答\n"
        "普通文本将发给 Codex。"
    )


def unknown_command() -> str:
    return "未知命令。发送 /list 查看命令表。"
