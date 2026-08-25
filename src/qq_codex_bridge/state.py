"""持久化状态 data/state.json。"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass
class State:
    project: str | None = None
    thread_id: str | None = None
    model: str | None = None
    effort: str | None = None
    mode: str = "safe"  # "safe" | "full"
    pending_full_confirm_until: str | None = None
    queue: list[dict] = field(default_factory=list)  # [{user_id, text}]


@dataclass
class StateRegistry:
    """按 QQ 私聊/群聊作用域隔离的持久化状态。"""

    sessions: dict[str, State] = field(default_factory=dict)
    queue: list[dict] = field(default_factory=list)


_STATE_PATH = Path("data/state.json")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_dt(iso: str | None) -> datetime | None:
    if not iso:
        return None
    try:
        return datetime.fromisoformat(iso)
    except ValueError:
        return None


def load(path: str | Path | None = None) -> State:
    p = Path(path) if path else _STATE_PATH
    if not p.exists():
        return State()
    try:
        with p.open("r", encoding="utf-8") as f:
            raw: dict[str, Any] = json.load(f)
    except Exception:
        return State()
    return State(
        project=raw.get("project"),
        thread_id=raw.get("thread_id"),
        model=raw.get("model") or None,
        effort=raw.get("effort") or None,
        mode=raw.get("mode", "safe"),
        pending_full_confirm_until=raw.get("pending_full_confirm_until"),
        queue=raw.get("queue", []),
    )


def save(state: State, path: str | Path | None = None) -> None:
    p = Path(path) if path else _STATE_PATH
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as f:
        json.dump(asdict(state), f, ensure_ascii=False, indent=2)


def _state_from_raw(raw: dict[str, Any]) -> State:
    return State(
        project=raw.get("project"),
        thread_id=raw.get("thread_id"),
        model=raw.get("model") or None,
        effort=raw.get("effort") or None,
        mode=raw.get("mode", "safe"),
        pending_full_confirm_until=raw.get("pending_full_confirm_until"),
        queue=raw.get("queue", []),
    )


def load_registry(path: str | Path | None = None) -> StateRegistry:
    """读取 v2 多会话状态；旧版单会话文件自动迁移到 WebUI 会话。"""

    p = Path(path) if path else _STATE_PATH
    if not p.exists():
        return StateRegistry()
    try:
        with p.open("r", encoding="utf-8") as f:
            raw: dict[str, Any] = json.load(f)
    except Exception:
        return StateRegistry()

    sessions_raw = raw.get("sessions")
    if isinstance(sessions_raw, dict):
        sessions = {
            str(key): _state_from_raw(value)
            for key, value in sessions_raw.items()
            if isinstance(value, dict)
        }
        return StateRegistry(sessions=sessions, queue=raw.get("queue", []))

    # v1 只有一个全局 State。保留为本地 WebUI 会话，避免丢失历史选择。
    legacy = _state_from_raw(raw)
    queue = legacy.queue
    legacy.queue = []
    return StateRegistry(sessions={"webui": legacy}, queue=queue)


def save_registry(registry: StateRegistry, path: str | Path | None = None) -> None:
    p = Path(path) if path else _STATE_PATH
    p.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": 2,
        "sessions": {key: asdict(value) for key, value in registry.sessions.items()},
        "queue": registry.queue,
    }
    with p.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def set_pending_full_confirm(state: State) -> None:
    state.pending_full_confirm_until = _now_iso()


def is_pending_full_confirm(state: State, timeout_sec: float = 30.0) -> bool:
    dt = _parse_dt(state.pending_full_confirm_until)
    if not dt:
        return False
    return (datetime.now(timezone.utc) - dt).total_seconds() < timeout_sec
