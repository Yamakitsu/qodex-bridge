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


def set_pending_full_confirm(state: State) -> None:
    state.pending_full_confirm_until = _now_iso()


def is_pending_full_confirm(state: State, timeout_sec: float = 30.0) -> bool:
    dt = _parse_dt(state.pending_full_confirm_until)
    if not dt:
        return False
    return (datetime.now(timezone.utc) - dt).total_seconds() < timeout_sec
