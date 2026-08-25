"""WebUI 新建 project 的持久化 overlay：data/projects.json。

config.toml 中的 projects 优先；overlay 只补充 config.toml 里没有的条目。
"""

from __future__ import annotations

import json
from pathlib import Path

_OVERLAY_PATH = Path("data/projects.json")


def load_overlay(path: str | Path | None = None) -> dict[str, str]:
    p = Path(path) if path else _OVERLAY_PATH
    if not p.exists():
        return {}
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(raw, dict):
        return {}
    projects: dict[str, str] = {}
    for name, cwd in raw.items():
        if isinstance(name, str) and isinstance(cwd, str):
            try:
                projects[name] = str(Path(cwd).resolve())
            except Exception:
                continue
    return projects


def save_overlay(projects: dict[str, str], path: str | Path | None = None) -> None:
    p = Path(path) if path else _OVERLAY_PATH
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(projects, ensure_ascii=False, indent=2), encoding="utf-8")
