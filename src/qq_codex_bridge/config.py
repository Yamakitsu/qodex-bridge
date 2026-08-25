"""读取 config.toml。"""

from __future__ import annotations

import os
import shutil
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class WebUIConfig:
    enabled: bool = True
    host: str = "127.0.0.1"
    port: int = 8765


@dataclass
class Config:
    ws_url: str = "ws://127.0.0.1:3001"
    access_token: str | None = None
    whitelist: set[str] = field(default_factory=set)
    projects: dict[str, str] = field(default_factory=dict)
    default_project: str | None = None
    default_model: str | None = None
    approval_timeout_sec: int = 60
    codex_path: str = "codex"
    extra_writable_roots: list[str] = field(default_factory=list)
    webui: WebUIConfig = field(default_factory=WebUIConfig)


def resolve_codex_path(configured_path: str) -> str:
    """解析 Codex CLI；Windows 下自动发现 Codex Desktop 自带版本。"""
    expanded = str(Path(configured_path).expanduser())
    if configured_path.lower() != "codex":
        return expanded

    on_path = shutil.which("codex")
    if on_path:
        return on_path

    if os.name == "nt":
        local_app_data = os.environ.get("LOCALAPPDATA")
        if local_app_data:
            bin_root = Path(local_app_data) / "OpenAI" / "Codex" / "bin"
            candidates = list(bin_root.glob("*/codex.exe"))
            if candidates:
                newest = max(candidates, key=lambda path: path.stat().st_mtime)
                return str(newest)

    return expanded


def ensure_config_file(path: str | Path, template_path: str | Path | None = None) -> bool:
    """缺少配置时从公开模板创建一份，已存在时绝不覆盖。

    返回 True 表示本次创建了配置文件，False 表示文件原本就存在。
    """
    target = Path(path)
    if target.exists():
        return False

    candidates = [Path(template_path)] if template_path is not None else [
        target.with_name("config.example.toml"),
        Path.cwd() / "config.example.toml",
    ]
    template = next((candidate for candidate in candidates if candidate.exists()), None)
    if template is None:
        searched = ", ".join(str(candidate.resolve()) for candidate in candidates)
        raise FileNotFoundError(f"配置模板不存在，已检查: {searched}")

    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(template, target)
    return True


def load_config(path: str | Path) -> Config:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"配置文件不存在: {p.resolve()}")

    with p.open("rb") as f:
        raw: dict[str, Any] = tomllib.load(f)

    napcat = raw.get("napcat", {})
    access_token = napcat.get("access_token", "")
    if access_token == "":
        access_token = None

    projects_raw = raw.get("projects", {})
    # 把 project 路径规范化（Windows 路径兼容）
    projects: dict[str, str] = {}
    for name, cwd in projects_raw.items():
        if not isinstance(cwd, str):
            raise ValueError(f"projects.{name} 必须是路径字符串，当前为 {cwd!r}")
        projects[name] = str(Path(cwd).resolve())

    whitelist_raw = raw.get("whitelist") or napcat.get("whitelist") or []
    whitelist = {str(x) for x in whitelist_raw}

    bridge = raw.get("bridge", {})
    default_model = bridge.get("default_model", raw.get("default_model", ""))
    if default_model == "":
        default_model = None

    webui_raw = raw.get("webui", {})
    webui = WebUIConfig(
        enabled=webui_raw.get("enabled", True),
        host=webui_raw.get("host", "127.0.0.1"),
        port=webui_raw.get("port", 8765),
    )

    extra_writable_roots = bridge.get("extra_writable_roots", raw.get("extra_writable_roots", []))
    if extra_writable_roots is None:
        extra_writable_roots = []
    extra_writable_roots = [str(Path(p).resolve()) for p in extra_writable_roots]

    return Config(
        ws_url=napcat.get("ws_url", "ws://127.0.0.1:3001"),
        access_token=access_token,
        whitelist=whitelist,
        projects=projects,
        default_project=bridge.get("default_project", raw.get("default_project")),
        default_model=default_model,
        approval_timeout_sec=bridge.get("approval_timeout_sec", raw.get("approval_timeout_sec", 60)),
        codex_path=resolve_codex_path(
            bridge.get("codex_path", raw.get("codex_path", Config.codex_path))
        ),
        extra_writable_roots=extra_writable_roots,
        webui=webui,
    )
