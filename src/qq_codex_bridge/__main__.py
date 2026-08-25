"""入口：python -m qq_codex_bridge [--config PATH]"""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys
from pathlib import Path

from .config import load_config
from .orchestrator import Orchestrator
from .projects import load_overlay
from .webui import WebUI, WebUIConfig


def _setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        stream=sys.stdout,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="QQ <-> Codex app-server bridge")
    parser.add_argument("--config", default="config.toml", help="Configuration file path (default: config.toml)")
    args = parser.parse_args()

    _setup_logging()
    config = load_config(args.config)
    # 合并 WebUI 里新建的 project（config.toml 中的同名条目优先）
    for name, cwd in load_overlay().items():
        config.projects.setdefault(name, cwd)

    orchestrator = Orchestrator(config)
    webui: WebUI | None = None
    if config.webui.enabled:
        webui = WebUI(orchestrator, config.webui)
        orchestrator.register_webui(webui)

    async def _run() -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, orchestrator._request_shutdown)
            except NotImplementedError:
                pass
        try:
            tasks = [asyncio.create_task(orchestrator.run(), name="orchestrator")]
            if webui is not None:
                tasks.append(asyncio.create_task(webui.run(), name="webui"))
            await asyncio.gather(*tasks)
        finally:
            if webui is not None:
                await webui.shutdown()
            await orchestrator.shutdown()

    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
