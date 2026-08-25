"""配置文件首次生成逻辑测试。"""

from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from qq_codex_bridge.config import ensure_config_file, load_config, resolve_codex_path


class EnsureConfigFileTests(unittest.TestCase):
    def test_load_access_and_routing_config(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config_path = root / "config.toml"
            config_path.write_text(
                'whitelist = ["2"]\n'
                '[access]\nadmins = ["1"]\ngroup_whitelist = ["9"]\n'
                "[routing]\nproject_root = '.\\qq-root'\nauto_create_projects = true\n",
                encoding="utf-8",
            )
            with patch("qq_codex_bridge.config.Path.cwd", return_value=root):
                config = load_config(config_path)
            self.assertEqual(config.admins, {"1"})
            self.assertEqual(config.whitelist, {"2"})
            self.assertEqual(config.group_whitelist, {"9"})
            self.assertTrue(config.routing.auto_create_projects)
            self.assertEqual(config.routing.public_model, "gpt-5.6-luna")
            self.assertTrue(config.routing.project_root.endswith("qq-root"))

    def test_create_from_template_and_never_overwrite(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            template = root / "config.example.toml"
            target = root / "config.toml"
            template.write_text('whitelist = ["123456789"]\n', encoding="utf-8")

            self.assertTrue(ensure_config_file(target, template))
            self.assertEqual(target.read_bytes(), template.read_bytes())

            target.write_text("local-user-settings\n", encoding="utf-8")
            self.assertFalse(ensure_config_file(target, template))
            self.assertEqual(target.read_text(encoding="utf-8"), "local-user-settings\n")

    def test_resolve_codex_path_prefers_path(self) -> None:
        with patch("qq_codex_bridge.config.shutil.which", return_value=r"C:\Tools\codex.exe"):
            self.assertEqual(resolve_codex_path("codex"), r"C:\Tools\codex.exe")

    def test_explicit_codex_path_is_preserved(self) -> None:
        configured = r"D:\Apps\Codex\codex.exe"
        with patch("qq_codex_bridge.config.shutil.which") as which:
            self.assertEqual(resolve_codex_path(configured), configured)
            which.assert_not_called()


if __name__ == "__main__":
    unittest.main()
