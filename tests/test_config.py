"""配置文件首次生成逻辑测试。"""

from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from qq_codex_bridge.config import ensure_config_file


class EnsureConfigFileTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
