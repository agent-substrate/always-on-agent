"""Test core startup without launching Hermes or touching /data."""
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

import yaml


class EntrypointTests(unittest.TestCase):
    def boot(self):
        root = Path(__file__).resolve().parent
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        folder = Path(temp.name)
        data = folder / "data"
        data.mkdir()
        share = folder / "share"
        (share / "hermes-skills").mkdir(parents=True)
        shutil.copy(root / "AGENTS.md", share / "hermes-skills/AGENTS.md")
        shutil.copytree(root / "plugins", share / "hermes-plugins")
        script = (root / "entrypoint.sh").read_text()
        script = script.replace("/usr/local/share", str(share)).replace("/data", str(data))
        script = script.replace("exec hermes gateway run", "exit 0")
        result = subprocess.run(
            ["sh", "-c", script], capture_output=True, text=True,
            env={"PATH": os.environ["PATH"], "HOME": str(data),
                 "HERMES_HOME": str(data / ".hermes")})
        return data, result

    def test_core(self):
        data, result = self.boot()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("persistent agent", (data / "AGENTS.md").read_text())
        self.assertFalse((data / ".hermes/skills").exists())
        config = yaml.safe_load((data / ".hermes/config.yaml").read_text())
        self.assertNotIn("platform_toolsets", config)
        self.assertIn("turn-signal", config["plugins"]["enabled"])
        self.assertTrue(config["memory"]["memory_enabled"])



if __name__ == "__main__":
    unittest.main()
