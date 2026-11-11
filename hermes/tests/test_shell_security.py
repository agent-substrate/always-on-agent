"""Check CLI rejection without invoking a provider or cluster."""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]

class ShellSecurityTests(unittest.TestCase):
    def test_unknown_command_fails(self):
        with tempfile.TemporaryDirectory() as folder:
            result = subprocess.run(
                ["bash", str(ROOT / "deploy/hermesctl"), "not-a-command"],
                env={"PATH": os.environ["PATH"], "HOME": folder,
                     "ENV_FILE": str(Path(folder) / "missing")},
                capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("unknown command", result.stderr + result.stdout)
