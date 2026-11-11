"""Standalone deployment configuration."""
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]

class CoreDeploymentTests(unittest.TestCase):
    def test_configure(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "deploy").mkdir()
            cli = root / "deploy/hermesctl"
            shutil.copy(ROOT / "deploy/hermesctl", cli)
            env = {"PATH": os.environ["PATH"], "HOME": directory,
                   "ENV_FILE": str(root / "deploy/env.sh")}
            result = subprocess.run(["bash", str(cli), "configure"],
                                    input="example-project\n", env=env,
                                    capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("PROJECT_ID=example-project",
                          Path(env["ENV_FILE"]).read_text())
