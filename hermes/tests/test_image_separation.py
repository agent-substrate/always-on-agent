"""Check the standalone runtime contract without cloud access."""
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]

class RuntimeTests(unittest.TestCase):
    def test_gateway_imports_in_isolation(self):
        with tempfile.TemporaryDirectory() as folder:
            shutil.copytree(ROOT / "gateway", Path(folder) / "gateway",
                            ignore=shutil.ignore_patterns("kubectl-ate", "__pycache__"))
            env = {"PATH": os.environ["PATH"], "HOME": folder,
                   "PYTHONPATH": str(Path(folder) / "gateway")}
            result = subprocess.run([sys.executable, "-c", """
import ingress
app = ingress.build_app()
paths = {r.resource.canonical for r in app.router.routes()}
assert paths == {'/message', '/v1/chat/completions', '/health', '/model/v1/chat/completions'}, paths
"""], cwd=folder, env=env, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)

    def test_actor_build_context(self):
        source = (ROOT / "actor/Dockerfile").read_text()
        self.assertIn("hermes-agent[all]", source)
        self.assertIn("COPY plugins/", source)
        self.assertIn("persistent agent", (ROOT / "actor/AGENTS.md").read_text())
