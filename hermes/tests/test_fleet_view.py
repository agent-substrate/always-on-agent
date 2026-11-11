"""The fleet CLI renders runtime-only rows."""
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]

class FleetViewTests(unittest.TestCase):
    def test_runtime_row(self):
        row = dict(agent_id="research", state="running", turns=1, failures=0,
                   tokens=12, last_turn=None, consecutive_failures=0)
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "fleet.json"
            path.write_text(json.dumps(dict(control_plane_reachable=True,
                agents=[row], totals=dict(agents=1, turns=1, failures=0, tokens=12,
                                         failing_now=0))))
            result = subprocess.run([sys.executable, str(ROOT / "deploy/fleet_view.py"),
                                     str(path)], capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("research", result.stdout)
