"""Repository-relative Markdown links resolve."""
from pathlib import Path
import re
import json
import unittest

ROOT = Path(__file__).resolve().parents[1]

class DocumentationTests(unittest.TestCase):
    def test_readme_links(self):
        for path in ROOT.rglob("*.md"):
            if ".git" in path.parts:
                continue
            for target in re.findall(r"\]\(([^)]+)\)", path.read_text()):
                target = target.split("#", 1)[0]
                if not target or re.match(r"[a-z]+:", target) or target.startswith("/"):
                    continue
                with self.subTest(file=str(path.relative_to(ROOT)), target=target):
                    self.assertTrue((path.parent / target).exists(), target)

    def test_example_persona_paths(self):
        folder = ROOT / "demo/fleet"
        fleet = json.loads((folder / "fleet.example.json").read_text())
        for shape in fleet["shapes"].values():
            with self.subTest(persona=shape["persona_file"]):
                self.assertTrue((folder / shape["persona_file"]).is_file())

if __name__ == "__main__":
    unittest.main()
