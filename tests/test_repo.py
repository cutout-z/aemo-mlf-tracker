"""Repository wiring: what the update runners stage must be committable."""

import re
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
RUNNERS = sorted(ROOT.glob(".github/workflows/*.yml")) + [ROOT / "deploy" / "run-update.sh"]


def staged_paths() -> list[tuple[str, str]]:
    out = []
    for f in RUNNERS:
        for line in f.read_text().splitlines():
            m = re.search(r"\bgit add\s+(.+)$", line.split(" #")[0].strip())
            if m:
                out += [(f.name, p) for p in m.group(1).split() if not p.startswith("-")]
    return out


@pytest.mark.skipif(shutil.which("git") is None, reason="git not installed")
def test_runners_only_stage_paths_git_will_accept(tmp_path):
    # `git add` of a gitignored path exits 1 ("paths are ignored"), which failed the fallback
    # workflow's commit step: it staged data/*.feather while .gitignore excludes data/.
    # Checked in a scratch repo holding only this .gitignore, so it works outside a checkout too.
    paths = staged_paths()
    assert paths, "no `git add` found in the runners"
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    shutil.copy(ROOT / ".gitignore", tmp_path / ".gitignore")
    ignored = [(f, p) for f, p in paths
               if subprocess.run(["git", "check-ignore", "-q", "--no-index", p.replace("*", "x")],
                                 cwd=tmp_path).returncode == 0]
    assert ignored == []
