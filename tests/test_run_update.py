"""deploy/run-update.sh starts every run from the committed outputs.

A failed or manual run can leave outputs/ edited or holding stray files. The pipeline overwrites
only the files it writes (it skips the workbook of a region with no rows), and `git add outputs/`
would publish the rest. No network: a local bare origin and a stub PYTHON stand in for GitHub and
the pipeline.
"""

import os
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "deploy" / "run-update.sh"

COMMITTED = "DUID\nA\n"
NEW = "DUID\nA\nB\n"

# Stands in for the pipeline: records the summary it found, then writes $NEW_SUMMARY and
# run_status.json (as src.main does every run). It does not write VIC_mlf.xlsx, as src.main skips a
# region with no rows. The validator and the post-publish check pass.
STUB = """#!/usr/bin/env bash
case "$*" in
  "-m src.main"*)
    cp outputs/summary.csv "${STUB_LOG}/input"
    printf '%s' "${NEW_SUMMARY}" > outputs/summary.csv
    printf '%s' "${NEW_STATUS}" > outputs/run_status.json
    exit 0 ;;
  *validate_outputs.py) exit 0 ;;
  "-m src.post_publish_check") exit 0 ;;
esac
exit 99
"""

pytestmark = pytest.mark.skipif(shutil.which("git") is None or shutil.which("bash") is None,
                                reason="needs git and bash")


def _git(cwd, *args):
    return subprocess.run(["git", "-c", "user.name=t", "-c", "user.email=t@t", *args], cwd=cwd,
                          check=True, capture_output=True, text=True).stdout


@pytest.fixture
def lane(tmp_path):
    origin, app, log = tmp_path / "origin.git", tmp_path / "app", tmp_path / "log"
    _git(tmp_path, "init", "-q", "--bare", "-b", "main", str(origin))
    _git(tmp_path, "clone", "-q", str(origin), str(app))
    _git(app, "checkout", "-q", "-b", "main")
    shutil.copy(ROOT / ".gitignore", app / ".gitignore")
    (app / "outputs").mkdir()
    (app / "outputs" / "summary.csv").write_text(COMMITTED)
    (app / "outputs" / "run_status.json").write_text('{"run_date": "2026-10-08"}\n')
    (app / "outputs" / "VIC_mlf.xlsx").write_text("committed")
    _git(app, "add", ".")
    _git(app, "commit", "-qm", "seed")
    _git(app, "push", "-q", "origin", "main")
    # A gitignored cache in data/: the reset must leave it alone.
    (app / "data").mkdir()
    (app / "data" / "dudetailsummary.feather").write_text("cache")
    log.mkdir()
    stub = tmp_path / "python"
    stub.write_text(STUB)
    stub.chmod(0o755)

    def run(new_summary, new_status='{"run_date": "2026-11-08"}\n'):
        env = os.environ | {"APP_DIR": str(app), "PYTHON": str(stub), "PUSH_CHANGES": "0",
                            "STUB_LOG": str(log), "NEW_SUMMARY": new_summary, "NEW_STATUS": new_status,
                            "GIT_AUTHOR_NAME": "bot", "GIT_AUTHOR_EMAIL": "bot@example.invalid"}
        return subprocess.run(["bash", str(SCRIPT)], env=env, capture_output=True, text=True)

    return {"app": app, "log": log, "run": run}


def _leave_failed_run_behind(app):
    (app / "outputs" / "summary.csv").write_text("edited by a failed run\n")
    (app / "outputs" / "VIC_mlf.xlsx").write_text("edited by a failed run")
    (app / "outputs" / "stray.xlsx").write_text("left over")
    (app / "outputs" / "old").mkdir()
    (app / "outputs" / "old" / "NSW_mlf.xlsx").write_text("left over")


def test_run_publishes_only_the_pipelines_outputs_not_leftovers(lane):
    app, log = lane["app"], lane["log"]
    _leave_failed_run_behind(app)
    run = lane["run"](NEW)
    assert run.returncode == 0, run.stderr
    # The pipeline starts from the committed outputs ...
    assert (log / "input").read_text() == COMMITTED
    # ... only the files it wrote are published ...
    published = sorted(_git(app, "show", "--name-only", "--format=", "HEAD").split())
    assert published == ["outputs/run_status.json", "outputs/summary.csv"]
    assert _git(app, "show", "HEAD:outputs/summary.csv") == NEW
    # ... the edit to a file it did not write is undone and the strays are gone ...
    assert (app / "outputs" / "VIC_mlf.xlsx").read_text() == "committed"
    assert not (app / "outputs" / "stray.xlsx").exists()
    assert not (app / "outputs" / "old").exists()
    # ... while the gitignored caches in data/ survive.
    assert (app / "data" / "dudetailsummary.feather").read_text() == "cache"


def test_leftovers_alone_are_never_published(lane):
    # The pipeline reproduces the committed outputs exactly: the leftovers are the only difference.
    app = lane["app"]
    _leave_failed_run_behind(app)
    run = lane["run"](COMMITTED, new_status='{"run_date": "2026-10-08"}\n')
    assert run.returncode == 0, run.stderr
    assert "No publishable output changes." in run.stdout
    assert _git(app, "rev-list", "--count", "HEAD").strip() == "1"


def test_unchanged_outputs_still_publish_nothing(lane):
    run = lane["run"](COMMITTED, new_status='{"run_date": "2026-10-08"}\n')
    assert run.returncode == 0, run.stderr
    assert "No publishable output changes." in run.stdout
    assert _git(lane["app"], "rev-list", "--count", "HEAD").strip() == "1"
