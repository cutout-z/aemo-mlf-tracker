"""src/post_publish_check.py: a blocked draft in March fails the lane after publishing."""

import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest

from src import post_publish_check as ppc

RUN_UPDATE = Path(__file__).resolve().parent.parent / "deploy" / "run-update.sh"


def _status(run_date, state):
    return {"run_date": run_date, "draft_workbook": {"fy": "2027-28", "state": state, "detail": "HTTP 403"}}


@pytest.mark.parametrize("run_date, state, code", [
    ("2027-03-08", "blocked", 1),        # draft season: the draft is out but could not be fetched
    ("2027-03-08", "not_published", 0),  # AEMO redirected to /404: nothing to fetch yet
    ("2027-04-08", "blocked", 0),        # after the final lands, a blocked draft is only a footer note
    ("2027-03-08", "published", 0),
])
def test_exit_code(tmp_path, capsys, run_date, state, code):
    path = tmp_path / "run_status.json"
    path.write_text(json.dumps(_status(run_date, state)))
    assert ppc.main([str(path)]) == code
    if code:
        assert "draft 2027-28 MLF workbook blocked in March" in capsys.readouterr().out


def test_missing_run_status_fails(tmp_path):
    assert ppc.main([str(tmp_path / "run_status.json")]) == 1


FAKE_PYTHON = """#!/bin/sh
case "$*" in
  "-m src.main"*) [ -n "$NEW_OUTPUT" ] && echo "$NEW_OUTPUT" > outputs/summary.csv; exit 0 ;;
  *validate_outputs.py) exit 0 ;;
  "-m src.post_publish_check") echo checked >> "$CHECK_LOG"; exit 3 ;;
esac
exit 99
"""


@pytest.fixture
def lane(tmp_path):
    """A clone of a bare origin with one outputs/ commit, and a stub interpreter whose check fails."""
    def git(*args, cwd):
        subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True)
    origin, app = tmp_path / "origin.git", tmp_path / "app"
    git("init", "-q", "--bare", "-b", "main", str(origin), cwd=tmp_path)
    git("clone", "-q", str(origin), str(app), cwd=tmp_path)
    (app / "outputs").mkdir()
    (app / "outputs" / "summary.csv").write_text("old\n")
    git("checkout", "-q", "-b", "main", cwd=app)
    git("add", "outputs/", cwd=app)
    git("-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "-m", "seed", cwd=app)
    git("push", "-q", "origin", "main", cwd=app)
    fake = tmp_path / "python"
    fake.write_text(FAKE_PYTHON)
    fake.chmod(0o755)
    env = {**os.environ, "APP_DIR": str(app), "PYTHON": str(fake), "CHECK_LOG": str(tmp_path / "check.log")}

    def run(new_output=""):
        out = subprocess.run(["bash", str(RUN_UPDATE)], env={**env, "NEW_OUTPUT": new_output},
                             capture_output=True, text=True)
        log = tmp_path / "check.log"
        ran = log.exists() and log.read_text().count("checked")
        pushed = subprocess.run(["git", "log", "-1", "--format=%s", "main"], cwd=origin,
                                capture_output=True, text=True).stdout.strip()
        return out.returncode, ran, pushed
    return run


@pytest.mark.skipif(shutil.which("git") is None or shutil.which("bash") is None, reason="git/bash not installed")
def test_runner_pushes_then_fails_on_the_check(lane):
    # deploy/run-update.sh stops before committing on any non-zero exit, so the check comes after the push.
    code, ran, pushed = lane(new_output="new")
    assert (code, ran) == (3, 1)
    assert pushed.startswith("Update MLF data")


@pytest.mark.skipif(shutil.which("git") is None or shutil.which("bash") is None, reason="git/bash not installed")
def test_runner_runs_the_check_when_nothing_changed(lane):
    code, ran, pushed = lane()
    assert (code, ran, pushed) == (3, 1, "seed")
