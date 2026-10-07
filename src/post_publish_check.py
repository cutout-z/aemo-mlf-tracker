"""Post-publish check: a draft workbook blocked in March turns the lane RED.

AEMO publishes the next FY's draft MLFs early in March, so a blocked draft fetch in March
means a draft is very likely out and the page is missing it. The run still publishes
(without the draft column; the footer says why), so deploy/run-update.sh runs this after
its commit/push step, or on its "no changes" exit, and exits with this check's status.
Outside March a blocked draft is only a footer note.

    python -m src.post_publish_check [path/to/run_status.json]
"""

import datetime as _dt
import json
import sys
from pathlib import Path

from . import config

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DRAFT_MONTH = 3  # March


def check(status: dict) -> str | None:
    """The failure message, or None when the run is fine."""
    draft = status.get("draft_workbook") or {}
    try:
        month = _dt.date.fromisoformat(status["run_date"]).month
    except (KeyError, TypeError, ValueError):
        return f"run_status.json has no valid run_date ({status.get('run_date')!r})"
    if draft.get("state") == "blocked" and month == DRAFT_MONTH:
        return (f"draft {draft.get('fy', '?')} MLF workbook blocked in March ({draft.get('detail', 'no detail')}): "
                "AEMO has likely published it, but this run could not fetch it, so the page went out "
                "without the draft column")
    return None


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    path = Path(argv[0]) if argv else PROJECT_ROOT / config.RUN_STATUS_JSON
    try:
        status = json.loads(path.read_text())
    except (OSError, ValueError) as e:
        print(f"POST-PUBLISH CHECK FAILED: cannot read {path}: {e}")
        return 1
    problem = check(status)
    if problem:
        print(f"POST-PUBLISH CHECK FAILED: {problem}")
        return 1
    print("Post-publish check passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
