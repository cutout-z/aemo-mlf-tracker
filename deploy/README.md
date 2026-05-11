# VPS Annual Updates

Production model:

- Hetzner VPS runs the annual final/draft MLF refreshes.
- GitHub stores code and publishable `outputs/`.
- GitHub Pages deploys after the VPS pushes updated outputs.
- GitHub Actions remains available for manual verification, but should not be the primary scheduled data runner.

The source data footprint is tiny, so the VPS lane intentionally runs `--full-refresh` rather than preserving long-lived source caches.

## Lane

| Lane | Timer | Pipeline args | Purpose |
| --- | --- | --- | --- |
| Annual MLF refresh | `aemo-mlf-tracker.timer` | `--full-refresh` | Refresh final MLFs in April and draft/indicative MLFs in October. |

Recommended layout:

```text
/opt/aemo-mlf-tracker      git checkout + virtualenv
/etc/aemo-mlf-tracker/env  service settings
```

Create `/etc/aemo-mlf-tracker/env` from `env.example`. The service user needs a repo-scoped deploy key that can push to `cutout-z/aemo-mlf-tracker`.

## Install Timer

```bash
sudo cp deploy/aemo-mlf-tracker.service /etc/systemd/system/
sudo cp deploy/aemo-mlf-tracker.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now aemo-mlf-tracker.timer
```

Run once manually:

```bash
sudo systemctl start aemo-mlf-tracker.service
journalctl -u aemo-mlf-tracker.service -f
```
