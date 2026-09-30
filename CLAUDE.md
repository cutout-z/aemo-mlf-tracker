# CLAUDE.md — starting point for a Claude Code session in this folder

This repo is worked by more than one agent. **`AGENTS.md` is the contract; `BRIEF.md` is the job.**
Read them in that order: `CLAUDE.md` → `AGENTS.md` → `BRIEF.md`.

## First three commands

```bash
cd ~/Design/"AEMO MLF Tracker"
git status && git rev-parse --short HEAD     # expect: clean tree, branch design/2026-10
python3 -c "import playwright" || echo "no playwright — the gates need it"
```

If git fails here, stop and report it rather than working around it (macOS TCC blocks git under
`~/Documents`; this copy exists so that never happens).

## What is already here, and who put it there

| Path | Status | Owner |
|---|---|---|
| `index.html` | the page — **yours to change** | the pass |
| `assets/css/tailwind.src.css`, `tailwind.config.js`, `design/design-tokens.md`, `design/tokens.html`, `scripts/build-css.sh`, `assets/css/app.css` | the frozen family design language, installed and built by Hermes **before** your session. Adopt it; do not fork it. | Hermes |
| `scripts/verify-design.py` | **red today (18 of 25 checks fail)** — your target | Hermes |
| `scripts/verify-interactions.py` | **green today (28 checks)** — do not break it | Hermes |
| `outputs/**`, `data/**`, `src/**`, `deploy/**`, `tests/**`, `.github/**` | off limits | the data lane |
| `design/screens/before-*.png` | the before-state evidence | Hermes |

## Rules that cause the most rework

1. **Branch `design/2026-10` only.** Never `main`; a push to `main` is a deploy of a public site.
2. **The ids in `AGENTS.md` → "DOM contract" are wiring, not styling.** Keep them; the committed gates
   read them.
3. **Verify in a browser with real data.** `scripts/verify-design.py` is the gate — run it, don't
   eyeball. An HTTP 200 proves nothing.
4. **Rebuild the CSS after any class change** (`./scripts/build-css.sh`) and commit `assets/css/app.css`,
   or the styling silently does nothing.
5. **Stop after BRIEF step 1 and report** — the token layer plus the shell is where the old inline
   rules and the new preflight interact, and it is the step most likely to shift the page by accident.

## Handback

`AGENTS.md` → "Finishing (the handback)" is the checklist; `docs/design-pass-2026-09-30.md` is the
record the next agent reads. If you run out of time, commit what works, leave the branch pushed, and
say plainly what is half-done.
