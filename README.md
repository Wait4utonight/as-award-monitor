# AS Award Monitor (GitHub Actions)

Alaska/Hawaiian Airlines partner-business award-space monitor. Runs on a GitHub
hosted runner (US egress) every 30 minutes, checks three date windows between
15 Asia cities and 4 US-West cities at a 95,000-point cap, and pushes new
deals to an Enterprise WeChat webhook.

## How it works

- `monitor.py` — the monitor. Each run:
  1. Pre-checks 6 probe routes; aborts with no side effects if the HA API is
     blocked.
  2. Queries partner-business award calendars (rate-limited, 2 workers).
  3. Tracks deals in `as_award_state_*.json` (new / disappeared / reappeared
     after 24h = new).
  4. Pushes only genuinely new deals to WeChat.
- `.github/workflows/monitor.yml` — runs the script on schedule, then commits
  the updated state files and logs back to this repository (this is also what
  keeps the scheduled workflow from being auto-disabled after 60 days of
  inactivity).

## Secrets

- `WECHAT_WEBHOOK` — Enterprise WeChat group-bot webhook URL. Set under
  Settings → Secrets and variables → Actions.

## Tasks

| Task | Route | Dates |
|------|-------|-------|
| go_sep | Asia 15 → US West 4 | 2027-09-17 ~ 2027-09-19 |
| go_nyd | Asia 15 → US West 4 | 2026-12-31 ~ 2027-01-04 |
| rt_dec | US West 4 → Asia 15 | 2026-12-09 ~ 2026-12-12 |
