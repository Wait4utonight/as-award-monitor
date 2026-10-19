# AS Award Monitor (GitHub Actions)

Alaska/Hawaiian Airlines partner-business award-space monitor. Runs on GitHub
Actions every 30 minutes, checking three date windows between 15 Asia cities
and 4 US-West cities at a 95,000-point cap, and pushing new deals to an
Enterprise WeChat webhook.

## Architecture (sharded matrix)

HA/Fastly allows only a small number of consecutive queries per IP before
returning 406, so a single long run always gets throttled. The workflow
therefore splits the 240 queries across 16 parallel jobs:

```
monitor (matrix x16)                    aggregate
  shard i: 15 queries, own IP  ─► artifact ─► merge shards
                                        ├─ new deals / reappeared (24h+) -> WeChat
                                        ├─ disappeared marks (covered routes only)
                                        ├─ state files committed back to repo
                                        └─ degraded (<70% ok) -> skip disappearance marks
```

- `monitor.py` — probe + query engine. Set `AS_SHARD_INDEX` / `AS_SHARD_COUNT`
  for shard mode (sequential, globally paced ~2.5s/req, 45s backoff on 406).
  Without those vars it runs full-mode locally (e.g. over a VPN).
- `aggregator.py` — runs in the aggregate job. Merges `shards/shard_result_*.json`
  against the repo state files, updates them, pushes WeChat, logs.
- State files `as_award_state_*.json` live in this repo and are committed after
  each run — this also keeps the scheduled workflow from being auto-disabled.

## Secrets

- `WECHAT_WEBHOOK` — Enterprise WeChat group-bot webhook URL
  (Settings → Secrets and variables → Actions).

## Tasks

| Task | Route | Dates |
|------|-------|-------|
| go_sep | Asia 15 → US West 4 | 2027-09-17 ~ 2027-09-19 |
| go_nyd | Asia 15 → US West 4 | 2026-12-31 ~ 2027-01-04 |
| rt_dec | US West 4 → Asia 15 | 2026-12-09 ~ 2026-12-12 |
