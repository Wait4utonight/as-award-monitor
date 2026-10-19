#!/usr/bin/env python3
"""
Aggregate AS monitor shard artifacts, merge state files, push WeChat.

Runs in the aggregate job after all matrix shards finish. Reads
shards/shard_result_*.json (uploaded as artifacts), compares against the
repo state files, and:

  - records new deals (brand-new, or reappeared after 24+ hours)
  - marks disappeared deals ONLY for routes covered by successful queries,
    and only when >=70% of all queries succeeded this run (protects against
    a partially-blocked round writing false disappearance marks)
  - pushes a combined WeChat message for new deals; sends a link-test
    message when AS_PUSH_TEST=1 and nothing new; sends a blocked alert
    (6h dedup, via repo file) when nearly all shards were geo-blocked

Exit codes: 0 normal / 3 degraded (disappeared marks skipped) / 1 no shards.
"""

import os, sys, json, glob
from monitor import (CONFIGS, MAX_POINTS, WECHAT_WEBHOOK, PUSH_TEST,
                     load_state, save_state, log_to_file, now_cn, parse_ts,
                     send_wechat, all_tasks, vpn_alert_dedup_ok, vpn_alert_mark)

SHARD_DIR = os.environ.get('AS_SHARD_DIR', 'shards')
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) or '.'

shards = []
for p in sorted(glob.glob(os.path.join(SCRIPT_DIR, SHARD_DIR, 'shard_result_*.json'))):
    try:
        with open(p, 'r', encoding='utf-8') as f:
            shards.append(json.load(f))
    except Exception as e:
        print(f'WARN: unreadable shard file {p}: {e}')

if not shards:
    print('FATAL: no shard results found. Exit 1.')
    log_to_file(f'[{now_cn().isoformat()}] AGG: no shard results found.')
    sys.exit(1)

total_q = len(all_tasks())
ok_shards = [s for s in shards if s.get('status') == 'ok']
success_q = sum(1 for s in shards for r in s.get('results', []) if r.get('ok'))
n_aborted = sum(1 for s in shards if s.get('status') == 'aborted')
n_blocked = sum(1 for s in shards if s.get('status') == 'blocked')
degraded = success_q < 0.7 * total_q
full_blocked = n_blocked >= 0.75 * len(shards)

print(f'Shards: {len(shards)} (ok={len(ok_shards)} aborted={n_aborted} blocked={n_blocked}) | '
      f'queries ok {success_q}/{total_q} ({success_q / total_q:.0%}) | degraded={degraded}')

# ---------- collect coverage & found deals per config ----------
found_all = {}   # config_name -> {deal_id: deal}
cover = {}       # config_name -> {(o, d): set(target dates seen)}
for s in shards:
    for r in s.get('results', []):
        if not r.get('ok'):
            continue
        tag = r['config']
        cover.setdefault(tag, {}).setdefault((r['o'], r['d']), set()).update(r.get('dates_seen', []))
        for deal in r.get('deals', []):
            did = f"{r['o']}-{r['d']}_{deal['date']}_{deal['points']}"
            found_all.setdefault(tag, {})[did] = deal

# ---------- merge into state files ----------
now = now_cn()
new_deals_all = {}   # config_name -> (config, [deals])
for cfg in CONFIGS:
    tag = cfg['name']
    state = load_state(tag)
    for k, v in list(state.items()):
        if isinstance(v, str):   # migrate old format
            state[k] = {'first_seen': v, 'last_seen': v, 'disappeared_at': None}
    found = found_all.get(tag, {})
    cover_cfg = cover.get(tag, {})
    news = []
    for did, deal in found.items():
        if did not in state:
            state[did] = {'first_seen': now.isoformat(), 'last_seen': now.isoformat(),
                          'disappeared_at': None}
            news.append(deal)
            print(f"  NEW [{tag}]: {did}", flush=True)
        else:
            da = state[did].get('disappeared_at')
            if da:
                gone = (now - parse_ts(da)).total_seconds() / 3600
                if gone >= 24:
                    news.append(deal)
                    print(f"  REAPPEARED [{tag}]: {did} (gone {gone:.1f}h)", flush=True)
            state[did]['last_seen'] = now.isoformat()
            state[did]['disappeared_at'] = None
    if not degraded:
        for did, st in state.items():
            if did in found:
                continue
            try:
                parts = did.split('_')
                route, date = parts[0], parts[1]
                o, d = route.split('-')
            except Exception:
                continue
            if date in cover_cfg.get((o, d), set()) and st.get('disappeared_at') is None:
                st['disappeared_at'] = now.isoformat()
                print(f"  DISAPPEARED [{tag}]: {did}", flush=True)
    save_state(tag, state)
    if news:
        new_deals_all[tag] = (cfg, news)

# ---------- WeChat ----------
total_new = sum(len(x[1]) for x in new_deals_all.values())
if WECHAT_WEBHOOK:
    if full_blocked and vpn_alert_dedup_ok():
        msg = (f"## ⚠️ AS里程监控异常：HA API封锁Actions出口\n\n"
               f"**时间**: {now.strftime('%Y-%m-%d %H:%M')}\n\n"
               f"**现象**: {n_blocked}/{len(shards)} 个分片预检406，疑似出口IP段被限\n\n"
               f"**处置**: 本轮状态零污染；请检查Actions运行情况")
        if send_wechat(msg):
            vpn_alert_mark()
            print('Blocked alert pushed (dedup 6h).', flush=True)
    if new_deals_all:
        lines = [f"## AS里程商务舱 - 新票源发现\n",
                 f"**时间**: {now.strftime('%Y-%m-%d %H:%M')}\n",
                 f"**积分上限**: {MAX_POINTS:,}分\n"]
        shown = 0
        for tag, (cfg, deals) in new_deals_all.items():
            lines.append(f"\n**{cfg['description']}** ({len(deals)}条)\n")
            for deal in deals:
                if shown >= 15:
                    lines.append(f"- ……共 {total_new} 条新票源，其余见日志\n")
                    break
                url = (f"https://www.hawaiianairlines.com/search/calendar?O={deal['origin']}"
                       f"&D={deal['destination']}&OD={deal['date']}&A=1&RT=false"
                       f"&RequestType=Calendar&ShoppingMethod=onlineaward&locale=en-us&FareType=Partner+Business")
                lines.append(f"**{deal['origin']} → {deal['destination']}** ({deal['date']})："
                             f"{deal['points']:,}分 + ${deal['price']:.2f} [查看]({url})\n")
                shown += 1
        lines.append(f"\n> 查询成功 {success_q}/{total_q} 条（{success_q / total_q:.0%}），"
                     f"{len(ok_shards)}/{len(shards)} 分片健康")
        if send_wechat('\n'.join(lines)):
            print(f'WeChat: pushed {total_new} new deal(s).', flush=True)
        else:
            print('WeChat: push FAILED', flush=True)
    elif PUSH_TEST:
        ok = send_wechat(f"## ✅ AS里程监控链路测试\n\n**时间**: {now.strftime('%Y-%m-%d %H:%M')}\n\n"
                         f"GitHub Actions 分片监控已上线：{len(ok_shards)}/{len(shards)} 分片健康，"
                         f"查询成功 {success_q}/{total_q} 条，本轮无 ≤{MAX_POINTS // 1000}k 的新票源。")
        print(f'Push test message: {"sent" if ok else "FAILED"}', flush=True)

print(f"\nAggregate done: shards={len(shards)} (ok={len(ok_shards)} aborted={n_aborted} "
      f"blocked={n_blocked}) | queries {success_q}/{total_q} | new deals {total_new} | "
      f"degraded={degraded}", flush=True)
log_to_file(f"[{now.isoformat()}] AGG: shards={len(shards)} ok={len(ok_shards)} "
            f"aborted={n_aborted} blocked={n_blocked} queries={success_q}/{total_q} "
            f"new={total_new} degraded={degraded}")

sys.exit(3 if degraded else 0)
