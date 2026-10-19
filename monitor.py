#!/usr/bin/env python3
"""
Alaska/Hawaiian Award Monitor - GitHub Actions edition
Runs on GitHub-hosted runners (US egress). State files live in the repo and
are committed back after each run by the workflow, preserving deal history.

Safety design (state can never be polluted by a blocked/degraded run):
  1) PRE-CHECK: 6 probe queries before any search. All/dead-blocked -> exit 1,
     no queries, no state write.
  2) MID-RUN GUARD: >30% no-data routes or >=3 blocked responses -> abort,
     state NOT saved.
  3) Rate limit: 2 workers * 1.2s interval (~1.7 req/s).

Env:
  WECHAT_WEBHOOK   enterprise WeChat webhook (from repo secret). Empty = no push.
  AS_PROXY_PREFIX  optional forwarder prefix (https://worker/?url=)
  AS_MIN_INTERVAL  default 1.2
  AS_WORKERS       default 2
  AS_PUSH_TEST     1 = send a link-test message on clean run (setup only)
"""

import json, time, sys, os, urllib.request, urllib.parse, urllib.error, gzip
from datetime import datetime, timedelta, timezone

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(SCRIPT_DIR, "multi_monitor_log.txt")
DEDUP_FILE = os.path.join(SCRIPT_DIR, "as_alert_dedup.json")

WECHAT_WEBHOOK = os.environ.get('WECHAT_WEBHOOK', '').strip()
AS_PROXY_PREFIX = os.environ.get('AS_PROXY_PREFIX', '').strip()
MIN_INTERVAL = float(os.environ.get('AS_MIN_INTERVAL', '1.2'))
WORKERS = max(1, int(os.environ.get('AS_WORKERS', '2')))
PUSH_TEST = os.environ.get('AS_PUSH_TEST', '') in ('1', 'true', 'True')

TZ8 = timezone(timedelta(hours=8))

def now_cn():
    return datetime.now(TZ8)

def parse_ts(s):
    """Local-v4 state files stored naive local(+8) timestamps; new ones are aware."""
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=TZ8)
    return dt

# ==================== CITY LISTS ====================
ASIA_ORIGINS = [
    'HND', 'NRT', 'KIX', 'CTS', 'NGO',
    'SIN', 'BKK', 'KUL',
    'ICN', 'HKG', 'TPE',
    'PVG', 'PEK', 'MNL', 'SGN',
]
US_WEST = ['LAX', 'SFO', 'SAN', 'SEA']
MAX_POINTS = 95000

# ==================== 3 SEARCH CONFIGS ====================
CONFIGS = [
    {
        'name': 'go_sep',
        'description': '亚洲→美西 2027-09-17~09-19',
        'origins': ASIA_ORIGINS,
        'destinations': US_WEST,
        'target_dates': ['2027-09-17', '2027-09-18', '2027-09-19'],
        'calendar_dates': ['2027-09-17'],
    },
    {
        'name': 'go_nyd',
        'description': '亚洲→美西 2026-12-31~2027-01-04',
        'origins': ASIA_ORIGINS,
        'destinations': US_WEST,
        'target_dates': ['2026-12-31', '2027-01-01', '2027-01-02', '2027-01-03', '2027-01-04'],
        'calendar_dates': ['2026-12-31', '2027-01-02'],
    },
    {
        'name': 'rt_dec',
        'description': '美西→亚洲 2026-12-09~12-12',
        'origins': US_WEST,
        'destinations': ASIA_ORIGINS,
        'target_dates': ['2026-12-09', '2026-12-10', '2026-12-11', '2026-12-12'],
        'calendar_dates': ['2026-12-10'],
    },
]

# ==================== API CORE ====================
HA_BASE = 'https://www.hawaiianairlines.com'
HTTP_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36',
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referer': 'https://www.hawaiianairlines.com/search/calendar',
}


def http_get(url, timeout=20):
    """Returns (status, text). 406 = geo-block. 0 = connection error."""
    if AS_PROXY_PREFIX:
        fetch_url = AS_PROXY_PREFIX + urllib.parse.quote(url, safe='')
    else:
        fetch_url = url
    req = urllib.request.Request(fetch_url, headers=HTTP_HEADERS)
    for attempt in (1, 2):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                data = resp.read()
                if resp.headers.get('Content-Encoding') == 'gzip':
                    data = gzip.decompress(data)
                return resp.status, data.decode('utf-8', errors='replace')
        except urllib.error.HTTPError as e:
            try: e.read()
            except Exception: pass
            return e.code, ''
        except Exception:
            if attempt == 2:
                return 0, ''
            time.sleep(3)
    return 0, ''


def parse_sveltekit(content):
    chunks = {}
    for line in content.strip().split('\n'):
        line = line.strip()
        if not line: continue
        try: obj = json.loads(line)
        except: continue
        if obj.get('type') == 'data':
            for node in obj.get('nodes', []):
                if node.get('type') == 'data' and 'data' in node:
                    chunks['main'] = node['data']
        elif obj.get('type') == 'chunk':
            cid = obj.get('id')
            if cid is not None and 'data' in obj:
                chunks[cid] = obj['data']
    return chunks


def resolve_ref(data_array, index, depth=0, max_depth=5):
    if depth >= max_depth: return index
    if not isinstance(index, int) or isinstance(index, bool): return index
    if index < 0 or index >= len(data_array): return index
    val = data_array[index]
    if isinstance(val, (dict, list)): return resolve_value(data_array, val, depth+1, max_depth)
    if isinstance(val, (str, float, bool)): return val
    if isinstance(val, int):
        resolved = resolve_ref(data_array, val, depth+1, max_depth)
        if isinstance(resolved, (str, float, dict, list, bool)): return resolved
        return val
    return val


def resolve_value(data_array, value, depth=0, max_depth=5):
    if depth >= max_depth: return value
    if isinstance(value, dict):
        return {k: resolve_value(data_array, v, depth+1, max_depth) for k, v in value.items()}
    if isinstance(value, list):
        return [resolve_value(data_array, v, depth+1, max_depth) for v in value]
    if isinstance(value, int) and not isinstance(value, bool):
        return resolve_ref(data_array, value, depth, max_depth)
    return value


def extract_calendar_dates(chunks):
    dates = []
    for chunk_id, data_array in chunks.items():
        if chunk_id == 'main': continue
        for item in data_array:
            if not isinstance(item, dict): continue
            if 'date' not in item or 'awardPoints' not in item: continue
            resolved = resolve_value(data_array, item, max_depth=4)
            d = resolved.get('date', '')
            ap = resolved.get('awardPoints', 0)
            pr = resolved.get('price', 0)
            if isinstance(d, str) and isinstance(ap, (int, float)):
                price_val = float(pr) if isinstance(pr, (int, float)) else 0.0
                dates.append((d, int(ap), price_val))
    return dates


def build_calendar_url(origin, dest, calendar_date):
    params = urllib.parse.urlencode({
        'O': origin, 'D': dest, 'OD': calendar_date,
        'A': '1', 'RT': 'false',
        'RequestType': 'Calendar', 'ShoppingMethod': 'onlineaward',
        'locale': 'en-us', 'FareType': 'Partner Business',
    })
    return f'{HA_BASE}/search/calendar/__data.json?{params}'


def fetch_business_calendar(origin, dest, calendar_date):
    """Returns (dates, status)."""
    status, content = http_get(build_calendar_url(origin, dest, calendar_date))
    if status != 200 or not content:
        return [], status
    try:
        return extract_calendar_dates(parse_sveltekit(content)), status
    except Exception:
        return [], status


# ==================== PRE-CHECK ====================
def precheck():
    probes = [
        ('LAX', 'HND', '2026-12-10'), ('SFO', 'NRT', '2026-12-10'),
        ('SEA', 'ICN', '2026-12-10'), ('SAN', 'KIX', '2026-12-10'),
        ('HND', 'LAX', '2027-09-17'), ('ICN', 'SFO', '2027-09-17'),
    ]
    blocked, ok = 0, 0
    for o, d, od in probes:
        status, _ = http_get(build_calendar_url(o, d, od), timeout=15)
        if status == 406:
            blocked += 1
        elif status == 200:
            ok += 1
        time.sleep(1.0)
    if blocked > 0 or ok == 0:
        return False, f"blocked={blocked} ok={ok}"
    return True, f"ok={ok}/{len(probes)}"


def get_exit_ip():
    for svc in ('https://ipinfo.io/json', 'https://api.ipify.org?format=json'):
        try:
            req = urllib.request.Request(svc, headers={'User-Agent': 'curl/8.0'})
            with urllib.request.urlopen(req, timeout=8) as r:
                data = json.loads(r.read().decode('utf-8'))
            ip = data.get('ip') or data.get('query')
            if ip:
                return f"{ip} ({data.get('country','')} {data.get('org','')})".strip()
        except Exception:
            continue
    return 'unknown'


# ==================== WECHAT & DEDUP ====================
def send_wechat(message):
    if not WECHAT_WEBHOOK:
        return False
    payload = json.dumps({'msgtype': 'markdown', 'markdown': {'content': message}}).encode('utf-8')
    req = urllib.request.Request(WECHAT_WEBHOOK, data=payload,
                                 headers={'Content-Type': 'application/json'}, method='POST')
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read().decode('utf-8')).get('errcode') == 0
    except Exception:
        return False


def vpn_alert_dedup_ok():
    try:
        with open(DEDUP_FILE, 'r', encoding='utf-8') as f:
            last = json.load(f).get('vpn_alert_at')
        if last:
            h = (now_cn() - parse_ts(last)).total_seconds() / 3600
            return h >= 6
    except Exception:
        pass
    return True


def vpn_alert_mark():
    try:
        with open(DEDUP_FILE, 'w', encoding='utf-8') as f:
            json.dump({'vpn_alert_at': now_cn().isoformat()}, f)
    except Exception:
        pass


# ==================== STATE ====================
def load_state(config_name):
    state_file = os.path.join(SCRIPT_DIR, f'as_award_state_{config_name}.json')
    try:
        with open(state_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return {}


def save_state(config_name, state):
    state_file = os.path.join(SCRIPT_DIR, f'as_award_state_{config_name}.json')
    try:
        with open(state_file, 'w', encoding='utf-8') as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def log_to_file(msg):
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(msg + "\n")
    except Exception:
        pass


# ==================== RUN ONE CONFIG ====================
def run_config(config, config_idx):
    total_routes = len(config['origins']) * len(config['destinations'])
    print(f"\n{'#'*60}", flush=True)
    print(f"# Task {config_idx+1}/{len(CONFIGS)}: {config['description']}", flush=True)
    print(f"# Routes: {total_routes} | Dates: {config['target_dates'][0]}~{config['target_dates'][-1]}", flush=True)
    print(f"# Max: {MAX_POINTS:,}pts | Workers: {WORKERS} x {MIN_INTERVAL}s", flush=True)
    print(f"{'#'*60}", flush=True)

    state = load_state(config['name'])
    for k, v in list(state.items()):
        if isinstance(v, str):
            state[k] = {'first_seen': v, 'last_seen': v, 'disappeared_at': None}

    all_deals, new_deals = [], []
    routes_checked = nodata_count = blocked_count = 0
    found_deal_ids = set()

    tasks = []
    for cal_date in config['calendar_dates']:
        for origin in config['origins']:
            for dest in config['destinations']:
                tasks.append((origin, dest, cal_date))

    def worker(origin, dest, cal_date):
        time.sleep(MIN_INTERVAL * (hash((origin, dest, cal_date)) % WORKERS) / 2)
        deals = []
        try:
            cal_dates, status = fetch_business_calendar(origin, dest, cal_date)
            if status != 200:
                return deals, status
            target_cal = [(d, p, pr) for d, p, pr in cal_dates if d in config['target_dates']]
            for date_str, points, price in target_cal:
                if 0 < points <= MAX_POINTS:
                    deals.append({'origin': origin, 'destination': dest,
                                  'date': date_str, 'points': points, 'price': price})
            return deals, status
        except Exception:
            return deals, 0

    abort = False
    from concurrent.futures import ThreadPoolExecutor, as_completed
    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = {executor.submit(worker, o, d, c): (o, d, c) for o, d, c in tasks}
        for future in as_completed(futures):
            origin, dest, cal_date = futures[future]
            routes_checked += 1
            route_key = f"{origin}-{dest}"
            try:
                deals, status = future.result()
                if status == 406:
                    blocked_count += 1
                if status != 200:
                    nodata_count += 1
                else:
                    if deals:
                        summary = ", ".join(f"{d['date'][-5:]}:{d['points']//1000}k" for d in deals)
                        print(f"  *** {route_key}({cal_date}): {summary} ***", flush=True)
                        now = now_cn()
                        for deal in deals:
                            deal_id = f"{route_key}_{deal['date']}_{deal['points']}"
                            found_deal_ids.add(deal_id)
                            all_deals.append(deal)
                            is_new = False
                            if deal_id not in state:
                                is_new = True
                                state[deal_id] = {'first_seen': now.isoformat(),
                                                  'last_seen': now.isoformat(),
                                                  'disappeared_at': None}
                            else:
                                disappeared_at = state[deal_id].get('disappeared_at')
                                if disappeared_at:
                                    hours_gone = (now - parse_ts(disappeared_at)).total_seconds() / 3600
                                    if hours_gone >= 24:
                                        is_new = True
                                        print(f"    REAPPEARED (gone {hours_gone:.1f}h)", flush=True)
                                state[deal_id]['last_seen'] = now.isoformat()
                                state[deal_id]['disappeared_at'] = None
                            if is_new:
                                new_deals.append(deal)
                                print(f"    NEW DEAL: {deal['date']} = {deal['points']:,}pts + ${deal['price']:.2f}", flush=True)
                if routes_checked % 30 == 0:
                    print(f"  [{routes_checked}/{len(tasks)}] checked, nodata={nodata_count}", flush=True)
            except Exception:
                nodata_count += 1

            if routes_checked >= 10:
                fail_rate = nodata_count / routes_checked
                if fail_rate > 0.30 or blocked_count >= 3:
                    print(f"  !! MID-RUN ABORT: fail_rate={fail_rate:.0%} blocked={blocked_count}. State NOT saved.", flush=True)
                    log_to_file(f"[{now_cn().isoformat()}] MIDRUN-ABORT {config['name']}: "
                                f"{routes_checked}/{len(tasks)} checked, fail={nodata_count}, blocked={blocked_count}. State untouched.")
                    abort = True
                    executor.shutdown(wait=False, cancel_futures=True)
                    break

    if abort or blocked_count >= 3:
        return None

    now = now_cn()
    for deal_id in list(state.keys()):
        if deal_id not in found_deal_ids:
            if state[deal_id].get('disappeared_at') is None:
                state[deal_id]['disappeared_at'] = now.isoformat()
                print(f"  DISAPPEARED: {deal_id}", flush=True)

    if new_deals:
        print(f"\n*** FOUND {len(new_deals)} NEW DEAL(S)! ***", flush=True)
        msg_lines = [
            f"## AS里程商务舱 - {config['description']}\n",
            f"**时间**: {now.strftime('%Y-%m-%d %H:%M')}\n",
            f"**积分上限**: {MAX_POINTS:,}分\n\n",
        ]
        for deal in new_deals:
            url = (f"https://www.hawaiianairlines.com/search/calendar?O={deal['origin']}"
                   f"&D={deal['destination']}&OD={deal['date']}&A=1&RT=false"
                   f"&RequestType=Calendar&ShoppingMethod=onlineaward&locale=en-us&FareType=Partner+Business")
            msg_lines.extend([
                f"**{deal['origin']} → {deal['destination']}** ({deal['date']})",
                f"- 积分: {deal['points']:,}分 | 税费: ${deal['price']:.2f}",
                f"- [查看]({url})\n",
            ])
        msg_lines.append(f"> 检查 {routes_checked} 条航线")
        if send_wechat('\n'.join(msg_lines)):
            print("  WeChat: sent!", flush=True)
        else:
            print("  WeChat: FAILED (webhook empty or error)", flush=True)

    save_state(config['name'], state)
    print(f"  Result: {len(all_deals)} total, {len(new_deals)} new, {routes_checked} routes, nodata={nodata_count}", flush=True)
    return all_deals, new_deals


if __name__ == '__main__':
    print(f"\n{'='*60}", flush=True)
    print(f"AS Award Monitor (GitHub Actions edition)", flush=True)
    print(f"Time: {now_cn().strftime('%Y-%m-%d %H:%M:%S')} (+08) | UTC run {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}", flush=True)
    print(f"Egress: {'proxy=' + AS_PROXY_PREFIX if AS_PROXY_PREFIX else 'direct'} | ExitIP: {get_exit_ip()}", flush=True)
    print(f"WeChat push: {'ON' if WECHAT_WEBHOOK else 'OFF (no webhook)'}", flush=True)
    print(f"{'='*60}", flush=True)

    ok, detail = precheck()
    if not ok:
        print(f"PRE-CHECK FAILED ({detail}) -> HA API blocked/unreachable. No queries, state untouched. Exit 1.", flush=True)
        log_to_file(f"[{now_cn().isoformat()}] PRECHECK-FAIL {detail}. No run, state untouched.")
        if WECHAT_WEBHOOK and vpn_alert_dedup_ok():
            msg = (f"## ⚠️ AS里程监控中止：HA API封锁\n\n**时间**: {now_cn().strftime('%Y-%m-%d %H:%M')}\n\n"
                   f"**现象**: 预检 {detail}，出口IP {get_exit_ip()}\n\n"
                   f"**处置**: 安全中止，未发查询，状态零污染\n\n"
                   f"> GitHub Actions 出口异常，请检查")
            if send_wechat(msg):
                vpn_alert_mark()
                print("Block alert pushed (dedup 6h).", flush=True)
        sys.exit(1)
    print(f"PRE-CHECK OK ({detail}) -> running.", flush=True)

    exit_code = 0
    all_total = all_new = 0
    for i, cfg in enumerate(CONFIGS):
        log_to_file(f"[{now_cn().isoformat()}] Start: {cfg['name']}")
        start = time.time()
        result = run_config(cfg, i)
        elapsed = time.time() - start
        if result is None:
            log_to_file(f"[{now_cn().isoformat()}] ABORTED (unhealthy): {cfg['name']} in {elapsed:.1f}s. State untouched.")
            exit_code = 3
            break
        deals, new = result
        all_total += len(deals)
        all_new += len(new)
        log_to_file(f"[{now_cn().isoformat()}] Done: {cfg['name']} in {elapsed:.1f}s - {len(deals)} deals, {len(new)} new")

    if PUSH_TEST and exit_code == 0 and WECHAT_WEBHOOK:
        ok = send_wechat(f"## ✅ AS里程监控链路测试\n\n**时间**: {now_cn().strftime('%Y-%m-%d %H:%M')}\n\n"
                         f"GitHub Actions 定时监控已上线，出口IP {get_exit_ip()}，"
                         f"本次检查 {all_total} 条在架票源、{all_new} 条新票。")
        print(f"Push test message: {'sent' if ok else 'FAILED'}", flush=True)

    print(f"\n{'='*60}", flush=True)
    print(f"All done. Total: {all_total} deals, {all_new} new | exit={exit_code}", flush=True)
    print(f"{'='*60}", flush=True)
    sys.exit(exit_code)
