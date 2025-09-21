import os, json, time, math, random, datetime, csv, logging
from typing import List, Dict
import requests, yaml, pathlib
from pathlib import Path

# ========= ĐƯỜNG DẪN =========
ROOT       = pathlib.Path(__file__).resolve().parent
CONFIG_PATH= ROOT / "config" / "config.yml"
STATE_PATH = ROOT / ".state" / "queue.json"
CSV_PATH   = ROOT / "data" / "latest.csv"

# ========= CẤU HÌNH TƯƠNG ỨNG APPS SCRIPT =========
FB_API_VERSION = "v20.0"
HEADERS_VN = [
    "NGÀY BẮT ĐẦU","ID TÀI KHOẢN","TÊN TÀI KHOẢN",
    "TÊN CHIẾN DỊCH","NGÂN SÁCH CHIẾN DỊCH (VND)",
    "TÊN NHÓM QUẢNG CÁO","NGÂN SÁCH NHÓM QUẢNG CÁO (VND)","CHI TIÊU NHÓM QUẢNG CÁO (VND)",
    "TÊN QUẢNG CÁO","LƯỢT BẮT ĐẦU TRÒ CHUYỆN","KẾT QUẢ","CHI PHÍ/MỖI KẾT QUẢ (VND)",
    "CHI TIÊU QUẢNG CÁO (VND)","CPC CLICK (QC) (VND)","CPC TẤT CẢ (QC) (VND)",
    "CTR CLICK (QC) (%)","CTR TẤT CẢ (QC) (%)","CPM (QC) (VND)",
    "LƯỢT HIỂN THỊ (QC)","NGƯỜI TIẾP CẬN (QC)"
]

# sẽ override bằng config.yml
CHUNK_DAYS = 3
TIME_BUDGET_S = 300
PACE_MS = 800
RATE_LIMIT_RETRIES = 4
RATE_LIMIT_ERR = "RATE_LIMIT"

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("fb-export")
_LAST_TS = 0

# ========= CSV LỖI (CÁCH 2) =========
def emit_error_csv(msg: str):
    CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        # Đơn giản để IMPORTDATA hiển thị rõ lỗi
        f.write("ERROR\n")
        f.write((msg or "").strip() + "\n")

# ========= TIỆN ÍCH =========
def pace():
    global _LAST_TS
    now = time.time() * 1000
    wait = PACE_MS - (now - _LAST_TS) if _LAST_TS else 0
    if wait > 0:
        time.sleep(wait/1000.0)
    _LAST_TS = time.time() * 1000

def to_num(v):
    try:
        n = float(v)
        if math.isfinite(n): return n
        return 0.0
    except:
        return 0.0

def pct2(a, b):
    n, d = to_num(a), to_num(b)
    return "" if d <= 0 else round((n/d)*100, 2)

def minor_unit_divisor(cur: str) -> int:
    return 1 if (cur or "").upper() in ("VND","JPY","KRW") else 100

def with_token(url: str, token: str) -> str:
    sep = "&" if "?" in url else "?"
    return f"{url}{sep}access_token={requests.utils.quote(token)}"

def fb_get(url: str, token: str, try_count=0):
    pace()
    MAX_TRIES = 8
    backoff = min(1.2*(1.8**try_count) + random.random()*0.7, 25.0)
    r = requests.get(with_token(url, token), timeout=60)
    code = r.status_code
    if 200 <= code < 300:
        return r.json()
    err = None
    try: err = r.json().get("error")
    except: pass
    if code == 403 and err and (err.get("code")==4 or err.get("is_transient") is True):
        if try_count < RATE_LIMIT_RETRIES:
            time.sleep(backoff); return fb_get(url, token, try_count+1)
        raise RuntimeError(RATE_LIMIT_ERR)
    if (code == 400 and err and str(err.get("code"))=="17") or code == 429 or code >= 500:
        if try_count < MAX_TRIES:
            time.sleep(backoff); return fb_get(url, token, try_count+1)
        raise RuntimeError(f"HTTP {code} after retries: {r.text}")
    raise RuntimeError(f"HTTP {code}: {r.text}")

def fb_paged(url_no_token: str, token: str) -> List[dict]:
    out = []; url = url_no_token; guard = 0
    while url:
        j = fb_get(url, token)
        out.extend(j.get("data",[]) or [])
        url = j.get("paging",{}).get("next")
        guard += 1
        if guard > 10000: raise RuntimeError("Paging overflow.")
    return out

# ========= FETCHERS =========
def fetch_account_meta(act_id: str, token: str) -> dict:
    url = f"https://graph.facebook.com/{FB_API_VERSION}/{requests.utils.quote(act_id)}?fields=name,currency"
    try:
        j = fb_get(url, token)
        return {"name": j.get("name",""), "currency": j.get("currency","VND")}
    except Exception as e:
        if str(e)==RATE_LIMIT_ERR: raise
        return {"name":"","currency":"VND"}

def fetch_budgets_only(act_id: str, token: str):
    base = f"https://graph.facebook.com/{FB_API_VERSION}"
    act = requests.utils.quote(act_id)
    camps = fb_paged(f"{base}/{act}/campaigns?fields=id,daily_budget,lifetime_budget&limit=500", token)
    sets  = fb_paged(f"{base}/{act}/adsets?fields=id,campaign_id,daily_budget,lifetime_budget&limit=500", token)
    return {"campaigns": camps, "adsets": sets}

def fetch_adset_spend_map_vnd(act_id: str, since: str, until: str, rate: float, token: str) -> dict:
    act = requests.utils.quote(act_id)
    base = f"https://graph.facebook.com/{FB_API_VERSION}/{act}/insights"
    params = {
        "level":"adset", "fields":"date_start,adset_id,spend", "limit":"500",
        "time_range": json.dumps({"since":since,"until":until}), "time_increment":"1"
    }
    q = "&".join([f"{k}={requests.utils.quote(str(v))}" for k,v in params.items()])
    url = f"{base}?{q}"
    out = {}; guard = 0
    while url:
        j = fb_get(url, token)
        for row in j.get("data",[]) or []:
            key = f"{row.get('adset_id','')}|{row.get('date_start','')}"
            vnd = (float(row.get("spend",0)) if row.get("spend") else 0.0) * rate
            out[key] = vnd
        url = j.get("paging",{}).get("next")
        guard += 1
        if guard > 10000: raise RuntimeError("Paging overflow (adset spend).")
    return out

def fetch_insights_ad(act_id: str, since: str, until: str, token: str) -> List[dict]:
    act = requests.utils.quote(act_id)
    base = f"https://graph.facebook.com/{FB_API_VERSION}/{act}/insights"
    fields = ",".join([
        "date_start","account_id","campaign_id","campaign_name",
        "adset_id","adset_name","ad_id","ad_name",
        "impressions","reach","spend","clicks","inline_link_clicks",
        "cpm","cpc","ctr","actions","cost_per_action_type"
    ])
    params = {
        "level":"ad","fields":fields,"limit":"500",
        "time_range": json.dumps({"since":since,"until":until}),
        "time_increment":"1","action_report_time":"conversion"
    }
    q = "&".join([f"{k}={requests.utils.quote(str(v))}" for k,v in params.items()])
    url = f"{base}?{q}"
    out = []; guard = 0
    while url:
        j = fb_get(url, token)
        out.extend(j.get("data",[]) or [])
        url = j.get("paging",{}).get("next")
        guard += 1
        if guard > 10000: raise RuntimeError("Paging overflow.")
    return out

# ========= TRANSFORMS =========
def build_budget_maps_vnd(camps, sets, rate, divisor):
    camp_map = {}
    for c in camps or []:
        daily = (float(c["daily_budget"])/divisor)*rate if c.get("daily_budget") else ""
        life  = (float(c["lifetime_budget"])/divisor)*rate if c.get("lifetime_budget") else ""
        camp_map[c["id"]] = {"daily": daily, "lifetime": life}
    adset_map = {}
    for s in sets or []:
        daily = (float(s["daily_budget"])/divisor)*rate if s.get("daily_budget") else ""
        life  = (float(s["lifetime_budget"])/divisor)*rate if s.get("lifetime_budget") else ""
        adset_map[s["id"]] = {"daily": daily, "lifetime": life, "campaign_id": s.get("campaign_id")}
    return camp_map, adset_map

def extract_msg_started(r: dict) -> int:
    arr = r.get("actions")
    if not isinstance(arr, list): return 0
    keys = [
        "messaging_conversation_started","messaging_conversations_started","messaging_first_reply",
        "onsite_conversion.messaging_first_reply",
        "onsite_conversion.messaging_conversation_started_1d",
        "onsite_conversion.messaging_conversation_started_7d",
        "onsite_conversion.messaging_conversation_started_28d"
    ]
    # exact
    for k in keys:
        nd = k.lower()
        for it in arr:
            if str(it.get("action_type","")).lower() == nd:
                try: return int(float(it.get("value",0)))
                except: return 0
    # contains
    for k in keys:
        nd = k.lower()
        for it in arr:
            if nd in str(it.get("action_type","")).lower():
                try: return int(float(it.get("value",0)))
                except: return 0
    return 0

def pick_cost_per_action(r: dict, type_name: str):
    arr = r.get("cost_per_action_type")
    if not isinstance(arr, list): return ""
    nd = type_name.lower()
    for it in arr:
        if str(it.get("action_type","")).lower() == nd:
            return it.get("value","")
    for it in arr:
        if nd in str(it.get("action_type","")).lower():
            return it.get("value","")
    return ""

def map_rows(ad_rows, adset_map, camp_map, adset_spend_map, account_name, rate):
    out = []
    for r in ad_rows or []:
        s = adset_map.get(r.get("adset_id",""), {})
        c = camp_map.get(r.get("campaign_id",""), {})
        key = f"{r.get('adset_id','')}|{r.get('date_start','')}"
        adset_spend_vnd = adset_spend_map.get(key, "")

        spend_vnd = to_num(r.get("spend")) * rate
        clicks    = to_num(r.get("clicks"))
        impr      = to_num(r.get("impressions"))
        link      = to_num(r.get("inline_link_clicks"))

        cpc_api  = r.get("cpc")
        cpm_api  = r.get("cpm")
        ctr_api  = r.get("ctr")

        cpc_click_vnd = (to_num(cpc_api)*rate) if (cpc_api not in (None,"")) else ((spend_vnd/link) if link>0 else "")
        cpc_all_vnd   = (spend_vnd/clicks) if clicks>0 else ""
        ctr_all_pct   = (to_num(ctr_api)) if (ctr_api not in (None,"")) else (pct2(clicks, impr))
        ctr_click_pct = pct2(link, impr)
        cpm_vnd       = (to_num(cpm_api)*rate) if (cpm_api not in (None,"")) else (((spend_vnd/impr)*1000.0) if impr>0 else "")

        msg = extract_msg_started(r)
        cost_per_msg = pick_cost_per_action(r, "messaging_conversation_started")
        cost_per_msg_vnd = (to_num(cost_per_msg)*rate) if cost_per_msg!="" else ((spend_vnd/msg) if msg>0 else "")

        out.append([
            r.get("date_start",""),
            r.get("account_id",""),
            account_name or "",
            r.get("campaign_name",""),
            camp_map.get(s.get("campaign_id",""),{}).get("daily","") if s.get("campaign_id") else c.get("daily",""),
            r.get("adset_name",""),
            s.get("daily",""),
            adset_spend_vnd or "",
            r.get("ad_name",""),
            msg or "",
            msg or "",
            cost_per_msg_vnd or "",
            spend_vnd or "",
            cpc_click_vnd or "",
            cpc_all_vnd or "",
            ctr_click_pct or "",
            ctr_all_pct or "",
            cpm_vnd or "",
            impr or "",
            r.get("reach","") or ""
        ])
    return out

def slice_dates(since: str, until: str, chunk_days: int):
    s = datetime.date.fromisoformat(since)
    u = datetime.date.fromisoformat(until)
    cur = s
    while cur <= u:
        end = min(u, cur + datetime.timedelta(days=chunk_days-1))
        yield cur.isoformat(), end.isoformat()
        cur = end + datetime.timedelta(days=1)

# ========= STATE & CSV =========
def read_state():
    if STATE_PATH.exists():
        try: return json.loads(STATE_PATH.read_text(encoding="utf-8"))
        except: return None
    return None

def write_state(obj):
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(obj, ensure_ascii=False), encoding="utf-8")

def clear_state():
    if STATE_PATH.exists(): STATE_PATH.unlink()

def ensure_csv_header():
    CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not CSV_PATH.exists():
        with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(HEADERS_VN)

def append_csv_rows(rows: List[List]):
    if not rows: return
    ensure_csv_header()
    with open(CSV_PATH, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerows(rows)

# ========= LOAD CONFIG + VALIDATION (CÁCH 2) =========
def load_config_or_fail():
    if not CONFIG_PATH.exists():
        emit_error_csv("Thiếu file config/config.yml trong repo")
        raise SystemExit(1)
    try:
        cfg = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8"))
    except Exception as e:
        emit_error_csv(f"Lỗi đọc config.yml: {e}")
        raise SystemExit(1)

    # bắt buộc: since, until, accounts
    missing = []
    if not cfg.get("since"):    missing.append("since")
    if not cfg.get("until"):    missing.append("until")
    if not cfg.get("accounts"): missing.append("accounts")
    if missing:
        emit_error_csv("Thiếu cấu hình trong config.yml: " + ", ".join(missing))
        raise SystemExit(1)

    # kiểm tra format yyyy-MM-dd
    try: datetime.date.fromisoformat(cfg["since"])
    except: emit_error_csv("Sai định dạng 'since' (yyyy-MM-dd)"); raise SystemExit(1)
    try: datetime.date.fromisoformat(cfg["until"])
    except: emit_error_csv("Sai định dạng 'until' (yyyy-MM-dd)"); raise SystemExit(1)

    accounts = [str(a).strip() for a in cfg["accounts"]]
    if not accounts:
        emit_error_csv("Danh sách accounts rỗng trong config.yml")
        raise SystemExit(1)
    accounts = [a if a.startswith("act_") else f"act_{a}" for a in accounts]

    # FX
    fx_raw = cfg.get("fx") or {}
    try:
        fx = {str(k).upper(): float(v) for k,v in fx_raw.items()}
    except Exception:
        emit_error_csv("Sai cấu hình fx trong config.yml (phải là số)")
        raise SystemExit(1)
    if "VND" not in fx: fx["VND"] = 1.0

    # override tham số nâng cao
    global CHUNK_DAYS, TIME_BUDGET_S, PACE_MS, RATE_LIMIT_RETRIES
    CHUNK_DAYS = int(cfg.get("chunk_days", CHUNK_DAYS))
    TIME_BUDGET_S = int(cfg.get("time_budget_s", TIME_BUDGET_S))
    PACE_MS = int(cfg.get("pace_ms", PACE_MS))
    RATE_LIMIT_RETRIES = int(cfg.get("rate_limit_retries", RATE_LIMIT_RETRIES))

    return {
        "since": cfg["since"],
        "until": cfg["until"],
        "accounts": accounts,
        "fx": fx
    }

# ========= MAIN (resume theo TIME_BUDGET_S) =========
def run_timed():
    # Lấy token từ secret (bắt buộc)
    meta_token = os.environ.get("META_TOKEN")
    if not meta_token:
        emit_error_csv("Thiếu GitHub Secret META_TOKEN")
        raise SystemExit(1)

    # Đọc & validate config
    cfg = load_config_or_fail()

    start = time.time()
    st = read_state()
    if not st or not isinstance(st.get("queue",[]), list) or not st["queue"]:
        queue = []
        for act in cfg["accounts"]:
            for s,u in slice_dates(cfg["since"], cfg["until"], CHUNK_DAYS):
                queue.append({"actId": act, "since": s, "until": u})
        st = {"queue": queue}

    ensure_csv_header()
    flushed = 0

    while st["queue"] and (time.time()-start) < (TIME_BUDGET_S - 10):
        job = st["queue"].pop(0)

        # Meta & tỷ giá
        meta = fetch_account_meta(job["actId"], meta_token)
        cur  = (meta.get("currency") or "VND").upper()
        rate = 1.0 if cur=="VND" else float(cfg["fx"].get(cur, 0))
        if cur!="VND" and (not rate or rate <= 0):
            emit_error_csv(f"Thiếu tỷ giá VND cho {cur} trong config.yml")
            raise SystemExit(1)
        divisor = minor_unit_divisor(cur)

        # Budgets & Spend & Insights
        b = fetch_budgets_only(job["actId"], meta_token)
        camp_map, adset_map = build_budget_maps_vnd(b["campaigns"], b["adsets"], rate, divisor)

        adset_spend = fetch_adset_spend_map_vnd(job["actId"], job["since"], job["until"], rate, meta_token)
        ads = fetch_insights_ad(job["actId"], job["since"], job["until"], meta_token)

        rows = map_rows(ads, adset_map, camp_map, adset_spend, meta["name"], rate)
        append_csv_rows(rows)
        flushed += len(rows)

    if st["queue"]:
        write_state(st)
        print(json.dumps({"status":"paused","remaining_jobs":len(st["queue"]),"flushed_rows":flushed}, ensure_ascii=False))
    else:
        clear_state()
        print(json.dumps({"status":"done","flushed_rows":flushed}, ensure_ascii=False))

if __name__ == "__main__":
    try:
        run_timed()
    except SystemExit:
        # đã emit_error_csv trước đó
        raise
    except Exception as e:
        # lỗi bất ngờ -> cũng ghi CSV lỗi để Sheet thấy
        emit_error_csv(f"Lỗi không xác định: {e}")
        raise
