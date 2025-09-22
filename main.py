import os, io, json, time, math, random, datetime, csv, logging
from typing import List
import requests, yaml, pathlib

# ========= ĐƯỜNG DẪN =========
ROOT        = pathlib.Path(__file__).resolve().parent
CONFIG_PATH = ROOT / "config" / "config.yml"
CSV_PATH    = ROOT / "data" / "latest.csv"

# ========= CẤU HÌNH =========
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

PACE_MS            = 800
RATE_LIMIT_RETRIES = 4
RATE_LIMIT_ERR     = "RATE_LIMIT"
DEBUG              = os.environ.get("DEBUG","0") == "1"

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("fb-export")
_LAST_TS = 0

# ========= CSV LỖI =========
def emit_error_csv(msg: str):
    CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        f.write("ERROR\n")
        f.write((msg or "").strip() + "\n")

# ========= TIỆN ÍCH =========
def _env_int(name: str, default: int) -> int:
    v = os.environ.get(name)
    if v is None: return default
    s = str(v).strip()
    if s == "":   return default
    try:          return int(float(s))
    except:       return default

def _apply_env_overrides():
    global PACE_MS, RATE_LIMIT_RETRIES
    PACE_MS            = _env_int("PACE_MS", PACE_MS)
    RATE_LIMIT_RETRIES = _env_int("RATE_LIMIT_RETRIES", RATE_LIMIT_RETRIES)

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

def money0(v) -> int:
    try:
        return int(round(float(v)))
    except:
        return 0

def pct2(a, b):
    n, d = to_num(a), to_num(b)
    return None if d <= 0 else (n/d)*100.0

def fmt_pct(val) -> str:
    if val is None or val == "": return ""
    try:
        return f"{round(float(val), 2)}%"
    except:
        return ""

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

    err_json = None
    try: err_json = r.json()
    except: pass

    if DEBUG:
        short = url.split("?")[0]
        print(f"[FB_ERR] HTTP {code} @ {short}")
        if err_json: print("[FB_ERR_BODY]", err_json)
        else: print("[FB_ERR_TEXT]", r.text[:500])

    err = (err_json or {}).get("error", {})

    if code == 403 and (err.get("code")==4 or err.get("is_transient") is True):
        if try_count < RATE_LIMIT_RETRIES:
            time.sleep(backoff); return fb_get(url, token, try_count+1)
        raise RuntimeError(RATE_LIMIT_ERR)

    if (code == 400 and str(err.get("code"))=="17") or code == 429 or code >= 500:
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
            vnd = money0((float(row.get("spend",0)) if row.get("spend") else 0.0) * rate)
            out[key] = vnd
        url = j.get("paging",{}).get("next")
        guard += 1
        if guard > 10000: raise RuntimeError("Paging overflow (adset spend).")
    return out

def fetch_insights_ad(act_id: str, since: str, until: str, token: str) -> List[dict]:
    def build_url(with_conversion=True):
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
            "time_increment":"1",
        }
        if with_conversion:
            params["action_report_time"] = "conversion"
        q = "&".join([f"{k}={requests.utils.quote(str(v))}" for k,v in params.items()])
        return f"{base}?{q}"

    url = build_url(with_conversion=True)
    out = []; guard = 0; tried_no_conv = False
    while url:
        try:
            j = fb_get(url, token)
        except RuntimeError as e:
            msg = str(e)
            if (not tried_no_conv) and ("Invalid parameter" in msg or "\"code\":100" in msg):
                if DEBUG: print("[RETRY] insights without action_report_time=conversion")
                url = build_url(with_conversion=False)
                tried_no_conv = True
                continue
            raise
        out.extend(j.get("data",[]) or [])
        url = j.get("paging",{}).get("next"); guard += 1
        if guard > 10000: raise RuntimeError("Paging overflow.")
    return out

# ========= TRANSFORMS =========
def build_budget_maps_vnd(camps, sets, rate, divisor):
    camp_map = {}
    for c in camps or []:
        daily = money0((float(c["daily_budget"])/divisor)*rate) if c.get("daily_budget") else ""
        life  = money0((float(c["lifetime_budget"])/divisor)*rate) if c.get("lifetime_budget") else ""
        camp_map[c["id"]] = {"daily": daily, "lifetime": life}
    adset_map = {}
    for s in sets or []:
        daily = money0((float(s["daily_budget"])/divisor)*rate) if s.get("daily_budget") else ""
        life  = money0((float(s["lifetime_budget"])/divisor)*rate) if s.get("lifetime_budget") else ""
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
    for k in keys:
        nd = k.lower()
        for it in arr:
            if str(it.get("action_type","")).lower() == nd:
                try: return int(float(it.get("value",0)))
                except: return 0
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

        spend_vnd = money0(to_num(r.get("spend")) * rate)
        clicks    = to_num(r.get("clicks"))
        impr      = to_num(r.get("impressions"))
        link      = to_num(r.get("inline_link_clicks"))

        cpc_api  = r.get("cpc")
        cpm_api  = r.get("cpm")

        cpc_click_vnd = money0(to_num(cpc_api)*rate) if (cpc_api not in (None,"")) else (money0(spend_vnd/link) if link>0 else "")
        cpc_all_vnd   = money0(spend_vnd/clicks) if clicks>0 else ""
        cpm_vnd       = money0(to_num(cpm_api)*rate) if (cpm_api not in (None,"")) else (money0((spend_vnd/impr)*1000.0) if impr>0 else "")

        ctr_api       = r.get("ctr")
        ctr_all_pct   = to_num(ctr_api) if (ctr_api not in (None,"")) else (pct2(clicks, impr) or "")
        ctr_click_pct = pct2(link, impr) or ""

        msg = extract_msg_started(r)
        cost_per_msg = pick_cost_per_action(r, "messaging_conversation_started")
        cost_per_msg_vnd = money0(to_num(cost_per_msg)*rate) if cost_per_msg!="" else (money0(spend_vnd/msg) if msg>0 else "")

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
            fmt_pct(ctr_click_pct),
            fmt_pct(ctr_all_pct),
            cpm_vnd or "",
            impr or "",
            r.get("reach","") or ""
        ])
    return out

# ========= ĐỌC GOOGLE SHEET =========
def to_ymd_any(val: str) -> str:
    s = (val or "").strip()
    if not s: return ""
    s = s.split(" ")[0]
    if "/" in s:
        p = s.split("/")
        if len(p)==3:
            d, m, y = p[0], p[1], p[2]
            return f"{int(y):04d}-{int(m):02d}-{int(d):02d}"
    return s  # yyyy-MM-dd

def _csv_rows_from_gsheet_csv(sheet_id: str, sheet_name: str=None, gid: str=None, a1_range: str=None):
    """
    Ưu tiên endpoint export?format=csv&gid=... (ổn định với ô chứa dấu phẩy như D4).
    Nếu lỗi hoặc không có gid thì fallback sang gviz/tq.
    """
    urls = []
    if gid:
        u = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"
        if a1_range: u += f"&range={a1_range}"
        urls.append(u)
    # fallback gviz
    base = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv"
    if sheet_name: base += f"&sheet={requests.utils.quote(sheet_name)}"
    if a1_range:   base += f"&range={a1_range}"
    urls.append(base)

    last_err = None
    for u in urls:
        try:
            r = requests.get(u, timeout=30); r.raise_for_status()
            rows = list(csv.reader(io.StringIO(r.text)))
            if rows:
                return rows
        except Exception as e:
            last_err = e
            continue
    if last_err:
        raise last_err
    return []

def _clamp_dates(since: str, until: str) -> (str, str):
    today = datetime.date.today()
    s = datetime.date.fromisoformat(since)
    u = datetime.date.fromisoformat(until)
    if u > today: u = today
    if s > u: s = u
    return s.isoformat(), u.isoformat()

def load_from_sheet_or_fail() -> dict:
    sheet_id   = os.environ.get("SHEET_ID")
    if not sheet_id:
        emit_error_csv("Thiếu biến SHEET_ID (repo → Settings → Secrets and variables → Actions).")
        raise SystemExit(1)

    sheet_name = os.environ.get("API_SHEET_NAME","api")
    sheet_gid  = os.environ.get("API_SHEET_GID")

    d_vals = _csv_rows_from_gsheet_csv(sheet_id, sheet_name=sheet_name, gid=sheet_gid, a1_range="D2:D4")
    vals = [(row[0].strip() if row and len(row)>=1 else "") for row in d_vals]
    since_raw  = vals[0] if len(vals)>0 else ""
    until_raw  = vals[1] if len(vals)>1 else ""
    accounts_s = vals[2] if len(vals)>2 else ""

    since = to_ymd_any(since_raw)
    until = to_ymd_any(until_raw)

    if not since or not until or not accounts_s:
        if DEBUG:
            print("[SHEET DUMP D2:D4]", d_vals)
        emit_error_csv("Thiếu cấu hình trong sheet 'api': D2 (since), D3 (until), D4 (ad accounts).")
        raise SystemExit(1)

    try: datetime.date.fromisoformat(since)
    except: emit_error_csv("Sai định dạng 'since' (yyyy-MM-dd hoặc dd/MM/yyyy)"); raise SystemExit(1)
    try: datetime.date.fromisoformat(until)
    except: emit_error_csv("Sai định dạng 'until' (yyyy-MM-dd hoặc dd/MM/yyyy)"); raise SystemExit(1)

    since, until = _clamp_dates(since, until)

    accounts = [a.strip() for a in accounts_s.split(",") if a.strip()]
    if not accounts:
        emit_error_csv("D4 rỗng: cần danh sách account, phân tách dấu phẩy (KHÔNG cần 'act_').")
        raise SystemExit(1)
    accounts = [a if a.startswith("act_") else f"act_{a}" for a in accounts]

    fx_rows = _csv_rows_from_gsheet_csv(sheet_id, sheet_name=sheet_name, gid=sheet_gid, a1_range="G2:H")
    fx = {}
    for r in fx_rows:
        if len(r)>=2 and r[0] and r[1]:
            try:
                cur = str(r[0]).strip().upper()
                rate= float(str(r[1]).strip())
                if rate>0: fx[cur]=rate
            except: pass
    if "VND" not in fx: fx["VND"] = 1.0

    if DEBUG:
        print("[CONFIG] since:", since, "until:", until, "accounts:", accounts)

    _apply_env_overrides()
    return {"since": since, "until": until, "accounts": accounts, "fx": fx}

def load_from_config_file_or_fail() -> dict:
    if not CONFIG_PATH.exists():
        emit_error_csv("Thiếu SHEET_ID hoặc config/config.yml — cần một trong hai.")
        raise SystemExit(1)
    try:
        cfg = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8"))
    except Exception as e:
        emit_error_csv(f"Lỗi đọc config.yml: {e}")
        raise SystemExit(1)

    missing = []
    if not cfg.get("since"):    missing.append("since")
    if not cfg.get("until"):    missing.append("until")
    if not cfg.get("accounts"): missing.append("accounts")
    if missing:
        emit_error_csv("Thiếu cấu hình trong config.yml: " + ", ".join(missing))
        raise SystemExit(1)

    since = to_ymd_any(cfg["since"]); until = to_ymd_any(cfg["until"])
    try: datetime.date.fromisoformat(since)
    except: emit_error_csv("Sai định dạng 'since' trong config.yml"); raise SystemExit(1)
    try: datetime.date.fromisoformat(until)
    except: emit_error_csv("Sai định dạng 'until' trong config.yml"); raise SystemExit(1)

    since, until = _clamp_dates(since, until)

    accounts = [str(a).strip() for a in cfg["accounts"] if str(a).strip()]
    if not accounts:
        emit_error_csv("Danh sách accounts rỗng trong config.yml"); raise SystemExit(1)
    accounts = [a if a.startswith("act_") else f"act_{a}" for a in accounts]

    fx_raw = cfg.get("fx") or {}
    try:
        fx = {str(k).upper(): float(v) for k,v in fx_raw.items()}
    except Exception:
        emit_error_csv("Sai cấu hình fx trong config.yml (phải là số)"); raise SystemExit(1)
    if "VND" not in fx: fx["VND"] = 1.0

    if DEBUG:
        print("[CONFIG] since:", since, "until:", until, "accounts:", accounts)

    _apply_env_overrides()
    return {"since": since, "until": until, "accounts": accounts, "fx": fx}

def load_config_or_fail() -> dict:
    if os.environ.get("SHEET_ID"):
        return load_from_sheet_or_fail()
    return load_from_config_file_or_fail()

# ========= MAIN =========
def write_full_csv(rows: List[List]):
    CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(HEADERS_VN)
        if rows:
            w.writerows(rows)

def run_once():
    cfg = load_config_or_fail()

    token = os.environ.get("META_TOKEN")
    if not token:
        emit_error_csv("Thiếu META_TOKEN trong workflow (sync.yml).")
        raise SystemExit(1)

    all_rows: List[List] = []
    for act in cfg["accounts"]:
        meta = fetch_account_meta(act, token)
        cur  = (meta.get("currency") or "VND").upper()
        rate = 1.0 if cur=="VND" else float(cfg["fx"].get(cur, 0))
        if cur!="VND" and (not rate or rate <= 0):
            emit_error_csv(f"Thiếu tỷ giá VND cho {cur} (cột G:H).")
            raise SystemExit(1)
        divisor = minor_unit_divisor(cur)

        b = fetch_budgets_only(act, token)
        camp_map, adset_map = build_budget_maps_vnd(b["campaigns"], b["adsets"], rate, divisor)

        adset_spend = fetch_adset_spend_map_vnd(act, cfg["since"], cfg["until"], rate, token)
        ads = fetch_insights_ad(act, cfg["since"], cfg["until"], token)

        rows = map_rows(ads, adset_map, camp_map, adset_spend, meta["name"], rate)
        all_rows.extend(rows)

    write_full_csv(all_rows)
    print(json.dumps({"status":"done","rows":len(all_rows)}, ensure_ascii=False))

if __name__ == "__main__":
    try:
        run_once()
    except SystemExit:
        raise
    except Exception as e:
        emit_error_csv(f"Lỗi không xác định: {e}")
        raise
