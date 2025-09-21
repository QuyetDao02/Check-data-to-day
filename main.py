import os, json, time, math, random, datetime, logging
from typing import List, Dict, Optional
import requests

# Google Sheets API
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# ================= ENV & CONST =================
FB_API_VERSION   = os.getenv("FB_API_VERSION", "v20.0")
SHEET_DATA_NAME  = os.getenv("SHEET_DATA_NAME", "Data")
STATE_TAB        = os.getenv("STATE_TAB", "_state")
STATE_KEY        = os.getenv("STATE_KEY", "DATA_JOB_QUEUE_VND")  # giữ giống Apps Script

# Apps Script của bạn: TIME_INCREMENT = 1; CHUNK_DAYS = 3; TIME_BUDGET_MS=5 phút
TIME_INCREMENT   = int(os.getenv("TIME_INCREMENT", "1"))
CHUNK_DAYS       = int(os.getenv("CHUNK_DAYS", "3"))
TIME_BUDGET_S    = int(os.getenv("TIME_BUDGET_S", "300"))  # 5 phút
PACE_MS          = int(os.getenv("PACE_MS", "800"))
RATE_LIMIT_RETRIES = int(os.getenv("RATE_LIMIT_RETRIES", "4"))

SPREADSHEET_ID   = os.environ["SPREADSHEET_ID"]            # bắt buộc (GitHub Secret)
GOOGLE_CREDENTIALS = os.environ["GOOGLE_CREDENTIALS"]      # JSON SA (GitHub Secret)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("fb-sheets-sync")

_LAST_CALL_TS = 0
RATE_LIMIT_ERR = "RATE_LIMIT"

# ============== Google Sheets helpers ==============
def sheets():
    info = json.loads(GOOGLE_CREDENTIALS)
    creds = Credentials.from_service_account_info(
        info, scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return build("sheets", "v4", credentials=creds)

def read_range(tab: str, rng: str):
    svc = sheets()
    return svc.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID, range=f"{tab}!{rng}"
    ).execute().get("values", [])

def write_range(tab: str, start_cell: str, values: List[List]):
    svc = sheets()
    svc.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{tab}!{start_cell}",
        valueInputOption="RAW",
        body={"values": values}
    ).execute()

def append_rows(tab: str, rows: List[List]):
    if not rows: return
    svc = sheets()
    svc.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{tab}!A1",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": rows}
    ).execute()

# ============== State trong sheet _state (A1) ==============
def state_read_all() -> dict:
    v = read_range(STATE_TAB, "A1:A1")
    if v and v[0] and v[0][0]:
        try: return json.loads(v[0][0])
        except: return {}
    return {}

def state_write_all(obj: dict):
    write_range(STATE_TAB, "A1", [[json.dumps(obj)]] if obj else [[""]])

def state_read(key: str) -> Optional[dict]:
    return state_read_all().get(key)

def state_write(key: str, obj: dict):
    s = state_read_all()
    s[key] = obj
    state_write_all(s)

def state_clear(key: str):
    s = state_read_all()
    if key in s: del s[key]
    state_write_all(s)

# ============== Utils ==============
def pace():
    global _LAST_CALL_TS
    now = time.time()*1000
    wait = PACE_MS - (now - _LAST_CALL_TS) if _LAST_CALL_TS else 0
    if wait > 0: time.sleep(wait/1000.0)
    _LAST_CALL_TS = time.time()*1000

def to_num(v):
    try:
        n = float(v)
        return 0 if math.isnan(n) or math.isinf(n) else n
    except: return 0

def round2(x):
    try: return round(float(x)+1e-12, 2)
    except: return ""

def pct2(a,b):
    n, d = to_num(a), to_num(b)
    return "" if d <= 0 else round2((n/d)*100)

def minor_unit_divisor(cur: str) -> int:
    return 1 if (cur or "").upper() in ("VND","JPY","KRW") else 100

def headers_vn():
    return [
        "NGÀY BẮT ĐẦU","ID TÀI KHOẢN","TÊN TÀI KHOẢN",
        "TÊN CHIẾN DỊCH","NGÂN SÁCH CHIẾN DỊCH (VND)",
        "TÊN NHÓM QUẢNG CÁO","NGÂN SÁCH NHÓM QUẢNG CÁO (VND)","CHI TIÊU NHÓM QUẢNG CÁO (VND)",
        "TÊN QUẢNG CÁO","LƯỢT BẮT ĐẦU TRÒ CHUYỆN","KẾT QUẢ","CHI PHÍ/MỖI KẾT QUẢ (VND)",
        "CHI TIÊU QUẢNG CÁO (VND)","CPC CLICK (QC) (VND)","CPC TẤT CẢ (QC) (VND)",
        "CTR CLICK (QC) (%)","CTR TẤT CẢ (QC) (%)","CPM (QC) (VND)",
        "LƯỢT HIỂN THỊ (QC)","NGƯỜI TIẾP CẬN (QC)"
    ]

def slice_dates(since: str, until: str, chunk_days: int) -> List[dict]:
    s = datetime.date.fromisoformat(since)
    u = datetime.date.fromisoformat(until)
    out = []
    cur = s
    while cur <= u:
        end = cur + datetime.timedelta(days=chunk_days-1)
        if end > u: end = u
        out.append({"since": cur.isoformat(), "until": end.isoformat()})
        cur = end + datetime.timedelta(days=1)
    return out

def to_ymd(s: str) -> str:
    s = (s or "").strip()
    if not s: return ""
    s = s.split(" ")[0]
    if "/" in s:
        d,m,y = s.split("/")
        return f"{y}-{m.zfill(2)}-{d.zfill(2)}"
    return s

def normalize_act(x: str) -> str:
    v = (x or "").strip()
    return v if v.startswith("act_") else ("act_"+v) if v else ""

# ============== Đọc cấu hình từ sheet 'api' ==============
def get_config() -> dict:
    d2 = read_range("api","D2:D2")
    d3 = read_range("api","D3:D3")
    d4 = read_range("api","D4:D4")
    d6 = read_range("api","D6:D6")
    since = to_ymd(d2[0][0] if d2 and d2[0] else "")
    until = to_ymd(d3[0][0] if d3 and d3[0] else "")
    accounts_raw = (d4[0][0] if d4 and d4[0] else "").strip()
    token = (d6[0][0] if d6 and d6[0] else "").strip()
    if not (since and until and accounts_raw and token):
        raise ValueError("Thiếu D2/D3/D4/D6 trong sheet 'api'.")
    accounts = [normalize_act(x) for x in accounts_raw.split(",") if x.strip()]
    if not accounts: raise ValueError("D4 không có account hợp lệ.")
    # FX G:H
    fx_rows = read_range("api","G2:H1000")
    fx = {"VND": 1.0}
    for r in fx_rows:
        if len(r) >= 2:
            cur = (r[0] or "").strip().upper()
            try: rate = float(r[1])
            except: rate = 0
            if cur and rate > 0: fx[cur] = rate
    return {"since": since, "until": until, "token": token, "accounts": accounts, "fx": fx}

# ============== Facebook Graph API wrappers ==============
def with_token(url: str, token: str) -> str:
    sep = "&" if "?" in url else "?"
    return f"{url}{sep}access_token={requests.utils.quote(token)}"

def fb_get(url: str, try_count=0):
    pace()
    MAX_TRIES = 8
    base_bo = 1.2
    backoff = min(base_bo*(1.8**try_count) + random.random()*0.7, 25.0)
    r = requests.get(url, timeout=60)
    code = r.status_code
    if 200 <= code < 300: return r.json()
    err = None
    try: err = r.json().get("error")
    except: pass
    # 403 code 4 (rate limit) hoặc transient
    if code == 403 and err and (err.get("code")==4 or err.get("is_transient") is True):
        if try_count < RATE_LIMIT_RETRIES:
            time.sleep(backoff); return fb_get(url, try_count+1)
        raise RuntimeError(RATE_LIMIT_ERR)
    # 400/17, 429, 5xx → retry
    if (code == 400 and err and str(err.get("code"))=="17") or code == 429 or code >= 500:
        if try_count < MAX_TRIES:
            time.sleep(backoff); return fb_get(url, try_count+1)
        raise RuntimeError(f"HTTP {code} after retries: {r.text}")
    raise RuntimeError(f"HTTP {code}: {r.text}")

def fb_paged(url_no_token: str, token: str) -> List[dict]:
    out = []
    next_url = with_token(url_no_token, token)
    guard = 0
    while next_url:
        j = fb_get(next_url)
        out.extend(j.get("data",[]) or [])
        next_url = j.get("paging",{}).get("next")
        guard += 1
        if guard > 10000: raise RuntimeError("Paging overflow.")
    return out

def fetch_account_meta(act_id: str, token: str) -> dict:
    url = f"https://graph.facebook.com/{FB_API_VERSION}/{requests.utils.quote(act_id)}?fields=name,currency"
    try:
        j = fb_get(with_token(url, token))
        return {"name": j.get("name",""), "currency": j.get("currency","VND")}
    except Exception as e:
        if str(e)==RATE_LIMIT_ERR: raise
        return {"name":"","currency":"VND"}

def fetch_budgets_only(act_id: str, token: str) -> dict:
    base = f"https://graph.facebook.com/{FB_API_VERSION}"
    act = requests.utils.quote(act_id)
    campaigns = fb_paged(f"{base}/{act}/campaigns?fields=id,daily_budget,lifetime_budget&limit=500", token)
    adsets    = fb_paged(f"{base}/{act}/adsets?fields=id,campaign_id,daily_budget,lifetime_budget&limit=500", token)
    return {"campaigns": campaigns, "adsets": adsets}

def fetch_adset_spend_map_vnd(act_id: str, token: str, since: str, until: str, rate: float) -> dict:
    act = requests.utils.quote(act_id)
    base = f"https://graph.facebook.com/{FB_API_VERSION}/{act}/insights"
    params = {
        "level":"adset", "fields":"date_start,adset_id,spend", "limit":"500",
        "time_range": json.dumps({"since":since,"until":until}),
        "time_increment":"1"
    }
    q = "&".join([f"{k}={requests.utils.quote(str(v))}" for k,v in params.items()])
    next_url = with_token(f"{base}?{q}", token)
    out = {}; guard = 0
    while next_url:
        j = fb_get(next_url)
        for row in j.get("data",[]) or []:
            key = f"{row.get('adset_id','')}|{row.get('date_start','')}"
            out[key] = (float(row.get("spend",0)) if row.get("spend") else 0.0) * rate
        next_url = j.get("paging",{}).get("next")
        guard += 1
        if guard > 10000: raise RuntimeError("Paging overflow (adset spend).")
    return out

def build_budget_maps_vnd(campaigns, adsets, rate, divisor):
    camp_map = {}
    for c in campaigns or []:
        daily    = (float(c["daily_budget"])/divisor)*rate if c.get("daily_budget") else ""
        lifetime = (float(c["lifetime_budget"])/divisor)*rate if c.get("lifetime_budget") else ""
        camp_map[c["id"]] = {"daily": daily, "lifetime": lifetime}
    adset_map = {}
    for s in adsets or []:
        daily    = (float(s["daily_budget"])/divisor)*rate if s.get("daily_budget") else ""
        lifetime = (float(s["lifetime_budget"])/divisor)*rate if s.get("lifetime_budget") else ""
        adset_map[s["id"]] = {"daily": daily, "lifetime": lifetime, "campaign_id": s.get("campaign_id")}
    return camp_map, adset_map

def extract_msg_started(row: dict) -> int:
    arr = row.get("actions")
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
            at = str(it.get("action_type","")).lower()
            if at == nd:
                try: return int(float(it.get("value",0)))
                except: return 0
    for k in keys:
        nd = k.lower()
        for it in arr:
            at = str(it.get("action_type","")).lower()
            if nd in at:
                try: return int(float(it.get("value",0)))
                except: return 0
    return 0

def fetch_insights_ad(act_id: str, token: str, since: str, until: str) -> List[dict]:
    act = requests.utils.quote(act_id)
    base = f"https://graph.facebook.com/{FB_API_VERSION}/{act}/insights"
    fields = ",".join([
        "date_start","account_id","campaign_id","campaign_name",
        "adset_id","adset_name","ad_id","ad_name",
        "impressions","reach","spend","clicks","inline_link_clicks","actions"
    ])
    params = {
        "level":"ad", "fields":fields, "limit":"500",
        "time_range": json.dumps({"since":since,"until":until}),
        "time_increment": str(TIME_INCREMENT),
        "action_report_time":"conversion"
    }
    q = "&".join([f"{k}={requests.utils.quote(str(v))}" for k,v in params.items()])
    url = with_token(f"{base}?{q}", token)
    all_rows = []; guard = 0
    while url:
        j = fb_get(url)
        all_rows.extend(j.get("data",[]) or [])
        url = j.get("paging",{}).get("next")
        guard += 1
        if guard > 10000: raise RuntimeError("Paging overflow.")
    return all_rows

def map_rows(ad_rows, adset_map, camp_map, adset_spend_map, account_name, rate) -> List[dict]:
    out = []
    for r in ad_rows or []:
        s = adset_map.get(r.get("adset_id",""), {})
        c = camp_map.get(r.get("campaign_id",""), {})
        key = f"{r.get('adset_id','')}|{r.get('date_start','')}"
        adset_spend_vnd = adset_spend_map.get(key, "")

        spend_vnd = to_num(r.get("spend")) * rate
        clicks = to_num(r.get("clicks"))
        impr = to_num(r.get("impressions"))
        link_clicks = to_num(r.get("inline_link_clicks"))

        cpc_click_vnd = (spend_vnd/link_clicks) if link_clicks>0 else ""
        cpc_all_vnd   = (spend_vnd/clicks) if clicks>0 else ""
        ctr_click_pct = pct2(link_clicks, impr)
        ctr_all_pct   = pct2(clicks, impr)
        cpm_vnd       = ((spend_vnd/impr)*1000.0) if impr>0 else ""

        msg = extract_msg_started(r)
        cost_per_msg = (spend_vnd/msg) if msg>0 else ""

        out.append({
            "NGÀY BẮT ĐẦU": r.get("date_start",""),
            "ID TÀI KHOẢN": r.get("account_id",""),
            "TÊN TÀI KHOẢN": account_name or "",
            "TÊN CHIẾN DỊCH": r.get("campaign_name",""),
            "NGÂN SÁCH CHIẾN DỊCH (VND)": (camp_map.get(s.get("campaign_id",""),{}).get("daily","") if s.get("campaign_id") else c.get("daily","")),
            "TÊN NHÓM QUẢNG CÁO": r.get("adset_name",""),
            "NGÂN SÁCH NHÓM QUẢNG CÁO (VND)": s.get("daily",""),
            "CHI TIÊU NHÓM QUẢNG CÁO (VND)": adset_spend_vnd or "",
            "TÊN QUẢNG CÁO": r.get("ad_name",""),
            "LƯỢT BẮT ĐẦU TRÒ CHUYỆN": msg or "",
            "KẾT QUẢ": msg or "",
            "CHI PHÍ/MỖI KẾT QUẢ (VND)": cost_per_msg or "",
            "CHI TIÊU QUẢNG CÁO (VND)": spend_vnd or "",
            "CPC CLICK (QC) (VND)": cpc_click_vnd or "",
            "CPC TẤT CẢ (QC) (VND)": cpc_all_vnd or "",
            "CTR CLICK (QC) (%)": ctr_click_pct or "",
            "CTR TẤT CẢ (QC) (%)": ctr_all_pct or "",
            "CPM (QC) (VND)": cpm_vnd or "",
            "LƯỢT HIỂN THỊ (QC)": impr or "",
            "NGƯỜI TIẾP CẬN (QC)": r.get("reach","") or ""
        })
    return out

# ============== Ghi sheet "Data" ==============
def write_data_fresh(rows: List[dict]):
    hdr = headers_vn()
    matrix = [hdr] + [[r.get(h,"") for h in hdr] for r in rows]
    # clear toàn bộ Data trước khi ghi
    svc = sheets()
    svc.spreadsheets().values().clear(
        spreadsheetId=SPREADSHEET_ID, range=f"{SHEET_DATA_NAME}!A:ZZ"
    ).execute()
    write_range(SHEET_DATA_NAME, "A1", matrix if rows else [hdr])

def write_data_append(rows: List[dict]):
    if not rows: return
    hdr = headers_vn()
    # đảm bảo header tồn tại/đúng thứ tự
    cur = read_range(SHEET_DATA_NAME, "1:1")
    if not (cur and cur[0] and len(cur[0])==len(hdr) and all(cur[0][i]==hdr[i] for i in range(len(hdr)))):
        write_data_fresh([])  # chỉ ghi header
    matrix = [[r.get(h,"") for h in hdr] for r in rows]
    append_rows(SHEET_DATA_NAME, matrix)

# ============== Luồng giống updateDataOnlyTimed ==============
def run_timed():
    start = time.time()
    cfg = get_config()
    state = state_read(STATE_KEY)
    # khởi tạo queue nếu chưa có
    if not state or not isinstance(state.get("queue",[]), list) or not state["queue"]:
        chunks = slice_dates(cfg["since"], cfg["until"], CHUNK_DAYS)
        queue = []
        for act in cfg["accounts"]:
            for ch in chunks:
                queue.append({"actId": act, "since": ch["since"], "until": ch["until"]})
        state = {"queue": queue, "initialized": False}

    if not state["initialized"]:
        write_data_fresh([])  # clear + header
        state["initialized"] = True
        state_write(STATE_KEY, state)

    buffer = []
    while state["queue"] and (time.time()-start) < (TIME_BUDGET_S - 15):
        job = state["queue"].pop(0)

        meta = fetch_account_meta(job["actId"], cfg["token"])
        cur  = (meta.get("currency") or "VND").upper()
        rate = 1.0 if cur=="VND" else float(cfg["fx"].get(cur, 0))
        if cur!="VND" and (not rate or rate<=0):
            raise ValueError(f"Thiếu tỷ giá VND cho \"{cur}\" tại 'api'!G:H.")
        divisor = minor_unit_divisor(cur)

        budgets = fetch_budgets_only(job["actId"], cfg["token"])
        camp_map, adset_map = build_budget_maps_vnd(budgets["campaigns"], budgets["adsets"], rate, divisor)

        adset_spend = fetch_adset_spend_map_vnd(job["actId"], cfg["token"], job["since"], job["until"], rate)
        ad_rows_raw = fetch_insights_ad(job["actId"], cfg["token"], job["since"], job["until"])
        rows_out = map_rows(ad_rows_raw, adset_map, camp_map, adset_spend, meta["name"], rate)
        buffer.extend(rows_out)

    if buffer: write_data_append(buffer)

    if state["queue"]:
        state_write(STATE_KEY, state)
        return {"status":"paused","remaining_jobs":len(state["queue"]),"flushed_rows":len(buffer)}
    else:
        state_clear(STATE_KEY)
        return {"status":"done","flushed_rows":len(buffer)}

# ============== Luồng giống runAll/updateDataOnly ==============
def run_all():
    cfg = get_config()
    all_rows = []
    for act in cfg["accounts"]:
        meta = fetch_account_meta(act, cfg["token"])
        cur  = (meta.get("currency") or "VND").upper()
        rate = 1.0 if cur=="VND" else float(cfg["fx"].get(cur, 0))
        if cur!="VND" and (not rate or rate<=0):
            raise ValueError(f"Thiếu tỷ giá VND cho \"{cur}\" tại 'api'!G:H.")
        divisor = minor_unit_divisor(cur)

        budgets = fetch_budgets_only(act, cfg["token"])
        camp_map, adset_map = build_budget_maps_vnd(budgets["campaigns"], budgets["adsets"], rate, divisor)

        adset_spend = fetch_adset_spend_map_vnd(act, cfg["token"], cfg["since"], cfg["until"], rate)
        ad_rows_raw = fetch_insights_ad(act, cfg["token"], cfg["since"], cfg["until"])
        rows_out = map_rows(ad_rows_raw, adset_map, camp_map, adset_spend, meta["name"], rate)
        all_rows.extend(rows_out)

    write_data_fresh(all_rows)
    return {"status":"done","flushed_rows":len(all_rows)}

if __name__ == "__main__":
    # MODE=timed (resume theo lô) | MODE=runall (chạy trọn)
    mode = os.getenv("MODE", "timed")
    res = run_timed() if mode=="timed" else run_all()
    print(json.dumps(res))
