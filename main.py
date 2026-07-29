# main.py
import os, io, csv, json, time, math, random, datetime, pathlib, logging
from typing import List
import requests, yaml

ROOT = pathlib.Path(__file__).resolve().parent
CONFIG_PATH = ROOT / "config" / "config.yml"
CSV_PATH = ROOT / "data" / "latest.csv"

FB_API_VERSION = "v20.0"

HEADERS_VN = [
    "NGÀY BẮT ĐẦU",
    "ID TÀI KHOẢN",
    "TÊN TÀI KHOẢN",
    "ID CHIẾN DỊCH",
    "TÊN CHIẾN DỊCH",
    "NGÂN SÁCH CHIẾN DỊCH (VND)",
    "CHI TIÊU CHIẾN DỊCH (VND)",
    "LƯỢT BẮT ĐẦU TRÒ CHUYỆN",
    "KẾT QUẢ"
]

PACE_MS = int(float(os.environ.get("PACE_MS", 1000)))
RETRIES = int(float(os.environ.get("RATE_LIMIT_RETRIES", 6)))
DEBUG = os.environ.get("DEBUG", "0") == "1"
_LAST_REQUEST = 0.0

LEAD_KEYS = ["lead", "onsite_conversion.lead", "leadgen", "onsite_conversion.lead_grouped"]
MSG_KEYS = [
    "messaging_conversations_started",
    "messaging_conversation_started",
    "onsite_conversion.messaging_conversation_started_7d",
    "onsite_conversion.messaging_conversation_started_28d",
    "onsite_conversion.messaging_conversation_started_1d"
]

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("fb-campaign-export")


def pace():
    global _LAST_REQUEST
    wait = PACE_MS / 1000 - (time.time() - _LAST_REQUEST)
    if wait > 0:
        time.sleep(wait)
    _LAST_REQUEST = time.time()


def num(v):
    try:
        x = float(v)
        return x if math.isfinite(x) else 0.0
    except Exception:
        return 0.0


def money(v):
    return int(round(num(v)))


def minor_divisor(currency):
    return 1 if str(currency).upper() in ("VND", "JPY", "KRW") else 100


def ymd(v):
    s = str(v or "").strip().split(" ")[0]
    if "/" in s:
        d, m, y = s.split("/")[:3]
        return f"{int(y):04d}-{int(m):02d}-{int(d):02d}"
    return s


def clamp_dates(since, until):
    today = datetime.date.today()
    s = datetime.date.fromisoformat(since)
    u = datetime.date.fromisoformat(until)
    u = min(u, today)
    s = min(s, u)
    return s.isoformat(), u.isoformat()


def error_csv(message):
    CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        csv.writer(f).writerows([["ERROR"], [str(message)]])


def fb_get(url, token, attempt=0):
    pace()
    try:
        r = requests.get(url, params={"access_token": token}, timeout=60)
    except requests.RequestException as e:
        if attempt < RETRIES:
            time.sleep(min(2 ** attempt, 20))
            return fb_get(url, token, attempt + 1)
        raise RuntimeError(f"Request error: {e}")

    if 200 <= r.status_code < 300:
        return r.json()

    try:
        err = r.json().get("error", {})
    except Exception:
        err = {}

    code = str(err.get("code", ""))
    retryable = (
        r.status_code == 429 or code in ("4", "17", "613")
        or err.get("is_transient") or r.status_code >= 500
    )

    if retryable and attempt < RETRIES:
        delay = min(2 ** attempt + random.random(), 20)
        if DEBUG:
            log.warning("Retry Meta API %.1fs", delay)
        time.sleep(delay)
        return fb_get(url, token, attempt + 1)

    raise RuntimeError(f"Meta API HTTP {r.status_code}: {r.text[:1000]}")


def fb_all(url, token):
    rows = []
    pages = 0
    while url:
        data = fb_get(url, token)
        rows.extend(data.get("data", []) or [])
        url = (data.get("paging") or {}).get("next")
        pages += 1
        if pages > 10000:
            raise RuntimeError("Paging overflow.")
    return rows


def account_meta(act_id, token):
    url = f"https://graph.facebook.com/{FB_API_VERSION}/{act_id}?fields=name,currency"
    data = fb_get(url, token)
    return {
        "name": data.get("name", ""),
        "currency": data.get("currency", "VND").upper()
    }


def fetch_campaigns(act_id, token):
    url = (
        f"https://graph.facebook.com/{FB_API_VERSION}/{act_id}/campaigns"
        "?fields=id,name,objective,daily_budget,lifetime_budget"
        "&limit=500"
    )
    return fb_all(url, token)


def campaign_budget(campaign, rate, divisor):
    # Chỉ lấy daily_budget vì cột hiển thị là ngân sách chiến dịch.
    if campaign.get("daily_budget") not in (None, ""):
        return money(num(campaign["daily_budget"]) / divisor * rate)
    return ""


def fetch_campaign_insights(act_id, since, until, token):
    fields = ",".join([
        "date_start",
        "account_id",
        "campaign_id",
        "campaign_name",
        "spend",
        "actions"
    ])

    params = {
        "level": "campaign",
        "fields": fields,
        "limit": 500,
        "time_range": json.dumps({"since": since, "until": until}),
        "time_increment": 1,
        "use_unified_attribution_setting": "true"
    }

    query = "&".join(
        f"{k}={requests.utils.quote(str(v))}"
        for k, v in params.items()
    )

    url = f"https://graph.facebook.com/{FB_API_VERSION}/{act_id}/insights?{query}"
    return fb_all(url, token)


def action_map(row):
    result = {}
    for item in row.get("actions", []) or []:
        key = str(item.get("action_type", "")).lower()
        if key not in result:
            result[key] = num(item.get("value"))
    return result


def pick_action(actions, keys):
    for key in keys:
        if key in actions:
            return actions[key]
    for key in keys:
        for actual, value in actions.items():
            if key in actual:
                return value
    return 0


def lead_count(row):
    return int(round(pick_action(action_map(row), LEAD_KEYS)))


def message_started(row):
    return int(round(pick_action(action_map(row), MSG_KEYS)))


def read_sheet_csv(sheet_id, sheet_name, cell_range):
    url = (
        f"https://docs.google.com/spreadsheets/d/{sheet_id}"
        f"/gviz/tq?tqx=out:csv"
        f"&sheet={requests.utils.quote(sheet_name)}"
        f"&range={cell_range}"
    )
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return list(csv.reader(io.StringIO(r.text)))


def load_from_sheet():
    sheet_id = os.environ.get("SHEET_ID")
    if not sheet_id:
        return None

    sheet_name = os.environ.get("API_SHEET_NAME", "api")
    values = read_sheet_csv(sheet_id, sheet_name, "D2:D4")

    def val(i):
        return values[i][0].strip() if i < len(values) and values[i] else ""

    since, until, account_text = ymd(val(0)), ymd(val(1)), val(2)

    if not since or not until or not account_text:
        raise RuntimeError("Thiếu api!D2, D3 hoặc D4.")

    since, until = clamp_dates(since, until)

    accounts = [
        x if x.startswith("act_") else f"act_{x}"
        for x in account_text.split(",") if x.strip()
    ]

    fx = {"VND": 1.0}
    for row in read_sheet_csv(sheet_id, sheet_name, "G2:H"):
        if len(row) >= 2 and row[0] and row[1]:
            try:
                rate = float(row[1].replace(",", "").strip())
                if rate > 0:
                    fx[row[0].strip().upper()] = rate
            except Exception:
                pass

    return {"since": since, "until": until, "accounts": accounts, "fx": fx}


def load_from_yaml():
    if not CONFIG_PATH.exists():
        raise RuntimeError("Không có SHEET_ID và không tìm thấy config/config.yml.")

    cfg = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8")) or {}
    since, until = ymd(cfg.get("since")), ymd(cfg.get("until"))
    accounts = [str(x).strip() for x in (cfg.get("accounts") or []) if str(x).strip()]

    if not since or not until or not accounts:
        raise RuntimeError("config.yml thiếu since / until / accounts.")

    since, until = clamp_dates(since, until)
    accounts = [x if x.startswith("act_") else f"act_{x}" for x in accounts]

    fx = {"VND": 1.0}
    for currency, rate in (cfg.get("fx") or {}).items():
        try:
            if float(rate) > 0:
                fx[str(currency).upper()] = float(rate)
        except Exception:
            pass

    return {"since": since, "until": until, "accounts": accounts, "fx": fx}


def load_config():
    return load_from_sheet() if os.environ.get("SHEET_ID") else load_from_yaml()


def export_csv(rows):
    CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(HEADERS_VN)
        writer.writerows(rows)


def run():
    cfg = load_config()
    token = os.environ.get("META_TOKEN")
    if not token:
        raise RuntimeError("Thiếu META_TOKEN.")

    all_rows = []

    for i, act_id in enumerate(cfg["accounts"]):
        if i:
            time.sleep(2)

        log.info("Campaign Level: %s", act_id)

        meta = account_meta(act_id, token)
        currency = meta["currency"]
        rate = 1.0 if currency == "VND" else cfg["fx"].get(currency, 0)

        if not rate:
            raise RuntimeError(f"Thiếu tỷ giá {currency} → VND.")

        divisor = minor_divisor(currency)

        # Chỉ 2 request chính: campaigns + campaign insights.
        campaigns = fetch_campaigns(act_id, token)
        campaign_map = {c["id"]: c for c in campaigns}
        insights = fetch_campaign_insights(
            act_id, cfg["since"], cfg["until"], token
        )

        for row in insights:
            campaign_id = row.get("campaign_id", "")
            campaign = campaign_map.get(campaign_id, {})

            all_rows.append([
                row.get("date_start", ""),
                row.get("account_id", act_id),
                meta["name"],
                campaign_id,
                row.get("campaign_name", campaign.get("name", "")),
                campaign_budget(campaign, rate, divisor),
                money(num(row.get("spend")) * rate) or "",
                message_started(row) or "",
                lead_count(row) or ""
            ])

    export_csv(all_rows)
    print(json.dumps({
        "status": "done",
        "level": "campaign",
        "rows": len(all_rows)
    }, ensure_ascii=False))


if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        error_csv(e)
        raise
