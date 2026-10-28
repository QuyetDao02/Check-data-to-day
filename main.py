# main.py
import os
import io
import json
import time
import math
import random
import datetime
import csv
import logging
import pathlib
from typing import List, Tuple

import requests
import yaml

ROOT = pathlib.Path(__file__).resolve().parent
CONFIG_PATH = ROOT / "config" / "config.yml"
CSV_PATH = ROOT / "data" / "latest.csv"

FB_API_VERSION = "v25.0"

HEADERS_VN = [
    "NGÀY BẮT ĐẦU",
    "ID TÀI KHOẢN",
    "TÊN TÀI KHOẢN",
    "ID CHIẾN DỊCH",
    "TÊN CHIẾN DỊCH",
    "CHI TIÊU CHIẾN DỊCH (VND)",
    "SỐ MESS",
    "SỐ LEAD"
]

PACE_MS = int(float(os.environ.get("PACE_MS", 200)))
RATE_LIMIT_RETRIES = int(
    float(os.environ.get("RATE_LIMIT_RETRIES", 4))
)
RATE_LIMIT_COOLDOWN = int(
    float(os.environ.get("RATE_LIMIT_COOLDOWN", 45))
)
PAGE_BURST = int(float(os.environ.get("PAGE_BURST", 100)))
PAGE_BURST_SLEEP = int(float(os.environ.get("PAGE_BURST_SLEEP", 1)))
ACCT_COOLDOWN = int(float(os.environ.get("ACCT_COOLDOWN", 1)))

RATE_LIMIT_ERR = "RATE_LIMIT"
DEBUG = os.environ.get("DEBUG", "0") == "1"
REPORT_TIME = (
    os.environ.get("REPORT_TIME") or "conversion"
).strip().lower()

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("fb-campaign-export")

_LAST_TS = 0
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "MetaAdsCSVExporter/1.0"
})


def emit_error_csv(msg: str):
    CSV_PATH.parent.mkdir(parents=True, exist_ok=True)

    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["ERROR"])
        writer.writerow([(msg or "").strip()])


def _env_int(name: str, default: int) -> int:
    value = os.environ.get(name)

    if value is None or not str(value).strip():
        return default

    try:
        return int(float(value))
    except Exception:
        return default


def _apply_env_overrides():
    global PACE_MS, RATE_LIMIT_RETRIES

    PACE_MS = _env_int("PACE_MS", PACE_MS)
    RATE_LIMIT_RETRIES = _env_int(
        "RATE_LIMIT_RETRIES", RATE_LIMIT_RETRIES
    )


def pace():
    global _LAST_TS

    now = time.time() * 1000

    if _LAST_TS:
        wait = PACE_MS - (now - _LAST_TS)
        if wait > 0:
            time.sleep(wait / 1000)

    _LAST_TS = time.time() * 1000


def to_num(value):
    try:
        number = float(value)
        return number if math.isfinite(number) else 0.0
    except Exception:
        return 0.0


def money0(value) -> int:
    try:
        return int(round(float(value)))
    except Exception:
        return 0


def quote(value) -> str:
    return requests.utils.quote(str(value))


def with_token(url: str, token: str) -> str:
    sep = "&" if "?" in url else "?"
    return f"{url}{sep}access_token={requests.utils.quote(token)}"


def fb_get(url: str, token: str, try_count=0):
    pace()

    max_tries = max(RATE_LIMIT_RETRIES, 5)
    backoff = min(
        3.0 * (2.0 ** try_count) + random.uniform(0.5, 2.0),
        60.0
    )

    try:
        response = SESSION.get(
            with_token(url, token),
            timeout=(10, 90)
        )
    except requests.RequestException as error:
        if try_count < max_tries:
            log.warning(
                "Request error, retry %s/%s after %.1fs: %s",
                try_count + 1, max_tries, backoff, error
            )
            time.sleep(backoff)
            return fb_get(url, token, try_count + 1)

        raise RuntimeError(f"REQUEST_ERROR: {error}")

    if 200 <= response.status_code < 300:
        try:
            return response.json()
        except Exception:
            raise RuntimeError(
                "Meta API trả về dữ liệu JSON không hợp lệ."
            )

    try:
        error_json = response.json()
    except Exception:
        error_json = {}

    error_info = (
        error_json.get("error", {})
        if isinstance(error_json, dict) else {}
    )

    error_code = str(error_info.get("code", ""))
    error_subcode = str(error_info.get("error_subcode", ""))
    error_message = str(error_info.get("message", ""))

    if DEBUG:
        print(f"[FB_ERR] HTTP {response.status_code} @ {url.split('?')[0]}")
        print("[FB_ERR_BODY]", error_json or response.text[:500])

    temporary_error = (
        response.status_code == 429
        or response.status_code >= 500
        or error_code in {"2", "4", "17", "613"}
        or error_subcode == "1504044"
        or bool(error_info.get("is_transient"))
        or "temporarily unavailable" in error_message.lower()
    )

    if temporary_error:
        if try_count < max_tries:
            log.warning(
                "Meta temporary error: HTTP=%s code=%s "
                "subcode=%s retry=%s/%s sleep=%.1fs",
                response.status_code, error_code, error_subcode,
                try_count + 1, max_tries, backoff
            )
            time.sleep(backoff)
            return fb_get(url, token, try_count + 1)

        raise RuntimeError(
            f"META_TEMPORARY_ERROR: HTTP {response.status_code}, "
            f"code={error_code}, subcode={error_subcode}: "
            f"{error_message}"
        )

    raise RuntimeError(
        f"HTTP {response.status_code}: {response.text}"
    )


def fb_get_safely(url: str, token: str):
    try:
        return fb_get(url, token)
    except RuntimeError as error:
        message = str(error)

        if (
            "1504044" in message
            or "META_TEMPORARY_ERROR" in message
            or "Service temporarily unavailable" in message
        ):
            raise RuntimeError("META_SKIP_ACCOUNT")

        raise


def fb_paged(url: str, token: str) -> List[dict]:
    rows = []
    page_count = 0

    while url:
        data = fb_get_safely(url, token)
        rows.extend(data.get("data", []) or [])
        url = data.get("paging", {}).get("next")
        page_count += 1

        if (
            url
            and PAGE_BURST > 0
            and page_count % PAGE_BURST == 0
            and PAGE_BURST_SLEEP > 0
        ):
            time.sleep(PAGE_BURST_SLEEP)

        if page_count > 10000:
            raise RuntimeError("Paging overflow.")

    return rows


def fetch_account_meta(act_id: str, token: str) -> dict:
    url = (
        f"https://graph.facebook.com/{FB_API_VERSION}/"
        f"{quote(act_id)}?fields=name,currency"
    )

    try:
        data = fb_get_safely(url, token)
        return {
            "name": data.get("name", ""),
            "currency": data.get("currency", "VND")
        }
    except Exception as error:
        if str(error) == RATE_LIMIT_ERR:
            raise

        log.warning("Không lấy được thông tin tài khoản %s: %s", act_id, error)
        return {"name": "", "currency": "VND"}
def fetch_campaign_insights(
    act_id: str,
    since: str,
    until: str,
    token: str
) -> List[dict]:
    """Lấy dữ liệu cấp Campaign; chỉ fallback nếu lỗi tham số."""
    act = quote(act_id)
    base = f"https://graph.facebook.com/{FB_API_VERSION}/{act}/insights"

    base_fields = [
        "date_start", "account_id", "campaign_id",
        "campaign_name", "spend"
    ]
    action_fields = ["actions", "cost_per_action_type"]

    def build_url(include_actions: bool) -> str:
        fields = base_fields + (
            action_fields if include_actions else []
        )

        params = {
            "level": "campaign",
            "limit": "500",
            "time_range": json.dumps({
                "since": since,
                "until": until
            }),
            "time_increment": "1",
            "use_unified_attribution_setting": "true",
            "fields": ",".join(fields)
        }

        if REPORT_TIME in ("conversion", "impression"):
            params["action_report_time"] = REPORT_TIME

        query = "&".join(
            f"{key}={quote(value)}"
            for key, value in params.items()
        )
        return f"{base}?{query}"

    for include_actions in (True, False):
        url = build_url(include_actions)
        output = []
        page_count = 0
        fallback_needed = False

        if DEBUG:
            print(f"[INSIGHTS] include_actions={include_actions}")

        while url:
            try:
                data = fb_get_safely(url, token)
            except RuntimeError as error:
                message = str(error)
                invalid_parameter = (
                    "Invalid parameter" in message
                    or '"code":100' in message
                    or 'error_subcode":1504018' in message
                )

                if include_actions and invalid_parameter:
                    fallback_needed = True
                    break

                raise

            output.extend(data.get("data", []) or [])
            url = data.get("paging", {}).get("next")
            page_count += 1

            if page_count > 10000:
                raise RuntimeError("Paging overflow.")

            if (
                url
                and PAGE_BURST > 0
                and page_count % PAGE_BURST == 0
                and PAGE_BURST_SLEEP > 0
            ):
                time.sleep(PAGE_BURST_SLEEP)

        if fallback_needed:
            continue

        if DEBUG:
            print(f"[INSIGHTS] success rows={len(output)}")

        return output

    return []


LEAD_KEYS_PRIORITY = [
    "lead",
    "onsite_conversion.lead",
    "leadgen",
    "onsite_conversion.lead_grouped"
]

MSG_KEYS_PRIORITY = [
    "messaging_conversations_started",
    "messaging_conversation_started",
    "onsite_conversion.messaging_conversation_started_7d",
    "onsite_conversion.messaging_first_reply",
    "onsite_conversion.messaging_conversation_started_28d",
    "onsite_conversion.messaging_conversation_started_1d"
]


def _actions_map(row: dict) -> dict:
    result = {}
    actions = row.get("actions")

    if not isinstance(actions, list):
        return result

    for item in actions:
        action_type = str(item.get("action_type", "")).lower()
        value = to_num(item.get("value"))

        if action_type not in result:
            result[action_type] = value

    return result


def _pick_first(
    action_map: dict,
    keys: List[str]
) -> Tuple[float, str]:
    for key in keys:
        if key in action_map:
            return action_map[key], key

    for key in keys:
        for actual_key, value in action_map.items():
            if key in actual_key:
                return value, actual_key

    return 0.0, ""


def extract_lead_count(row: dict) -> Tuple[int, str]:
    action_map = _actions_map(row)
    value, key = _pick_first(action_map, LEAD_KEYS_PRIORITY)
    return int(round(value)), key


def extract_msg_started(row: dict) -> int:
    action_map = _actions_map(row)
    value, _ = _pick_first(action_map, MSG_KEYS_PRIORITY)
    return int(round(value))


def map_rows(ad_rows, account_name, rate):
    output = []

    for row in ad_rows or []:
        spend_vnd = money0(to_num(row.get("spend")) * rate)
        lead_count, _ = extract_lead_count(row)
        msg_started = extract_msg_started(row)

        output.append([
            row.get("date_start", ""),
            row.get("account_id", ""),
            account_name or "",
            row.get("campaign_id", ""),
            row.get("campaign_name", ""),
            spend_vnd,
            msg_started,
            lead_count
        ])

    return output
def to_ymd_any(value: str) -> str:
    text = (value or "").strip()

    if not text:
        return ""

    text = text.split(" ")[0]

    if "/" in text:
        parts = text.split("/")

        if len(parts) == 3:
            day, month, year = parts
            return (
                f"{int(year):04d}-"
                f"{int(month):02d}-"
                f"{int(day):02d}"
            )

    return text


def _csv_rows_from_gsheet_csv(
    sheet_id: str,
    sheet_name: str = None,
    gid: str = None,
    a1_range: str = None
):
    urls = []

    if gid:
        url = (
            f"https://docs.google.com/spreadsheets/d/{sheet_id}"
            f"/export?format=csv&gid={gid}"
        )

        if a1_range:
            url += f"&range={a1_range}"

        urls.append(url)

    base = (
        f"https://docs.google.com/spreadsheets/d/{sheet_id}"
        "/gviz/tq?tqx=out:csv"
    )

    if sheet_name:
        base += f"&sheet={quote(sheet_name)}"

    if a1_range:
        base += f"&range={a1_range}"

    urls.append(base)
    last_error = None

    for url in urls:
        try:
            response = SESSION.get(url, timeout=(10, 30))
            response.raise_for_status()

            rows = list(csv.reader(io.StringIO(response.text)))

            if rows:
                return rows

        except Exception as error:
            last_error = error

    if last_error:
        raise last_error

    return []


def _clamp_dates(since: str, until: str):
    today = datetime.date.today()
    start = datetime.date.fromisoformat(since)
    end = datetime.date.fromisoformat(until)

    if end > today:
        end = today

    if start > end:
        start = end

    return start.isoformat(), end.isoformat()


def load_from_sheet_or_fail():
    sheet_id = os.environ.get("SHEET_ID")

    if not sheet_id:
        emit_error_csv("Thiếu biến SHEET_ID.")
        raise SystemExit(1)

    sheet_name = os.environ.get("API_SHEET_NAME", "api")
    sheet_gid = os.environ.get("API_SHEET_GID")

    d_values = _csv_rows_from_gsheet_csv(
        sheet_id,
        sheet_name=sheet_name,
        gid=sheet_gid,
        a1_range="D2:D4"
    )

    values = [
        row[0].strip() if row and len(row) >= 1 else ""
        for row in d_values
    ]

    since_raw = values[0] if len(values) > 0 else ""
    until_raw = values[1] if len(values) > 1 else ""
    accounts_text = values[2] if len(values) > 2 else ""

    since = to_ymd_any(since_raw)
    until = to_ymd_any(until_raw)

    if not since or not until or not accounts_text:
        emit_error_csv("Thiếu cấu hình api!D2, D3 hoặc D4.")
        raise SystemExit(1)

    try:
        datetime.date.fromisoformat(since)
    except Exception:
        emit_error_csv("Sai định dạng since.")
        raise SystemExit(1)

    try:
        datetime.date.fromisoformat(until)
    except Exception:
        emit_error_csv("Sai định dạng until.")
        raise SystemExit(1)

    since, until = _clamp_dates(since, until)

    accounts = [
        account.strip()
        for account in accounts_text.split(",")
        if account.strip()
    ]

    accounts = [
        account if account.startswith("act_") else f"act_{account}"
        for account in accounts
    ]

    fx_rows = _csv_rows_from_gsheet_csv(
        sheet_id,
        sheet_name=sheet_name,
        gid=sheet_gid,
        a1_range="G2:H"
    )

    fx = {}

    for row in fx_rows:
        if len(row) >= 2 and row[0] and row[1]:
            try:
                currency = str(row[0]).strip().upper()
                rate = float(str(row[1]).strip())

                if rate > 0:
                    fx[currency] = rate
            except Exception:
                pass

    if "VND" not in fx:
        fx["VND"] = 1.0

    _apply_env_overrides()

    return {
        "since": since,
        "until": until,
        "accounts": accounts,
        "fx": fx
    }


def load_from_config_file_or_fail():
    if not CONFIG_PATH.exists():
        emit_error_csv(
            "Thiếu SHEET_ID hoặc config/config.yml."
        )
        raise SystemExit(1)

    try:
        config = yaml.safe_load(
            CONFIG_PATH.read_text(encoding="utf-8")
        ) or {}
    except Exception as error:
        emit_error_csv(f"Lỗi đọc config.yml: {error}")
        raise SystemExit(1)

    missing = []

    if not config.get("since"):
        missing.append("since")
    if not config.get("until"):
        missing.append("until")
    if not config.get("accounts"):
        missing.append("accounts")

    if missing:
        emit_error_csv("Thiếu cấu hình: " + ", ".join(missing))
        raise SystemExit(1)

    since = to_ymd_any(config["since"])
    until = to_ymd_any(config["until"])
    since, until = _clamp_dates(since, until)

    accounts = [
        str(account).strip()
        for account in config["accounts"]
        if str(account).strip()
    ]

    accounts = [
        account if account.startswith("act_") else f"act_{account}"
        for account in accounts
    ]

    fx = {"VND": 1.0}

    for currency, value in (config.get("fx") or {}).items():
        try:
            rate = float(value)

            if rate > 0:
                fx[str(currency).upper()] = rate
        except Exception:
            pass

    _apply_env_overrides()

    return {
        "since": since,
        "until": until,
        "accounts": accounts,
        "fx": fx
    }


def load_config_or_fail():
    if os.environ.get("SHEET_ID"):
        return load_from_sheet_or_fail()

    return load_from_config_file_or_fail()
def write_full_csv(rows: List[List]):
    CSV_PATH.parent.mkdir(parents=True, exist_ok=True)

    with open(CSV_PATH, "w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(HEADERS_VN)

        if rows:
            writer.writerows(rows)


def run_once():
    cfg = load_config_or_fail()

    token = os.environ.get("META_TOKEN")

    if not token:
        emit_error_csv("Thiếu META_TOKEN trong workflow.")
        raise SystemExit(1)

    all_rows: List[List] = []
    skipped_accounts = []

    for idx, act in enumerate(cfg["accounts"]):
        if idx > 0:
            time.sleep(ACCT_COOLDOWN)

        log.info("Running Campaign Level: %s", act)

        try:
            meta = fetch_account_meta(act, token)
            currency = (meta.get("currency") or "VND").upper()

            if currency == "VND":
                rate = 1.0
            else:
                rate = float(cfg["fx"].get(currency, 0))

            if currency != "VND" and rate <= 0:
                log.error(
                    "Thiếu tỷ giá cho %s - bỏ qua %s",
                    currency, act
                )
                skipped_accounts.append(act)
                continue

            campaign_rows = fetch_campaign_insights(
                act,
                cfg["since"],
                cfg["until"],
                token
            )

            rows = map_rows(
                campaign_rows,
                meta.get("name", ""),
                rate
            )

            all_rows.extend(rows)

            log.info(
                "Campaign Level done: %s | rows=%s",
                act, len(rows)
            )

        except RuntimeError as error:
            if str(error) == "META_SKIP_ACCOUNT":
                log.error(
                    "Meta unavailable for %s - skip account",
                    act
                )
                skipped_accounts.append(act)
                continue

            log.error("Account %s failed: %s", act, error)
            skipped_accounts.append(act)
            continue

        except Exception as error:
            log.exception(
                "Unexpected error on account %s: %s",
                act, error
            )
            skipped_accounts.append(act)
            continue

    write_full_csv(all_rows)

    log.info(
        "Finished. Rows=%s | Skipped accounts=%s",
        len(all_rows), len(skipped_accounts)
    )

    if skipped_accounts:
        log.warning("Skipped: %s", ", ".join(skipped_accounts))

    print(json.dumps({
        "status": "done",
        "rows": len(all_rows),
        "skipped_accounts": len(skipped_accounts)
    }, ensure_ascii=False))


if __name__ == "__main__":
    try:
        run_once()
    except SystemExit:
        raise
    except Exception as error:
        emit_error_csv(f"Lỗi không xác định: {error}")
        raise
