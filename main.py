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


# =========================================================
# PATHS
# =========================================================
ROOT = pathlib.Path(__file__).resolve().parent
CONFIG_PATH = ROOT / "config" / "config.yml"
CSV_PATH = ROOT / "data" / "latest.csv"


# =========================================================
# CONFIG
# =========================================================
FB_API_VERSION = "v25.0"

HEADERS_VN = [
    "NGÀY BẮT ĐẦU",
    "ID TÀI KHOẢN",
    "TÊN TÀI KHOẢN",
    "ID CHIẾN DỊCH",
    "TÊN CHIẾN DỊCH",
    "CHI TIÊU CHIẾN DỊCH (VND)",
    "LƯỢT BẮT ĐẦU TRÒ CHUYỆN",
    "KẾT QUẢ",
]

PACE_MS = int(float(os.environ.get("PACE_MS", 1500)))
RATE_LIMIT_RETRIES = int(
    float(os.environ.get("RATE_LIMIT_RETRIES", 8))
)
RATE_LIMIT_COOLDOWN = int(
    float(os.environ.get("RATE_LIMIT_COOLDOWN", 120))
)
PAGE_BURST = int(float(os.environ.get("PAGE_BURST", 25)))
PAGE_BURST_SLEEP = int(float(os.environ.get("PAGE_BURST_SLEEP", 5)))
ACCT_COOLDOWN = int(float(os.environ.get("ACCT_COOLDOWN", 8)))

RATE_LIMIT_ERR = "RATE_LIMIT"

DEBUG = os.environ.get("DEBUG", "0") == "1"

REPORT_TIME = (
    os.environ.get("REPORT_TIME") or "conversion"
).strip().lower()

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("fb-campaign-export")

_LAST_TS = 0


# =========================================================
# ERROR CSV
# =========================================================
def emit_error_csv(msg: str):
    CSV_PATH.parent.mkdir(parents=True, exist_ok=True)

    with open(
        CSV_PATH,
        "w",
        newline="",
        encoding="utf-8"
    ) as f:
        writer = csv.writer(f)
        writer.writerow(["ERROR"])
        writer.writerow([(msg or "").strip()])


# =========================================================
# UTILS
# =========================================================
def _env_int(name: str, default: int) -> int:
    value = os.environ.get(name)

    if value is None:
        return default

    value = str(value).strip()

    if not value:
        return default

    try:
        return int(float(value))
    except Exception:
        return default


def _apply_env_overrides():
    global PACE_MS
    global RATE_LIMIT_RETRIES

    PACE_MS = _env_int("PACE_MS", PACE_MS)
    RATE_LIMIT_RETRIES = _env_int(
        "RATE_LIMIT_RETRIES",
        RATE_LIMIT_RETRIES
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

        if math.isfinite(number):
            return number

        return 0.0

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
    
# =========================================================
# FACEBOOK API
# =========================================================
def fb_get(url: str, token: str, try_count=0):
    pace()

    MAX_TRIES = max(RATE_LIMIT_RETRIES, 5)

    backoff = min(
        3.0 * (2.0 ** try_count) + random.uniform(0.5, 2.0),
        60.0
    )

    try:
        r = requests.get(
            with_token(url, token),
            timeout=90
        )
    except requests.RequestException as e:
        if try_count < MAX_TRIES:
            log.warning(
                "Request error, retry %s/%s after %.1fs: %s",
                try_count + 1,
                MAX_TRIES,
                backoff,
                e
            )
            time.sleep(backoff)
            return fb_get(url, token, try_count + 1)

        raise RuntimeError(f"REQUEST_ERROR: {e}")

    code = r.status_code

    if 200 <= code < 300:
        try:
            return r.json()
        except Exception:
            raise RuntimeError("Meta API trả về dữ liệu JSON không hợp lệ.")

    try:
        err_json = r.json()
    except Exception:
        err_json = {}

    err = err_json.get("error", {}) if isinstance(err_json, dict) else {}

    err_code = str(err.get("code", ""))
    err_subcode = str(err.get("error_subcode", ""))
    err_msg = str(err.get("message", ""))

    if DEBUG:
        short = url.split("?")[0]
        print(f"[FB_ERR] HTTP {code} @ {short}")
        print("[FB_ERR_BODY]", err_json or r.text[:500])

    # ==========================================================
    # META TEMPORARY / TRANSIENT ERRORS
    # ==========================================================
    temporary_error = (
        code == 429
        or code >= 500
        or err_code in {"2", "4", "17", "613"}
        or err_subcode == "1504044"
        or bool(err.get("is_transient"))
        or "temporarily unavailable" in err_msg.lower()
    )

    if temporary_error:
        if try_count < MAX_TRIES:
            log.warning(
                "Meta temporary error: HTTP=%s code=%s subcode=%s "
                "retry=%s/%s sleep=%.1fs",
                code,
                err_code,
                err_subcode,
                try_count + 1,
                MAX_TRIES,
                backoff
            )

            time.sleep(backoff)

            return fb_get(
                url,
                token,
                try_count + 1
            )

        raise RuntimeError(
            f"META_TEMPORARY_ERROR: "
            f"HTTP {code}, code={err_code}, "
            f"subcode={err_subcode}: {err_msg}"
        )

    # ==========================================================
    # CÁC LỖI KHÁC
    # ==========================================================
    raise RuntimeError(
        f"HTTP {code}: {r.text}"
    )


def fb_get_safely(url: str, token: str):
    try:
        return fb_get(url, token)

    except RuntimeError as e:
        msg = str(e)

        # Meta tạm thời lỗi -> báo cho caller xử lý
        if (
            "1504044" in msg
            or "META_TEMPORARY_ERROR" in msg
            or "Service temporarily unavailable" in msg
        ):
            raise RuntimeError("META_SKIP_ACCOUNT")

        raise

    raise RuntimeError(
        "Meta API vẫn không khả dụng sau nhiều lần retry/cooldown."
    )


def fb_paged(
    url: str,
    token: str
) -> List[dict]:

    rows = []
    page_count = 0

    while url:

        data = fb_get_safely(
            url,
            token
        )

        rows.extend(
            data.get("data", [])
            or []
        )

        url = (
            data.get("paging", {})
            .get("next")
        )

        page_count += 1

        if (
            page_count % PAGE_BURST
            == 0
        ):
            time.sleep(
                PAGE_BURST_SLEEP
            )

        if page_count > 10000:
            raise RuntimeError(
                "Paging overflow."
            )

    return rows


# =========================================================
# ACCOUNT
# =========================================================
def fetch_account_meta(
    act_id: str,
    token: str
) -> dict:

    url = (
        f"https://graph.facebook.com/"
        f"{FB_API_VERSION}/"
        f"{quote(act_id)}"
        "?fields=name,currency"
    )

    try:

        data = fb_get_safely(
            url,
            token
        )

        return {
            "name": data.get(
                "name",
                ""
            ),
            "currency": data.get(
                "currency",
                "VND"
            )
        }

    except Exception as error:

        if str(error) == RATE_LIMIT_ERR:
            raise

        return {
            "name": "",
            "currency": "VND"
        }


# =========================================================
# CAMPAIGN INSIGHTS
# =========================================================
def fetch_campaign_insights(
    act_id: str,
    since: str,
    until: str,
    token: str
) -> List[dict]:

    act = quote(act_id)

    base = (
        f"https://graph.facebook.com/"
        f"{FB_API_VERSION}/"
        f"{act}/insights"
    )

    # Chỉ Campaign Level
    base_fields = [
        "date_start",
        "account_id",
        "campaign_id",
        "campaign_name",
        "spend",
    ]

    action_fields = [
        "actions",
        "cost_per_action_type",
    ]

    def build_url(mode: str) -> str:

        fields = base_fields.copy()

        params = {
            "level": "campaign",
            "limit": "500",
            "time_range": json.dumps({
                "since": since,
                "until": until
            }),
            "time_increment": "1",
            "use_unified_attribution_setting": "true",
        }

        if REPORT_TIME in (
            "conversion",
            "impression"
        ):
            params[
                "action_report_time"
            ] = REPORT_TIME

        if mode in (
            "full",
            "plain"
        ):
            fields.extend(
                action_fields
            )

        params["fields"] = ",".join(
            fields
        )

        query = "&".join(
            f"{key}={quote(value)}"
            for key, value
            in params.items()
        )

        return f"{base}?{query}"

    modes = [
        "full",
        "plain",
        "basic"
    ]

    result = []

    for mode in modes:

        if mode == "basic":

            url = build_url(
                "plain"
            )

            url = url.replace(
                ",".join(action_fields),
                ""
            )

        else:
            url = build_url(
                mode
            )

        if DEBUG:
            print(
                f"[INSIGHTS] "
                f"try mode={mode}"
            )

        output = []
        page_count = 0

        while url:

            try:

                data = fb_get_safely(
                    url,
                    token
                )

            except RuntimeError as error:

                message = str(error)

                invalid_parameter = (
                    "Invalid parameter"
                    in message
                    or '"code":100'
                    in message
                    or "error_subcode\":1504018"
                    in message
                )

                if invalid_parameter:

                    if DEBUG:
                        print(
                            f"[INSIGHTS] "
                            f"mode={mode} "
                            "invalid -> fallback"
                        )

                    output = []
                    url = None
                    break

                raise

            output.extend(
                data.get("data", [])
                or []
            )

            url = (
                data.get("paging", {})
                .get("next")
            )

            page_count += 1

            if (
                page_count % PAGE_BURST
                == 0
            ):
                time.sleep(
                    PAGE_BURST_SLEEP
                )

            if page_count > 10000:
                raise RuntimeError(
                    "Paging overflow."
                )

        if output:

            result = output

            if DEBUG:
                print(
                    f"[INSIGHTS] "
                    f"success mode={mode}, "
                    f"rows={len(result)}"
                )

            break

    return result


# =========================================================
# ACTIONS
# =========================================================
LEAD_KEYS_PRIORITY = [
    "lead",
    "onsite_conversion.lead",
    "leadgen",
    "onsite_conversion.lead_grouped",
]


MSG_KEYS_PRIORITY = [
    "messaging_conversations_started",
    "messaging_conversation_started",
    "onsite_conversion.messaging_conversation_started_7d",
    "onsite_conversion.messaging_first_reply",
    "onsite_conversion.messaging_conversation_started_28d",
    "onsite_conversion.messaging_conversation_started_1d",
]


def _actions_map(
    row: dict
) -> dict:

    result = {}

    actions = row.get(
        "actions"
    )

    if not isinstance(
        actions,
        list
    ):
        return result

    for item in actions:

        action_type = str(
            item.get(
                "action_type",
                ""
            )
        ).lower()

        value = to_num(
            item.get("value")
        )

        if action_type not in result:
            result[
                action_type
            ] = value

    return result


def _pick_first(
    action_map: dict,
    keys: List[str]
) -> Tuple[float, str]:

    for key in keys:

        if key in action_map:
            return (
                action_map[key],
                key
            )

    for key in keys:

        for actual_key, value in (
            action_map.items()
        ):

            if key in actual_key:
                return (
                    value,
                    actual_key
                )

    return 0.0, ""


def extract_lead_count(
    row: dict
) -> Tuple[int, str]:

    action_map = _actions_map(
        row
    )

    value, key = _pick_first(
        action_map,
        LEAD_KEYS_PRIORITY
    )

    return (
        int(round(value)),
        key
    )


def extract_msg_started(
    row: dict
) -> int:

    action_map = _actions_map(
        row
    )

    value, _ = _pick_first(
        action_map,
        MSG_KEYS_PRIORITY
    )

    return int(
        round(value)
    )


# =========================================================
# MAP CAMPAIGN ROWS
# =========================================================
def map_rows(
    campaign_rows,
    account_name,
    rate
):

    rows = []

    for row in (
        campaign_rows or []
    ):

        spend_vnd = money0(
            to_num(
                row.get("spend")
            ) * rate
        )

        lead_count, _ = (
            extract_lead_count(row)
        )

        msg_started = (
            extract_msg_started(row)
        )

        rows.append([
            row.get(
                "date_start",
                ""
            ),
            row.get(
                "account_id",
                ""
            ),
            account_name or "",
            row.get(
                "campaign_id",
                ""
            ),
            row.get(
                "campaign_name",
                ""
            ),
            spend_vnd or "",
            msg_started or "",
            lead_count or "",
        ])

    return rows


# =========================================================
# GOOGLE SHEET
# =========================================================
def to_ymd_any(
    value: str
) -> str:

    text = (
        value or ""
    ).strip()

    if not text:
        return ""

    text = text.split(
        " "
    )[0]

    if "/" in text:

        parts = text.split("/")

        if len(parts) == 3:

            day, month, year = (
                parts[0],
                parts[1],
                parts[2]
            )

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
            f"https://docs.google.com/"
            f"spreadsheets/d/{sheet_id}"
            f"/export?format=csv"
            f"&gid={gid}"
        )

        if a1_range:
            url += (
                f"&range={a1_range}"
            )

        urls.append(url)

    base = (
        f"https://docs.google.com/"
        f"spreadsheets/d/{sheet_id}"
        f"/gviz/tq?tqx=out:csv"
    )

    if sheet_name:
        base += (
            f"&sheet={quote(sheet_name)}"
        )

    if a1_range:
        base += (
            f"&range={a1_range}"
        )

    urls.append(base)

    last_error = None

    for url in urls:

        try:

            response = requests.get(
                url,
                timeout=30
            )

            response.raise_for_status()

            rows = list(
                csv.reader(
                    io.StringIO(
                        response.text
                    )
                )
            )

            if rows:
                return rows

        except Exception as error:
            last_error = error

    if last_error:
        raise last_error

    return []


def _clamp_dates(
    since: str,
    until: str
):

    today = datetime.date.today()

    start = datetime.date.fromisoformat(
        since
    )

    end = datetime.date.fromisoformat(
        until
    )

    if end > today:
        end = today

    if start > end:
        start = end

    return (
        start.isoformat(),
        end.isoformat()
    )


# =========================================================
# CONFIG FROM GOOGLE SHEET
# =========================================================
def load_from_sheet_or_fail():

    sheet_id = os.environ.get(
        "SHEET_ID"
    )

    if not sheet_id:

        emit_error_csv(
            "Thiếu biến SHEET_ID."
        )

        raise SystemExit(1)

    sheet_name = os.environ.get(
        "API_SHEET_NAME",
        "api"
    )

    sheet_gid = os.environ.get(
        "API_SHEET_GID"
    )

    d_values = (
        _csv_rows_from_gsheet_csv(
            sheet_id,
            sheet_name=sheet_name,
            gid=sheet_gid,
            a1_range="D2:D4"
        )
    )

    values = [
        (
            row[0].strip()
            if row
            and len(row) >= 1
            else ""
        )
        for row in d_values
    ]

    since_raw = (
        values[0]
        if len(values) > 0
        else ""
    )

    until_raw = (
        values[1]
        if len(values) > 1
        else ""
    )

    accounts_text = (
        values[2]
        if len(values) > 2
        else ""
    )

    since = to_ymd_any(
        since_raw
    )

    until = to_ymd_any(
        until_raw
    )

    if (
        not since
        or not until
        or not accounts_text
    ):

        emit_error_csv(
            "Thiếu cấu hình api!D2, "
            "D3 hoặc D4."
        )

        raise SystemExit(1)

    try:
        datetime.date.fromisoformat(
            since
        )
    except Exception:

        emit_error_csv(
            "Sai định dạng since."
        )

        raise SystemExit(1)

    try:
        datetime.date.fromisoformat(
            until
        )
    except Exception:

        emit_error_csv(
            "Sai định dạng until."
        )

        raise SystemExit(1)

    since, until = _clamp_dates(
        since,
        until
    )

    accounts = [
        account.strip()
        for account in accounts_text.split(",")
        if account.strip()
    ]

    accounts = [
        account
        if account.startswith("act_")
        else f"act_{account}"
        for account in accounts
    ]

    fx_rows = (
        _csv_rows_from_gsheet_csv(
            sheet_id,
            sheet_name=sheet_name,
            gid=sheet_gid,
            a1_range="G2:H"
        )
    )

    fx = {}

    for row in fx_rows:

        if (
            len(row) >= 2
            and row[0]
            and row[1]
        ):

            try:

                currency = (
                    str(row[0])
                    .strip()
                    .upper()
                )

                rate = float(
                    str(row[1]).strip()
                )

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
        "fx": fx,
    }


# =========================================================
# CONFIG FROM YAML
# =========================================================
def load_from_config_file_or_fail():

    if not CONFIG_PATH.exists():

        emit_error_csv(
            "Thiếu SHEET_ID hoặc "
            "config/config.yml."
        )

        raise SystemExit(1)

    try:

        config = yaml.safe_load(
            CONFIG_PATH.read_text(
                encoding="utf-8"
            )
        ) or {}

    except Exception as error:

        emit_error_csv(
            f"Lỗi đọc config.yml: {error}"
        )

        raise SystemExit(1)

    missing = []

    if not config.get("since"):
        missing.append("since")

    if not config.get("until"):
        missing.append("until")

    if not config.get("accounts"):
        missing.append("accounts")

    if missing:

        emit_error_csv(
            "Thiếu cấu hình: "
            + ", ".join(missing)
        )

        raise SystemExit(1)

    since = to_ymd_any(
        config["since"]
    )

    until = to_ymd_any(
        config["until"]
    )

    since, until = _clamp_dates(
        since,
        until
    )

    accounts = [
        str(account).strip()
        for account in config["accounts"]
        if str(account).strip()
    ]

    accounts = [
        account
        if account.startswith("act_")
        else f"act_{account}"
        for account in accounts
    ]

    fx = {
        "VND": 1.0
    }

    for currency, value in (
        config.get("fx") or {}
    ).items():

        try:

            rate = float(value)

            if rate > 0:
                fx[
                    str(currency).upper()
                ] = rate

        except Exception:
            pass

    _apply_env_overrides()

    return {
        "since": since,
        "until": until,
        "accounts": accounts,
        "fx": fx,
    }


def load_config_or_fail():

    if os.environ.get(
        "SHEET_ID"
    ):
        return load_from_sheet_or_fail()

    return load_from_config_file_or_fail()


# =========================================================
# CSV
# =========================================================
def write_full_csv(
    rows: List[List]
):

    CSV_PATH.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    with open(
        CSV_PATH,
        "w",
        newline="",
        encoding="utf-8"
    ) as file:

        writer = csv.writer(file)

        writer.writerow(
            HEADERS_VN
        )

        if rows:
            writer.writerows(
                rows
            )


# =========================================================
# MAIN
# =========================================================
def run_once():

    config = load_config_or_fail()

    token = os.environ.get(
        "META_TOKEN"
    )

    if not token:

        emit_error_csv(
            "Thiếu META_TOKEN."
        )

        raise SystemExit(1)

    all_rows = []

    for idx, act in enumerate(cfg["accounts"]):

    if idx > 0:
        time.sleep(ACCT_COOLDOWN)

    log.info("Running Campaign Level: %s", act)

    try:
        meta = fetch_account_meta(act, token)

        cur = (meta.get("currency") or "VND").upper()

        rate = (
            1.0
            if cur == "VND"
            else float(cfg["fx"].get(cur, 0))
        )

        if cur != "VND" and rate <= 0:
            log.error(
                "Missing FX rate for %s - skip %s",
                cur,
                act
            )
            continue

        ads = fetch_campaign_insights(
            act,
            cfg["since"],
            cfg["until"],
            token
        )

        rows = map_rows(
            ads,
            meta.get("name", ""),
            rate
        )

        all_rows.extend(rows)

        log.info(
            "Campaign Level done: %s | rows=%s",
            act,
            len(rows)
        )

    except RuntimeError as e:

        if str(e) == "META_SKIP_ACCOUNT":
            log.error(
                "Meta unavailable for %s. "
                "Skip account and continue.",
                act
            )
            continue

        log.error(
            "Account %s failed: %s",
            act,
            e
        )
        continue

    except Exception as e:

        log.exception(
            "Unexpected error on account %s: %s",
            act,
            e
        )
        continue
    write_full_csv(
        all_rows
    )

    print(
        json.dumps(
            {
                "status": "done",
                "level": "campaign",
                "rows": len(all_rows)
            },
            ensure_ascii=False
        )
    )


# =========================================================
# START
# =========================================================
if __name__ == "__main__":

    try:

        run_once()

    except SystemExit:

        raise

    except Exception as error:

        emit_error_csv(
            f"Lỗi không xác định: {error}"
        )

        raise
