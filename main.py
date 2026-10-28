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
            # =========================
            # ACCOUNT INFO
            # =========================
            meta = fetch_account_meta(act, token)

            cur = (meta.get("currency") or "VND").upper()

            if cur == "VND":
                rate = 1.0
            else:
                rate = float(cfg["fx"].get(cur, 0))

            if cur != "VND" and rate <= 0:
                log.error(
                    "Thiếu tỷ giá cho %s - bỏ qua %s",
                    cur,
                    act
                )
                skipped_accounts.append(act)
                continue

            # =========================
            # CAMPAIGN LEVEL INSIGHTS
            # =========================
            ads = fetch_campaign_insights(
                act,
                cfg["since"],
                cfg["until"],
                token
            )

            # =========================
            # MAP DATA
            # =========================
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
                    "Meta unavailable for %s - skip account",
                    act
                )

                skipped_accounts.append(act)
                continue

            log.error(
                "Account %s failed: %s",
                act,
                e
            )

            skipped_accounts.append(act)
            continue

        except Exception as e:

            log.exception(
                "Unexpected error on account %s: %s",
                act,
                e
            )

            skipped_accounts.append(act)
            continue

    # =========================
    # WRITE CSV
    # =========================
    write_full_csv(all_rows)

    log.info(
        "Finished. Rows=%s | Skipped accounts=%s",
        len(all_rows),
        len(skipped_accounts)
    )

    if skipped_accounts:
        log.warning(
            "Skipped: %s",
            ", ".join(skipped_accounts)
        )

    print(
        json.dumps(
            {
                "status": "done",
                "rows": len(all_rows),
                "skipped_accounts": len(skipped_accounts)
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
