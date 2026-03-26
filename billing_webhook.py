"""
Pet Releaf — Billing Failure Webhook Server
Listens for Smartrr "Subscription failed payment" events, stores failures,
and places an automated outbound call via Plivo (once daily by default).

How the call works:
  1. Daily worker (or webhook, if enabled) schedules outbound calls
  2. Plivo calls the customer's phone
  3. If a human answers  → plays the message
  4. If voicemail/AMD detected → waits for beep, then plays the message
  5. Call ends cleanly

Requirements:
    pip install flask plivo python-dotenv

Environment variables (copy .env.example → .env and fill in values):
    PLIVO_AUTH_ID        - From Plivo console overview page
    PLIVO_AUTH_TOKEN     - From Plivo console overview page
    PLIVO_FROM_NUMBER    - Your Plivo number in E.164 format e.g. +17205551234
    ANSWER_URL           - Public URL of THIS server + /answer
                           e.g. https://your-app.railway.app/answer
    PAYMENT_UPDATE_URL   - Page where customers update payment info
    WEBHOOK_SECRET       - Optional: secret to verify Smartrr requests

Deploy to Railway/Render, then paste your public URL + /billing-failure
into Smartrr → Integrations → Webhooks → Subscription failed payment.
"""

import os
import logging
import plivo  # type: ignore
import sqlite3
import time
from datetime import datetime, timezone

from flask import Flask, request, jsonify, Response
from plivo import plivoxml  # type: ignore
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

PLIVO_AUTH_ID      = os.getenv("PLIVO_AUTH_ID")
PLIVO_AUTH_TOKEN   = os.getenv("PLIVO_AUTH_TOKEN")
PLIVO_FROM_NUMBER  = os.getenv("PLIVO_FROM_NUMBER")
ANSWER_URL         = os.getenv("ANSWER_URL")
PAYMENT_UPDATE_URL = os.getenv("PAYMENT_UPDATE_URL", "https://www.petreleaf.com/account")
WEBHOOK_SECRET     = os.getenv("WEBHOOK_SECRET")

DB_PATH = os.getenv("DB_PATH", "billing_failures.db")

# Webhook behavior:
# - If CALL_ON_WEBHOOK=true, the server places the call immediately.
# - If false (default), it only stores failures and a daily job will call later.
CALL_ON_WEBHOOK = os.getenv("CALL_ON_WEBHOOK", "false").lower() == "true"

# Daily batching behavior:
# - "daily worker" runs once on startup (if enabled) and then every N seconds.
SCHEDULER_ENABLED = os.getenv("SCHEDULER_ENABLED", "false").lower() == "true"
DAILY_CALL_INTERVAL_SECONDS = int(os.getenv("DAILY_CALL_INTERVAL_SECONDS", "86400"))
DAILY_CALL_MAX_BATCH = int(os.getenv("DAILY_CALL_MAX_BATCH", "200"))
MAX_FAILURE_AGE_DAYS = int(os.getenv("MAX_FAILURE_AGE_DAYS", "30"))

# Optional shared secret for manual triggering of the daily job.
DAILY_JOB_SECRET = os.getenv("DAILY_JOB_SECRET") or WEBHOOK_SECRET

client = plivo.RestClient(PLIVO_AUTH_ID, PLIVO_AUTH_TOKEN)

VOICEMAIL_MESSAGE = (
    "Hi, this is a message from Pet Releaf. "
    "We noticed an issue processing your recent subscription payment. "
    "Please visit petreleaf dot com and update your payment information, "
    "or call us back and we will be happy to help. "
    "Thank you and have a wonderful day!"
)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _utc_today_yyyy_mm_dd() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _utc_cutoff_iso_days_ago(days: int) -> str:
    cutoff = datetime.now(timezone.utc).timestamp() - (days * 86400)
    return datetime.fromtimestamp(cutoff, tz=timezone.utc).isoformat()


def _db_connect():
    # NOTE: check_same_thread=False lets us use this from the Flask request thread
    # and the background scheduler thread.
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def _db_column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return any(r["name"] == column for r in rows)


def _db_init():
    conn = _db_connect()
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS failed_payments (
                phone TEXT PRIMARY KEY,
                name TEXT,
                email TEXT,
                first_failure_at TEXT,
                last_failure_at TEXT,
                last_called_date TEXT,
                last_request_uuid TEXT,
                resolved_at TEXT
            )
            """
        )
        # Migration for older DBs (older versions created without resolved_at).
        if not _db_column_exists(conn, "failed_payments", "resolved_at"):
            conn.execute("ALTER TABLE failed_payments ADD COLUMN resolved_at TEXT")

        conn.commit()
    finally:
        conn.close()


def upsert_failure(phone: str, name: str, email: str):
    now_iso = _utc_now_iso()
    today = _utc_today_yyyy_mm_dd()

    conn = _db_connect()
    try:
        # phone is the unique key; we update the timestamps on repeated failures.
        conn.execute(
            """
            INSERT INTO failed_payments (
                phone, name, email, first_failure_at, last_failure_at, last_called_date, last_request_uuid, resolved_at
            ) VALUES (?, ?, ?, ?, ?, NULL, NULL, NULL)
            ON CONFLICT(phone) DO UPDATE SET
                name=excluded.name,
                email=excluded.email,
                last_failure_at=excluded.last_failure_at
                ,
                resolved_at=NULL,
                last_called_date=CASE
                    WHEN failed_payments.resolved_at IS NOT NULL THEN NULL
                    ELSE failed_payments.last_called_date
                END,
                last_request_uuid=CASE
                    WHEN failed_payments.resolved_at IS NOT NULL THEN NULL
                    ELSE failed_payments.last_request_uuid
                END
            """,
            (phone, name, email, now_iso, now_iso),
        )
        conn.commit()
    finally:
        conn.close()


def get_due_failures(today_yyyy_mm_dd: str, cutoff_iso: str, limit: int):
    conn = _db_connect()
    try:
        rows = conn.execute(
            """
            SELECT phone, name, email
            FROM failed_payments
            WHERE resolved_at IS NULL
              AND (last_called_date IS NULL OR last_called_date != ?)
              AND last_failure_at >= ?
            ORDER BY last_failure_at DESC
            LIMIT ?
            """,
            (today_yyyy_mm_dd, cutoff_iso, limit),
        ).fetchall()
        return rows
    finally:
        conn.close()


def mark_called(phone: str, request_uuid: str):
    conn = _db_connect()
    try:
        conn.execute(
            """
            UPDATE failed_payments
            SET last_called_date = ?, last_request_uuid = ?
            WHERE phone = ?
            """,
            (_utc_today_yyyy_mm_dd(), request_uuid, phone),
        )
        conn.commit()
    finally:
        conn.close()


def mark_resolved(phone: str):
    now_iso = _utc_now_iso()
    conn = _db_connect()
    try:
        conn.execute(
            """
            INSERT INTO failed_payments (
                phone, name, email, first_failure_at, last_failure_at,
                last_called_date, last_request_uuid, resolved_at
            ) VALUES (?, NULL, NULL, NULL, NULL, NULL, NULL, ?)
            ON CONFLICT(phone) DO UPDATE SET
                resolved_at=excluded.resolved_at
            """,
            (phone, now_iso),
        )
        conn.commit()
    finally:
        conn.close()


def extract_phone_from_payload(payload: dict) -> str | None:
    customer = payload.get("customer") or payload.get("subscription", {}).get("customer", {}) or {}
    phone = (
        customer.get("phone")
        or customer.get("phoneNumber")
        or customer.get("phone_number")
        or payload.get("phone")
        or payload.get("phone_number")
    )
    if not phone:
        return None

    # Normalise to E.164 if needed (+1XXXXXXXXXX)
    if isinstance(phone, str) and phone.startswith("+"):
        return phone

    if isinstance(phone, str):
        digits = "".join(c for c in phone if c.isdigit())
        if digits:
            return "+1" + digits
    return None


def extract_name_email_from_payload(payload: dict) -> tuple[str, str]:
    customer = payload.get("customer") or payload.get("subscription", {}).get("customer", {}) or {}
    name = (
        customer.get("name")
        or f"{customer.get('firstName', '')} {customer.get('lastName', '')}".strip()
        or "Valued Customer"
    )
    email = customer.get("email", "unknown")
    return name, email


def normalize_extract_customer_info(payload: dict) -> tuple[str | None, str, str]:
    phone = extract_phone_from_payload(payload)
    name, email = extract_name_email_from_payload(payload)
    return phone, name, email


def place_voicemail_call(phone: str):
    # Plivo fetches this XML the moment the call is answered — human or voicemail.
    # (Same voicemail message for every customer.)
    response = client.calls.create(
        from_=PLIVO_FROM_NUMBER,
        to_=phone,
        answer_url=ANSWER_URL,
        answer_method="GET",
        machine_detection="true",  # wait for beep, then play message
    )
    request_uuid = response[1].get("request_uuid", "unknown")
    return request_uuid

# ── Answer URL ────────────────────────────────────────────────────────────────
# Plivo fetches this XML the moment the call is answered — human or voicemail.
# We use Amazon Polly via Plivo's <Speak> for a natural-sounding voice.
# To use a pre-recorded audio file instead, swap <Speak> for:
#   <Play>https://your-server.com/static/billing_message.mp3</Play>

@app.route("/answer", methods=["GET", "POST"])
def answer():
    response = plivoxml.ResponseElement()
    response.add(
        plivoxml.SpeakElement(
            VOICEMAIL_MESSAGE,
            voice="Polly.Joanna",  # Natural US female voice via Amazon Polly
            language="en-US",
        )
    )

    return Response(response.to_string(), mimetype="application/xml")


# ── Billing Failure Webhook ───────────────────────────────────────────────────

@app.route("/billing-failure", methods=["POST"])
def billing_failure():
    """
    Receives Smartrr 'Subscription failed payment' webhook and triggers a call.
    Paste this endpoint's full public URL into:
    Smartrr → Integrations → Webhooks → Subscription failed payment
    """

    # Verify the request is genuinely from Smartrr (optional but recommended)
    if WEBHOOK_SECRET:
        incoming = request.headers.get("X-Smartrr-Secret") or request.args.get("secret")
        if incoming != WEBHOOK_SECRET:
            logger.warning("Invalid webhook secret — rejected")
            return jsonify({"error": "Unauthorized"}), 401

    payload = request.get_json(silent=True)
    if not payload:
        logger.warning("Empty or non-JSON payload")
        return jsonify({"error": "Invalid payload"}), 400

    logger.info(f"Billing failure received: {payload}")

    # ── Parse customer info ───────────────────────────────────────────────────
    # Field names vary slightly between Smartrr integrations; we keep this flexible.
    phone, name, email = normalize_extract_customer_info(payload)

    logger.info(f"Customer: {name} | phone: {phone} | email: {email}")

    if not phone:
        logger.info(f"No phone number on file for {name} ({email}) — skipping call")
        return jsonify({"status": "no_phone", "email": email}), 200

    # Store the failure so the daily job can call it once per day.
    upsert_failure(phone=phone, name=name, email=email)

    if not CALL_ON_WEBHOOK:
        return jsonify({"status": "queued_for_daily", "to": phone}), 200

    try:
        request_uuid = place_voicemail_call(phone)
        logger.info(f"Call initiated → {phone} | request_uuid: {request_uuid}")
        mark_called(phone=phone, request_uuid=request_uuid)
        return jsonify(
            {"status": "call_initiated", "to": phone, "request_uuid": request_uuid}
        ), 200

    except Exception as e:
        logger.error(f"Plivo call failed → {phone}: {str(e)}")
        return jsonify({"status": "call_failed", "error": str(e)}), 500


# ── Health Check ──────────────────────────────────────────────────────────────

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200


@app.route("/billing-success", methods=["POST"])
def billing_success():
    """
    Marks a previously failed subscription payment as resolved.
    Configure this endpoint in Smartrr for the "payment succeeded" /
    "subscription active again" webhook event.
    """
    if WEBHOOK_SECRET:
        incoming = request.headers.get("X-Smartrr-Secret") or request.args.get("secret")
        if incoming != WEBHOOK_SECRET:
            logger.warning("Invalid webhook secret — rejected")
            return jsonify({"error": "Unauthorized"}), 401

    payload = request.get_json(silent=True)
    if not payload:
        return jsonify({"error": "Invalid payload"}), 400

    phone, name, email = normalize_extract_customer_info(payload)
    if not phone:
        return jsonify({"status": "no_phone"}), 200

    logger.info(f"Billing success received: {name} | phone: {phone} | email: {email}")
    mark_resolved(phone=phone)
    return jsonify({"status": "resolved", "to": phone}), 200


@app.route("/run-daily-calls", methods=["POST"])
def run_daily_calls():
    """
    Manual trigger for the daily call worker.
    Useful if you don't want to keep a background scheduler enabled.
    """
    if DAILY_JOB_SECRET:
        incoming = request.headers.get("X-Smartrr-Secret") or request.args.get("secret")
        if incoming != DAILY_JOB_SECRET:
            return jsonify({"error": "Unauthorized"}), 401

    today = _utc_today_yyyy_mm_dd()
    results = run_due_calls_once(today=today)
    return jsonify(results), 200


def run_due_calls_once(today: str):
    cutoff_iso = _utc_cutoff_iso_days_ago(MAX_FAILURE_AGE_DAYS)
    due_rows = get_due_failures(
        today_yyyy_mm_dd=today,
        cutoff_iso=cutoff_iso,
        limit=DAILY_CALL_MAX_BATCH,
    )
    if not due_rows:
        return {"status": "no_due_calls", "today": today, "count": 0}

    ok = 0
    failed = 0
    last_error = None

    for row in due_rows:
        phone = row["phone"]
        try:
            request_uuid = place_voicemail_call(phone)
            mark_called(phone=phone, request_uuid=request_uuid)
            ok += 1
        except Exception as e:
            failed += 1
            last_error = str(e)
            logger.error(f"Daily call failed for {phone}: {last_error}")

    return {
        "status": "daily_call_run_complete",
        "today": today,
        "count_due": len(due_rows),
        "called_successfully": ok,
        "called_failed": failed,
        "last_error": last_error,
    }


def daily_worker_loop():
    _db_init()
    # Run immediately on startup.
    run_due_calls_once(today=_utc_today_yyyy_mm_dd())

    while True:
        time.sleep(DAILY_CALL_INTERVAL_SECONDS)
        try:
            run_due_calls_once(today=_utc_today_yyyy_mm_dd())
        except Exception as e:
            logger.exception(f"Daily worker loop error: {str(e)}")


# ── Run ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    _db_init()

    if SCHEDULER_ENABLED:
        logger.info(
            f"Starting daily worker thread (interval_seconds={DAILY_CALL_INTERVAL_SECONDS})"
        )
        import threading

        t = threading.Thread(target=daily_worker_loop, daemon=True)
        t.start()

    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
