"""
Pet Releaf — Billing Failure Webhook Server
Listens for Smartrr "Subscription failed payment" events, stores failures,
and places an automated outbound call via Twilio.

How the call works:
  1. On billing failure, optionally call immediately (if due) or wait for the scheduler
  2. Twilio calls the customer's phone
  3. Voicemail-style: machine answers get the message; humans/fax/unknown hang up
  4. Repeats on a schedule (default: every 30 days) until payment succeeds or cancel

Requirements:
    pip install flask twilio python-dotenv

Environment variables (copy .env.example → .env and fill in values):
    TWILIO_ACCOUNT_SID   - From Twilio console
    TWILIO_AUTH_TOKEN    - From Twilio console
    TWILIO_FROM_NUMBER   - Your Twilio number in E.164 format e.g. +17205551234
    ANSWER_URL           - Public URL of THIS server + /answer (TwiML webhook)
                           e.g. https://your-app.railway.app/answer
    PAYMENT_UPDATE_URL   - Page where customers update payment info
    WEBHOOK_SECRET       - Optional: secret to verify Smartrr requests

Deploy to Railway/Render, then paste your public URL + /billing-failure
into Smartrr → Integrations → Webhooks → Subscription failed payment.
"""

import os
import logging
import sqlite3
import time
from datetime import datetime, timezone

from flask import Flask, request, jsonify, Response
from dotenv import load_dotenv
from twilio.rest import Client
from twilio.twiml.voice_response import VoiceResponse

load_dotenv()

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _env_int(name: str, default: int) -> int:
    """Avoid startup crash if Railway has an empty or invalid numeric env var."""
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return default
    try:
        return int(raw)
    except ValueError:
        logger.warning(f"Invalid integer for {name}={raw!r} — using default {default}")
        return default


# ── Config ────────────────────────────────────────────────────────────────────

TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID")
TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN")
TWILIO_FROM_NUMBER = os.getenv("TWILIO_FROM_NUMBER")
ANSWER_URL = os.getenv("ANSWER_URL")
PAYMENT_UPDATE_URL = os.getenv("PAYMENT_UPDATE_URL", "https://www.petreleaf.com/account")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET")

DB_PATH = os.getenv("DB_PATH", "billing_failures.db")

# Webhook behavior:
# - If CALL_ON_WEBHOOK=true, the server may place a call when Smartrr POSTs
#   (only if no reminder was sent yet, or the last one was REMINDER_INTERVAL_DAYS ago).
# - If false (default), it only stores failures; the scheduler or /run-daily-calls places calls.
CALL_ON_WEBHOOK = os.getenv("CALL_ON_WEBHOOK", "false").lower() == "true"

# Minimum days between voicemail reminders for the same phone (while still failed).
REMINDER_INTERVAL_DAYS = _env_int("REMINDER_INTERVAL_DAYS", 30)

# Scheduler: how often to scan for due reminders (seconds). 86400 = once per day.
SCHEDULER_ENABLED = os.getenv("SCHEDULER_ENABLED", "false").lower() == "true"
DAILY_CALL_INTERVAL_SECONDS = _env_int("DAILY_CALL_INTERVAL_SECONDS", 86400)
DAILY_CALL_MAX_BATCH = _env_int("DAILY_CALL_MAX_BATCH", 200)
# Drop failures older than this from reminders (set high if you only use monthly calls).
MAX_FAILURE_AGE_DAYS = _env_int("MAX_FAILURE_AGE_DAYS", 365)

# Optional shared secret for manual triggering of the daily job.
DAILY_JOB_SECRET = os.getenv("DAILY_JOB_SECRET") or WEBHOOK_SECRET

_twilio_client: Client | None = None


def get_twilio_client() -> Client:
    """Create client on first use so bad/missing env does not break app import (e.g. /health)."""
    global _twilio_client
    if _twilio_client is None:
        _twilio_client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
    return _twilio_client

_DEFAULT_VOICEMAIL_MESSAGE = (
    "Hi, this is a message from Pet Releaf. "
    "We noticed an issue processing your recent subscription payment. "
    "Please visit petreleaf dot com and update your payment information, "
    "or call us back and we will be happy to help. "
    "Thank you and have a wonderful day!"
)
# Optional: set VOICEMAIL_MESSAGE in Railway to change text without redeploying code.
VOICEMAIL_MESSAGE = os.getenv("VOICEMAIL_MESSAGE") or _DEFAULT_VOICEMAIL_MESSAGE


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _utc_today_yyyy_mm_dd() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _utc_cutoff_iso_days_ago(days: int) -> str:
    cutoff = datetime.now(timezone.utc).timestamp() - (days * 86400)
    return datetime.fromtimestamp(cutoff, tz=timezone.utc).isoformat()


def _days_since_last_reminder(last_called_date: str | None) -> float | None:
    """last_called_date is stored as YYYY-MM-DD (UTC day of last outbound reminder)."""
    if not last_called_date:
        return None
    try:
        day = last_called_date.strip()[:10]
        last = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        return (now - last).total_seconds() / 86400.0
    except ValueError:
        return None


def is_due_for_reminder_call(last_called_date: str | None) -> bool:
    """True if we have never reminded, or the last reminder was long enough ago."""
    if last_called_date is None:
        return True
    days = _days_since_last_reminder(last_called_date)
    if days is None:
        return True
    return days >= REMINDER_INTERVAL_DAYS


def get_last_called_date(phone: str) -> str | None:
    conn = _db_connect()
    try:
        row = conn.execute(
            "SELECT last_called_date FROM failed_payments WHERE phone = ?",
            (phone,),
        ).fetchone()
        return row["last_called_date"] if row else None
    finally:
        conn.close()


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


def get_due_failures(cutoff_iso: str, limit: int):
    conn = _db_connect()
    try:
        rows = conn.execute(
            """
            SELECT phone, name, email
            FROM failed_payments
            WHERE resolved_at IS NULL
              AND last_failure_at >= ?
              AND (
                last_called_date IS NULL
                OR (julianday('now') - julianday(last_called_date)) >= ?
              )
            ORDER BY last_failure_at DESC
            LIMIT ?
            """,
            (cutoff_iso, REMINDER_INTERVAL_DAYS, limit),
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
    # Twilio fetches this TwiML when the call is answered (human or voicemail).
    call = get_twilio_client().calls.create(
        to_=phone,
        from_=TWILIO_FROM_NUMBER,
        url=ANSWER_URL,
        method="GET",
        # Ask Twilio to classify what answered so we can avoid calling humans.
        machine_detection="Enable",
    )
    return call.sid

# ── Answer URL ────────────────────────────────────────────────────────────────
# Twilio fetches this TwiML the moment the call is answered (human or voicemail).

@app.route("/answer", methods=["GET", "POST"])
def answer():
    # Twilio passes `AnsweredBy` when machine detection is enabled.
    # Common values: human, machine_start, machine_end, fax, unknown.
    answered_by = (request.values.get("AnsweredBy") or "").strip().lower()

    resp = VoiceResponse()

    # Keep it "voicemail-only" as much as possible.
    # If Twilio thinks a human (or fax/unknown) answered, hang up.
    if answered_by in {"human", "fax", "unknown"}:
        resp.hangup()
        return Response(resp.to_xml(), mimetype="text/xml")

    # Otherwise, play the voicemail reminder message.
    resp.say(
        VOICEMAIL_MESSAGE,
        voice="Polly.Joanna",
        language="en-US",
    )
    return Response(resp.to_xml(), mimetype="text/xml")


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

    # Store the failure; reminders run on an interval until resolved or cancel.
    upsert_failure(phone=phone, name=name, email=email)
    last_called = get_last_called_date(phone)

    if not CALL_ON_WEBHOOK:
        return jsonify({"status": "queued_for_reminder", "to": phone}), 200

    if not is_due_for_reminder_call(last_called):
        return jsonify(
            {
                "status": "skipped_not_due_yet",
                "to": phone,
                "last_reminder_day": last_called,
                "reminder_interval_days": REMINDER_INTERVAL_DAYS,
            }
        ), 200

    try:
        request_uuid = place_voicemail_call(phone)
        logger.info(f"Call initiated → {phone} | request_uuid: {request_uuid}")
        mark_called(phone=phone, request_uuid=request_uuid)
        return jsonify(
            {"status": "call_initiated", "to": phone, "request_uuid": request_uuid}
        ), 200

    except Exception as e:
        logger.error(f"Twilio call failed → {phone}: {str(e)}")
        return jsonify({"status": "call_failed", "error": str(e)}), 500


# ── Health Check ──────────────────────────────────────────────────────────────

@app.route("/", methods=["GET", "HEAD"])
def root():
    # Plain text + HEAD: avoids any JSON edge cases with host health probes.
    return Response("ok\n", status=200, mimetype="text/plain")


@app.route("/health", methods=["GET", "HEAD"])
def health():
    return Response("ok\n", status=200, mimetype="text/plain")


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


@app.route("/subscription-cancel", methods=["POST"])
def subscription_cancel():
    """
    Stops reminders when a subscription is cancelled (same effect as payment success).
    Add in Smartrr → Webhooks → Subscription cancel → this URL (same ?secret= as other webhooks).
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

    logger.info(f"Subscription cancel received: {name} | phone: {phone} | email: {email}")
    mark_resolved(phone=phone)
    return jsonify({"status": "cancelled_stopped_reminders", "to": phone}), 200


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

def _maybe_start_scheduler_thread():
    if not SCHEDULER_ENABLED:
        return
    logger.info(
        f"Starting daily worker thread (interval_seconds={DAILY_CALL_INTERVAL_SECONDS})"
    )
    import threading

    t = threading.Thread(target=daily_worker_loop, daemon=True)
    t.start()


# DB schema needed for `gunicorn billing_webhook:app` (no __main__ block runs).
# Do not let a bad path/permissions take down the whole app (Railway health checks need / and /health).
try:
    _db_init()
except Exception:
    logger.exception("Database init failed at import — fix DB_PATH or volume permissions")

# Gunicorn: do not use Flask's before_serving (it is not reliable with production WSGI servers).
_maybe_start_scheduler_thread()
logger.info("billing_webhook loaded (gunicorn worker ready for requests)")

if __name__ == "__main__":
    port = _env_int("PORT", 5000)
    app.run(host="0.0.0.0", port=port)
