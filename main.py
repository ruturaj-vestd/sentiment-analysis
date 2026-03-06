import os
import json
import time
import html as _html
import re
import traceback
import urllib.request
import urllib.error
import urllib.parse
from datetime import datetime, timezone

import boto3
from botocore.config import Config

# ---------------------- ENV ----------------------
HUBSPOT_TOKEN = os.getenv("HUBSPOT_TOKEN")
HUBSPOT_BASE = os.getenv("HUBSPOT_BASE", "https://api.hubapi.com")

SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
SLACK_CHANNEL_ID = os.getenv("SLACK_CHANNEL_ID")

BEDROCK_REGION = os.getenv("BEDROCK_REGION", "eu-north-1")
CLAUDE_MODEL_ID = os.getenv("CLAUDE_MODEL_ID")

DDB_TABLE = os.getenv("DDB_TABLE")
IDEMP_TTL_SECONDS = int(os.getenv("IDEMP_TTL_SECONDS", "172800"))  # 2 days

# REQUIRED: Support pipeline id and its "New" stage id
SUPPORT_PIPELINE_ID = os.getenv("SUPPORT_PIPELINE_ID")
SUPPORT_NEW_STAGE_ID = os.getenv("SUPPORT_NEW_STAGE_ID")

# NEW: Pipeline B + its Slack channel
PIPELINE_B_ID = os.getenv("PIPELINE_B_ID")                  # REQUIRED for pipeline B
PIPELINE_B_NEW_STAGE_ID = os.getenv("PIPELINE_B_NEW_STAGE_ID")  # REQUIRED for pipeline B
SLACK_CHANNEL_ID_B = os.getenv("SLACK_CHANNEL_ID_B")

#Hubspot Ticket Links
HUBSPOT_PORTAL_BASE = os.getenv("HUBSPOT_PORTAL_BASE", "https://app.hubspot.com")
HUBSPOT_PORTAL_ID = os.getenv("HUBSPOT_PORTAL_ID")  # e.g. "12345678" (REQUIRED for ticket link)

# ---------------------- CONSTANTS ----------------------
EMAIL_LIMIT = 20
MAX_EMAIL_BODY_CHARS = 1200
DEFAULT_HTTP_TIMEOUT = 15
MAX_SPAM_SCAN_CHARS = 2500

TS_FIELDS = [
    "hs_email_received_date",
    "hs_email_sent_at",
    "hs_timestamp",
    "createdate",
    "hs_createdate",
]
BODY_FIELDS = [
    "hs_email_subject",
    "hs_email_text",
    "hs_email_html",
]

TAG_RE = re.compile(r"<[^>]+>")
SPAM_SIGNAL_RE = re.compile(
    r"(\bout\s*of\s*office\b|\bautomatic\s*reply\b|\bauto\s*reply\b|\booo\b|"
    r"\bnewsletter\b|\bunsubscribe\b|\bmarketing\b|\bpromot(?:ion|ional)\b|"
    r"\badvertis(?:e|ing|ement)\b|\bcold\s*outreach\b|\blead\s*generation\b|"
    r"\bseo\s+services\b|\bppc\b|\bweb\s*design\s*services\b|"
    r"\bwe\s+offer\s+services\b|\boffering\s+services\b|\bbook\s+a\s+demo\b)",
    re.IGNORECASE,
)
AUTO_REPLY_SUBJECT_RE = re.compile(
    r"(^|\W)(automatic\s*reply|auto\s*reply|out\s*of\s*office|ooo|away\s*from\s*the\s*office|on\s*leave)(\W|$)",
    re.IGNORECASE,
)
AUTO_REPLY_BODY_RE = re.compile(
    r"(this\s+is\s+an\s+automatic\s+reply|i\s+(?:am|\'m)\s+currently\s+out\s+of\s+office|"
    r"i\s+will\s+be\s+out\s+of\s+office|i\s+am\s+away\s+from\s+the\s+office|"
    r"thank\s+you\s+for\s+your\s+email\.\s*i\s+am\s+currently\s+out)",
    re.IGNORECASE,
)

TICKET_PROPS = [
    "hs_pipeline",
    "hs_pipeline_stage",
    "subject",
    "content",
    "createdate",
    "hubspot_owner_id",
    "hs_ticket_owner",
]

CONTACT_PROPS = [
    "email",
    "firstname",
    "lastname",
]

# ---------------------- AWS CLIENTS ----------------------
ddb = boto3.client("dynamodb")

session = boto3.session.Session(region_name=BEDROCK_REGION)
bedrock = session.client(
    "bedrock-runtime",
    region_name=BEDROCK_REGION,
    endpoint_url=f"https://bedrock-runtime.{BEDROCK_REGION}.amazonaws.com",
    config=Config(retries={"max_attempts": 3}),
)

# ---------------------- CACHES (warm container) ----------------------
COMPANY_NAME_CACHE = {}
COMPANY_OWNER_CACHE = {}  # company_id -> owner_id or None
OWNER_NAME_CACHE = {}  # owner_id -> owner_name or None
CONTACT_CACHE = {}  # contact_id -> dict(props) or None
CONTACT_COMPANY_CACHE = {}  # contact_id -> company_id or None


# ---------------------- LOGGING ----------------------
def log(level: str, message: str, **fields):
    """
    Structured-ish CloudWatch logs.
    Example:
      {"level":"INFO","message":"Processing ticket","ticket_id":"123","msg_id":"abc"}
    """
    payload = {"level": level, "message": message}
    if fields:
        payload.update(fields)
    print(json.dumps(payload, ensure_ascii=False))


# ---------------------- HTTP ----------------------
def _http(method, url, headers, data=None, timeout=DEFAULT_HTTP_TIMEOUT):
    req = urllib.request.Request(url, method=method, headers=headers, data=data)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.getcode(), resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        try:
            body = e.read().decode("utf-8", errors="replace")
        except Exception:
            body = str(e)
        return e.code, body
    except Exception as e:
        return 599, str(e)


def truncate(text: str, limit: int) -> str:
    if text is None:
        return ""
    return text if len(text) <= limit else text[: max(0, limit - 14)] + " …[truncated]"


def hubspot_auth_headers():
    if not HUBSPOT_TOKEN:
        raise RuntimeError("Missing HUBSPOT_TOKEN env var")
    return {"Authorization": f"Bearer {HUBSPOT_TOKEN}"}


def hubspot_headers_json():
    h = hubspot_auth_headers()
    h["Content-Type"] = "application/json"
    return h

def route_for_ticket_pipeline_stage(ticket_props: dict):
    """
    Returns dict with routing info if ticket is in a supported (pipeline, new_stage), else None.
    """
    pipeline_id = str((ticket_props.get("hs_pipeline") or "")).strip()
    stage_id = str((ticket_props.get("hs_pipeline_stage") or "")).strip()

    # Pipeline A
    if pipeline_id == str(SUPPORT_PIPELINE_ID).strip() and stage_id == str(SUPPORT_NEW_STAGE_ID).strip():
        return {
            "name": "pipeline_a",
            "pipeline_id": pipeline_id,
            "stage_id": stage_id,
            "slack_channel": SLACK_CHANNEL_ID,
        }

    # Pipeline B
    if pipeline_id == str(PIPELINE_B_ID).strip() and stage_id == str(PIPELINE_B_NEW_STAGE_ID).strip():
        return {
            "name": "pipeline_b",
            "pipeline_id": pipeline_id,
            "stage_id": stage_id,
            "slack_channel": SLACK_CHANNEL_ID_B,
        }

    return None

def hubspot_ticket_url(ticket_id: str) -> str:
    """
    Builds a portal link to the ticket record.
    Requires HUBSPOT_PORTAL_ID env var.
    Example: https://app.hubspot.com/contacts/12345678/ticket/98765
    """
    if not HUBSPOT_PORTAL_ID:
        return ""
    tid = str(ticket_id).strip()
    pid = str(HUBSPOT_PORTAL_ID).strip()
    base = (HUBSPOT_PORTAL_BASE or "https://app.hubspot.com").rstrip("/")
    return f"{base}/contacts/{pid}/ticket/{tid}"


# ---------------------- SLACK ----------------------
def post_to_slack_text(text: str, channel_id: str = None):
    if not SLACK_BOT_TOKEN:
        raise RuntimeError("Missing SLACK_BOT_TOKEN env var")

    channel = channel_id or SLACK_CHANNEL_ID
    if not channel:
        raise RuntimeError("Missing SLACK_CHANNEL_ID (and no channel_id provided)")

    url = "https://slack.com/api/chat.postMessage"
    payload = {"channel": channel, "text": text}

    code, body = _http(
        "POST",
        url,
        headers={
            "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
            "Content-Type": "application/json; charset=utf-8",
        },
        data=json.dumps(payload).encode("utf-8"),
        timeout=20,
    )
    if code != 200:
        raise RuntimeError(f"Slack HTTP {code}: {truncate(body, 800)}")

    resp = json.loads(body)
    if not resp.get("ok"):
        raise RuntimeError(f"Slack not ok: {resp}")


# ---------------------- DDB IDEMPOTENCY ----------------------
def ddb_put_once(pk: str, sk: str) -> bool:
    """
    Returns True if inserted (first time), False if already exists.
    """
    if not DDB_TABLE:
        raise RuntimeError("Missing DDB_TABLE env var")

    now = int(time.time())
    ttl = now + IDEMP_TTL_SECONDS

    try:
        ddb.put_item(
            TableName=DDB_TABLE,
            Item={
                "pk": {"S": pk},
                "sk": {"S": sk},
                "created_at": {"N": str(now)},
                "ttl": {"N": str(ttl)},
            },
            ConditionExpression="attribute_not_exists(pk) AND attribute_not_exists(sk)",
        )
        return True
    except ddb.exceptions.ConditionalCheckFailedException:
        return False


# ---------------------- HTML/TEXT ----------------------
def html_to_text(s: str) -> str:
    if not s:
        return ""
    return re.sub(r"[ \t\r\f\v]+", " ", TAG_RE.sub("", _html.unescape(s))).strip()


def get_email_body(props: dict) -> str:
    return (props.get("hs_email_text") or "").strip() or html_to_text(props.get("hs_email_html") or "")


def parse_any_ts(v) -> int:
    if v in (None, ""):
        return 0
    try:
        return int(v)
    except Exception:
        pass
    try:
        dt = datetime.fromisoformat(str(v).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except Exception:
        return 0


def email_chrono_ts(props: dict) -> int:
    for k in TS_FIELDS:
        ts = parse_any_ts(props.get(k))
        if ts:
            return ts
    return 0


# ---------------------- HUBSPOT: Tickets ----------------------
def read_ticket(ticket_id: str):
    url = f"{HUBSPOT_BASE}/crm/v3/objects/tickets/{ticket_id}?properties=" + ",".join(TICKET_PROPS)
    code, body = _http("GET", url, headers=hubspot_auth_headers(), timeout=DEFAULT_HTTP_TIMEOUT)
    if code != 200:
        raise RuntimeError(f"HubSpot read ticket failed {code}: {truncate(body, 1000)}")
    data = json.loads(body)
    return {"id": str(data.get("id")), "properties": data.get("properties") or {}}


def get_company_from_ticket(ticket_id: str):
    url = f"{HUBSPOT_BASE}/crm/v4/objects/tickets/{ticket_id}/associations/companies?limit=1"
    code, body = _http("GET", url, headers=hubspot_auth_headers(), timeout=DEFAULT_HTTP_TIMEOUT)
    if code != 200:
        return None
    data = json.loads(body)
    ids = [str(r.get("toObjectId")) for r in (data.get("results") or []) if r.get("toObjectId")]
    return ids[0] if ids else None


def get_contact_ids_from_ticket(ticket_id: str, limit=5):
    url = f"{HUBSPOT_BASE}/crm/v4/objects/tickets/{ticket_id}/associations/contacts?limit={limit}"
    code, body = _http("GET", url, headers=hubspot_auth_headers(), timeout=DEFAULT_HTTP_TIMEOUT)
    if code != 200:
        return []
    data = json.loads(body)
    ids = [str(r.get("toObjectId")) for r in (data.get("results") or []) if r.get("toObjectId")]
    # de-dupe preserve order
    seen, out = set(), []
    for cid in ids:
        if cid not in seen:
            out.append(cid)
            seen.add(cid)
    return out


# ---------------------- HUBSPOT: Contacts & Company Resolution ----------------------
def read_contact(contact_id: str):
    if not contact_id:
        return None
    if contact_id in CONTACT_CACHE:
        return CONTACT_CACHE[contact_id]

    url = f"{HUBSPOT_BASE}/crm/v3/objects/contacts/{contact_id}?properties=" + ",".join(CONTACT_PROPS)
    code, body = _http("GET", url, headers=hubspot_auth_headers(), timeout=DEFAULT_HTTP_TIMEOUT)
    if code != 200:
        CONTACT_CACHE[contact_id] = None
        return None

    data = json.loads(body)
    props = data.get("properties") or {}
    CONTACT_CACHE[contact_id] = props
    return props


def format_contact_identity(contact_props: dict):
    if not contact_props:
        return "(unknown contact)"
    email = (contact_props.get("email") or "").strip()
    fn = (contact_props.get("firstname") or "").strip()
    ln = (contact_props.get("lastname") or "").strip()
    name = " ".join([x for x in (fn, ln) if x]).strip()
    if name and email:
        return f"{name} <{email}>"
    if email:
        return email
    if name:
        return name
    return "(unknown contact)"


def get_company_from_contact(contact_id: str):
    if not contact_id:
        return None
    if contact_id in CONTACT_COMPANY_CACHE:
        return CONTACT_COMPANY_CACHE[contact_id]

    url = f"{HUBSPOT_BASE}/crm/v4/objects/contacts/{contact_id}/associations/companies?limit=1"
    code, body = _http("GET", url, headers=hubspot_auth_headers(), timeout=DEFAULT_HTTP_TIMEOUT)
    if code != 200:
        CONTACT_COMPANY_CACHE[contact_id] = None
        return None
    data = json.loads(body)
    ids = [str(r.get("toObjectId")) for r in (data.get("results") or []) if r.get("toObjectId")]
    company_id = ids[0] if ids else None
    CONTACT_COMPANY_CACHE[contact_id] = company_id
    return company_id


def resolve_company_or_fallback_identity(ticket_id: str):
    """
    Returns:
      (company_id_or_none, contact_identity_str_or_none, contact_id_or_none)

    Resolution order:
    1) ticket -> company
    2) ticket -> contacts -> company
    3) ticket -> contacts (identity only)
    4) none
    """
    company_id = get_company_from_ticket(ticket_id)
    if company_id:
        return company_id, None, None

    contact_ids = get_contact_ids_from_ticket(ticket_id)
    for cid in contact_ids:
        c_company = get_company_from_contact(cid)
        if c_company:
            return c_company, None, cid

    if contact_ids:
        props = read_contact(contact_ids[0])
        return None, format_contact_identity(props), contact_ids[0]

    return None, None, None


def _read_company_properties(company_id: str):
    url = f"{HUBSPOT_BASE}/crm/v3/objects/companies/{company_id}?properties=name,hubspot_owner_id"
    code, body = _http("GET", url, headers=hubspot_auth_headers(), timeout=DEFAULT_HTTP_TIMEOUT)
    if code != 200:
        COMPANY_NAME_CACHE[company_id] = None
        COMPANY_OWNER_CACHE[company_id] = None
        return None
    data = json.loads(body)
    props = data.get("properties") or {}
    name = (props.get("name") or "").strip() or None
    owner_id = str(props.get("hubspot_owner_id") or "").strip() or None
    COMPANY_NAME_CACHE[company_id] = name
    COMPANY_OWNER_CACHE[company_id] = owner_id
    return props


def get_company_name(company_id: str):
    if company_id in COMPANY_NAME_CACHE:
        return COMPANY_NAME_CACHE[company_id]
    _read_company_properties(company_id)
    return COMPANY_NAME_CACHE.get(company_id)


def get_company_owner_id(company_id: str):
    if company_id in COMPANY_OWNER_CACHE:
        return COMPANY_OWNER_CACHE[company_id]
    _read_company_properties(company_id)
    return COMPANY_OWNER_CACHE.get(company_id)


def get_owner_name(owner_id: str):
    oid = str(owner_id or "").strip()
    if not oid:
        return None
    if oid in OWNER_NAME_CACHE:
        return OWNER_NAME_CACHE[oid]

    url = f"{HUBSPOT_BASE}/crm/v3/owners/{urllib.parse.quote(oid)}"
    code, body = _http("GET", url, headers=hubspot_auth_headers(), timeout=DEFAULT_HTTP_TIMEOUT)
    if code != 200:
        OWNER_NAME_CACHE[oid] = None
        return None

    data = json.loads(body)
    first = (data.get("firstName") or "").strip()
    last = (data.get("lastName") or "").strip()
    email = (data.get("email") or "").strip()
    full_name = " ".join([x for x in (first, last) if x]).strip()
    owner_name = full_name or email or oid
    OWNER_NAME_CACHE[oid] = owner_name
    return owner_name


def resolve_ticket_owner_name(ticket_props: dict, company_id: str = None):
    ticket_owner_id = str(
        ticket_props.get("hubspot_owner_id")
        or ticket_props.get("hs_ticket_owner")
        or ""
    ).strip()
    if ticket_owner_id:
        return get_owner_name(ticket_owner_id) or f"Owner ID {ticket_owner_id}"

    if company_id:
        company_owner_id = get_company_owner_id(company_id)
        if company_owner_id:
            return get_owner_name(company_owner_id) or f"Owner ID {company_owner_id}"

    return "(unassigned)"


# ---------------------- HUBSPOT: Emails ----------------------
def batch_read_emails(email_ids, properties):
    if not email_ids:
        return []
    url = f"{HUBSPOT_BASE}/crm/v3/objects/emails/batch/read"
    payload = {"properties": properties, "inputs": [{"id": str(eid)} for eid in email_ids]}

    code, body = _http(
        "POST",
        url,
        headers=hubspot_headers_json(),
        data=json.dumps(payload).encode("utf-8"),
        timeout=DEFAULT_HTTP_TIMEOUT,
    )
    if code != 200:
        raise RuntimeError(f"HubSpot batch read failed {code}: {truncate(body, 1000)}")

    return (json.loads(body).get("results") or [])


def fetch_company_email_assoc_page(company_id, after=None, limit=100):
    url = f"{HUBSPOT_BASE}/crm/v4/objects/companies/{company_id}/associations/emails?limit={limit}"
    if after:
        url += f"&after={urllib.parse.quote(str(after))}"

    code, body = _http("GET", url, headers=hubspot_auth_headers(), timeout=DEFAULT_HTTP_TIMEOUT)
    if code != 200:
        raise RuntimeError(f"HubSpot assoc fetch failed {code}: {truncate(body, 1000)}")
    return json.loads(body)


def get_latest_20_emails_for_company(company_id: str):
    """
    Robust version:
    - No early stopping
    - Deduplication
    - Full pagination scan
    - Strict sorting by timestamp (desc)
    Returns: list[(email_id, ts_ms)]
    """

    EMAIL_LIMIT = 20
    seen_ids = set()
    all_emails = []

    after = None

    while True:
        assoc = fetch_company_email_assoc_page(company_id, after=after, limit=100)

        ids_in_page = [
            str(r.get("toObjectId"))
            for r in (assoc.get("results") or [])
            if r.get("toObjectId")
        ]

        if not ids_in_page:
            break

        # Remove duplicates before batch call
        ids_in_page = [eid for eid in ids_in_page if eid not in seen_ids]

        if ids_in_page:
            recs = batch_read_emails(ids_in_page, properties=TS_FIELDS)

            for rec in recs:
                eid = str(rec.get("id"))
                if eid in seen_ids:
                    continue

                props = rec.get("properties") or {}
                ts = email_chrono_ts(props)

                if not ts:
                    continue  # skip emails with no timestamp

                all_emails.append((eid, ts))
                seen_ids.add(eid)

        # Pagination
        paging = (assoc.get("paging") or {}).get("next") or {}
        after = paging.get("after")

        if not after:
            break

    # FINAL SORT (global, high accuracy sorting)
    all_emails.sort(key=lambda x: x[1], reverse=True)

    # Return top 20
    return all_emails[:EMAIL_LIMIT]


# ---------------------- BEDROCK (Claude) ----------------------
def summarize_emails_with_claude(email_items):
    if not CLAUDE_MODEL_ID:
        raise RuntimeError("Missing CLAUDE_MODEL_ID env var")

    lines = []
    for i, item in enumerate(email_items[:EMAIL_LIMIT], 1):
        body = (item.get("body") or "").replace("\r", " ").replace("\n", " ")
        if len(body) > MAX_EMAIL_BODY_CHARS:
            body = body[:MAX_EMAIL_BODY_CHARS] + " …[truncated]"
        lines.append(
            f"Email {i}:\n"
            f"Subject: {item.get('subject')}\n"
            f"Date: {item.get('timestamp')}\n"
            f"Body: {body}\n"
        )

    prompt = (
        "You are an AI assistant that reads multiple customer email messages.\n\n"
        "This is part of an experiment to estimate how likely each company is to churn "
        "(cancel or significantly reduce their use of our product). When you assign the "
        "sentiment_score, you must take churn risk into account.\n\n"
        "Ignore legal footers, signatures, boilerplate.\n\n"
        "Return STRICT JSON ONLY:\n"
        "{\"summary\":\"<100-word-summary>\",\"sentiment_score\":<number 0..10>}\n\n"
        "Emails:\s:\n"
        + "\n\n".join(lines)
    )

    resp = bedrock.converse(
        modelId=CLAUDE_MODEL_ID,
        messages=[{"role": "user", "content": [{"text": prompt}]}],
        inferenceConfig={"maxTokens": 700},
    )

    text = (resp["output"]["message"]["content"][0]["text"] or "").strip()

    if text.startswith("```"):
        text = re.sub(r"^```[a-zA-Z0-9]*\n?", "", text)
        text = re.sub(r"\n?```$", "", text).strip()

    data = json.loads(text)
    summary = (data.get("summary") or "").strip()
    score = float(data.get("sentiment_score"))
    return summary, score


def summarize_ticket_with_claude(ticket_subject: str, ticket_content: str):
    """
    Fallback summarization when company_id cannot be resolved.
    """
    if not CLAUDE_MODEL_ID:
        raise RuntimeError("Missing CLAUDE_MODEL_ID env var")

    subj = (ticket_subject or "").strip() or "(no subject)"
    content = (ticket_content or "").strip()
    content = truncate(content, 8000)  # keep prompt sane

    prompt = (
        "You are an AI assistant summarizing a newly created support ticket.\n\n"
        "Goal: Provide a concise summary and churn-risk-aware sentiment_score.\n\n"
        "Return STRICT JSON ONLY:\n"
        "{\"summary\":\"<100-word-summary>\",\"sentiment_score\":<number 0..10>}\n\n"
        f"Ticket subject: {subj}\n\n"
        f"Ticket content:\n{content}\n"
    )

    resp = bedrock.converse(
        modelId=CLAUDE_MODEL_ID,
        messages=[{"role": "user", "content": [{"text": prompt}]}],
        inferenceConfig={"maxTokens": 600},
    )

    text = (resp["output"]["message"]["content"][0]["text"] or "").strip()

    if text.startswith("```"):
        text = re.sub(r"^```[a-zA-Z0-9]*\n?", "", text)
        text = re.sub(r"\n?```$", "", text).strip()

    data = json.loads(text)
    summary = (data.get("summary") or "").strip()
    score = float(data.get("sentiment_score"))
    return summary, score

def summarize_previous_emails_with_claude(email_items):
    """
    Summarizes historical emails (excluding latest).
    Focus: trend, tone over time, risk signals.
    STRICT: no assumptions, no hallucination.
    """
    if not CLAUDE_MODEL_ID:
        raise RuntimeError("Missing CLAUDE_MODEL_ID env var")

    lines = []
    for i, item in enumerate(email_items[:EMAIL_LIMIT], 1):
        body = (item.get("body") or "").replace("\r", " ").replace("\n", " ")
        if len(body) > MAX_EMAIL_BODY_CHARS:
            body = body[:MAX_EMAIL_BODY_CHARS] + " …[truncated]"
        lines.append(
            f"Email {i}:\n"
            f"Subject: {item.get('subject')}\n"
            f"Date: {item.get('timestamp')}\n"
            f"Body: {body}\n"
        )

    prompt = (
        "You are an AI assistant analyzing historical customer email conversations.\n\n"
        "These emails are NOT the latest message. They represent past interactions.\n\n"
        "Your task:\n"
        "- Identify overall tone and trend across emails\n"
        "- Detect repeated issues, dissatisfaction, or improvement over time\n"
        "- Assess churn risk based ONLY on what is explicitly written\n\n"
        "STRICT RULES:\n"
        "- DO NOT assume missing context\n"
        "- DO NOT invent details\n"
        "- DO NOT hallucinate causes or intent\n"
        "- ONLY use information explicitly present in the emails\n\n"
        "Return STRICT JSON ONLY:\n"
        "{\"summary\":\"<concise trend summary>\",\"sentiment_score\":<number 0..10>}\n\n"
        "Emails:\n\n"
        + "\n\n".join(lines)
    )

    resp = bedrock.converse(
        modelId=CLAUDE_MODEL_ID,
        messages=[{"role": "user", "content": [{"text": prompt}]}],
        inferenceConfig={"maxTokens": 700},
    )

    text = (resp["output"]["message"]["content"][0]["text"] or "").strip()

    if text.startswith("```"):
        text = re.sub(r"^```[a-zA-Z0-9]*\n?", "", text)
        text = re.sub(r"\n?```$", "", text).strip()

    data = json.loads(text)
    summary = (data.get("summary") or "").strip()
    score = float(data.get("sentiment_score"))
    return summary, score

def is_obvious_spam_text(subject: str, body: str) -> bool:
    subject_clean = (subject or "").strip().lower()
    body_clean = (body or "").strip().lower()
    text = f"{subject_clean}\n{body_clean}"
    compact = truncate(text, MAX_SPAM_SCAN_CHARS)

    if AUTO_REPLY_SUBJECT_RE.search(subject_clean):
        return True
    if AUTO_REPLY_BODY_RE.search(compact):
        return True
    return bool(SPAM_SIGNAL_RE.search(compact))


def detect_spam_with_claude(subject_text: str, body_text: str, summary_text: str = ""):
    """
    Returns True if spam, False otherwise.
    """
    if not CLAUDE_MODEL_ID:
        raise RuntimeError("Missing CLAUDE_MODEL_ID env var")

    subject_text = truncate(subject_text or "", 500)
    body_text = truncate(body_text or "", MAX_SPAM_SCAN_CHARS)
    summary_text = truncate(summary_text or "", 1200)

    prompt = (
        "You are a strict classifier that detects whether a customer message is spam.\n\n"
        "Mark is_spam=true when the message is any of these:\n"
        "- Out-of-office / automatic replies (always spam for this workflow)\n"
        "- Marketing, newsletter, promotional, or advertisement emails\n"
        "- Service-offering cold outreach (SEO, lead generation, web design, etc.)\n"
        "- Generic sales outreach not tied to a real support issue\n\n"
        "Mark is_spam=false only when there is a genuine support issue/question.\n\n"
        "Return STRICT JSON ONLY:\n"
        "{\"is_spam\": true/false, \"reason\":\"<short reason>\"}\n\n"
        f"Subject:\n{subject_text}\n\n"
        f"Body:\n{body_text}\n\n"
        f"Summary:\n{summary_text}\n"
    )

    resp = bedrock.converse(
        modelId=CLAUDE_MODEL_ID,
        messages=[{"role": "user", "content": [{"text": prompt}]}],
        inferenceConfig={"maxTokens": 200, "temperature": 0.0},
    )

    text = (resp["output"]["message"]["content"][0]["text"] or "").strip()

    if text.startswith("```"):
        text = re.sub(r"^```[a-zA-Z0-9]*\n?", "", text)
        text = re.sub(r"\n?```$", "", text).strip()

    data = json.loads(text)
    return bool(data.get("is_spam"))

# ---------------------- Webhook parsing (CHANGED) ----------------------
def extract_ticket_ids_any_ticket_event(events):
    """
    NEW RULE:
    We do NOT rely on event types/conditions beyond "it's a ticket event with an objectId".
    We'll read the ticket and apply ONE strict condition:
      ticket is currently in SUPPORT_PIPELINE_ID AND SUPPORT_NEW_STAGE_ID.
    """
    ticket_ids = []
    for e in events:
        ev_type = (e.get("subscriptionType") or e.get("eventType") or "").lower()
        if not ev_type.startswith("ticket."):
            continue
        tid = e.get("objectId") or e.get("object_id")
        if tid:
            ticket_ids.append(str(tid))

    # de-dupe preserve order
    seen, out = set(), []
    for t in ticket_ids:
        if t not in seen:
            out.append(t)
            seen.add(t)
    return out


def is_ticket_in_support_new_stage(ticket_props: dict) -> bool:
    pipeline_id = str((ticket_props.get("hs_pipeline") or "")).strip()
    stage_id = str((ticket_props.get("hs_pipeline_stage") or "")).strip()
    return pipeline_id == str(SUPPORT_PIPELINE_ID).strip() and stage_id == str(SUPPORT_NEW_STAGE_ID).strip()


# ---------------------- MAIN HANDLER ----------------------
def lambda_handler(event, context):
    if not HUBSPOT_PORTAL_ID:
        raise RuntimeError("Missing HUBSPOT_PORTAL_ID env var (needed to build ticket links)")
    if not SUPPORT_PIPELINE_ID or not SUPPORT_NEW_STAGE_ID:
        raise RuntimeError("Missing SUPPORT_PIPELINE_ID or SUPPORT_NEW_STAGE_ID env vars")

    if not PIPELINE_B_ID or not PIPELINE_B_NEW_STAGE_ID:
        raise RuntimeError("Missing PIPELINE_B_ID or PIPELINE_B_NEW_STAGE_ID env vars")

    if not SLACK_CHANNEL_ID:
        raise RuntimeError("Missing SLACK_CHANNEL_ID env var (pipeline A channel)")

    if not SLACK_CHANNEL_ID_B:
        raise RuntimeError("Missing SLACK_CHANNEL_ID_B env var (pipeline B channel)")


    records = event.get("Records", []) or []
    log("INFO", "Worker invoked", record_count=len(records))

    failures = []

    for record in records:
        msg_id = record.get("messageId")
        try:
            payload = json.loads(record.get("body") or "{}")
            events = payload.get("raw_events") or []
            dedupe_key = payload.get("dedupe_key") or f"msg:{msg_id}"

            ticket_ids = extract_ticket_ids_any_ticket_event(events)
            if not ticket_ids:
                log("INFO", "No ticket events in message; skipping", msg_id=msg_id)
                continue

            log("INFO", "Ticket ids extracted", msg_id=msg_id, dedupe_key=dedupe_key, ticket_ids=ticket_ids)

            for ticket_id in ticket_ids:
                # Dedupe per ticket per incoming message (or dedupe_key)
                pk = f"ticket#{ticket_id}"
                sk = f"dedupe#{dedupe_key}"
                if not ddb_put_once(pk, sk):
                    log("INFO", "Idempotency skip", msg_id=msg_id, ticket_id=ticket_id, pk=pk, sk=sk)
                    continue

                log("INFO", "Reading ticket", msg_id=msg_id, ticket_id=ticket_id)
                ticket = read_ticket(ticket_id)
                tprops = ticket.get("properties") or {}

               
                route = route_for_ticket_pipeline_stage(tprops)
                if not route:
                    log(
                        "INFO",
                        "Ticket not in any supported pipeline/new stage; skipping",
                        msg_id=msg_id,
                        ticket_id=ticket_id,
                        hs_pipeline=str(tprops.get("hs_pipeline") or ""),
                        hs_pipeline_stage=str(tprops.get("hs_pipeline_stage") or ""),
                        expected_a_pipeline=str(SUPPORT_PIPELINE_ID),
                        expected_a_stage=str(SUPPORT_NEW_STAGE_ID),
                        expected_b_pipeline=str(PIPELINE_B_ID),
                        expected_b_stage=str(PIPELINE_B_NEW_STAGE_ID),
                    )
                    continue

                log(
                    "INFO",
                    "Ticket qualifies — processing",
                    msg_id=msg_id,
                    ticket_id=ticket_id,
                    route=route["name"],
                    pipeline_id=route["pipeline_id"],
                    stage_id=route["stage_id"],
                )


                ticket_subject = (tprops.get("subject") or "").strip()
                ticket_content = (tprops.get("content") or "").strip()
                ticket_created = (tprops.get("createdate") or "").strip()

                log(
                    "INFO",
                    "Ticket qualifies (Support/New) — processing",
                    msg_id=msg_id,
                    ticket_id=ticket_id,
                    createdate=ticket_created,
                    subject=truncate(ticket_subject, 120),
                )

                # Resolve company if possible, else contact identity
                company_id, contact_identity, contact_id = resolve_company_or_fallback_identity(ticket_id)
                owner_name = resolve_ticket_owner_name(tprops, company_id=company_id)

                # -------- Path A: Company found -> summarize latest email + previous 19 emails --------
                if company_id:
                    log("INFO", "Resolved company", msg_id=msg_id, ticket_id=ticket_id, company_id=company_id)

                    top20 = get_latest_20_emails_for_company(company_id)
                    if not top20:
                        log("INFO", "No emails found for company; falling back to ticket summary", company_id=company_id, ticket_id=ticket_id)
                        summary, score = summarize_ticket_with_claude(ticket_subject, ticket_content)
                        summary_prev, score_prev = summary, score
                        goto_slack_company = get_company_name(company_id) or f"Company ID {company_id}"
                        goto_company_id = company_id
                        goto_identity = contact_identity or "(unknown contact)"
                        goto_mode = "ticket_fallback_no_emails"
                    else:
                        top_ids = [eid for eid, _ in top20]
                        details = batch_read_emails(top_ids, properties=TS_FIELDS + BODY_FIELDS)
                        id_to_props = {str(r.get("id")): (r.get("properties") or {}) for r in details}

                        email_items = []
                        for eid, ts in top20:
                            props = id_to_props.get(str(eid), {})
                            body = get_email_body(props)
                            if not body:
                                continue
                            subject = (props.get("hs_email_subject") or "").strip() or "(no subject)"
                            dt_iso = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).isoformat()
                            email_items.append({"subject": subject, "timestamp": dt_iso, "body": body})

                        if not email_items:
                            log("INFO", "No email bodies found; falling back to ticket summary", company_id=company_id, ticket_id=ticket_id)
                            summary, score = summarize_ticket_with_claude(ticket_subject, ticket_content)
                            summary_prev, score_prev = summary, score
                            goto_slack_company = get_company_name(company_id) or f"Company ID {company_id}"
                            goto_company_id = company_id
                            goto_identity = contact_identity or "(unknown contact)"
                            goto_mode = "ticket_fallback_no_email_bodies"
                        else:
                            latest_subject = email_items[0].get("subject") or ""
                            latest_body = email_items[0].get("body") or ""

                            # -------- Spam detection: heuristic + AI --------
                            is_spam = is_obvious_spam_text(latest_subject, latest_body)
                            if not is_spam:
                                is_spam = detect_spam_with_claude(latest_subject, latest_body)

                            if is_spam:
                                log(
                                    "INFO",
                                    "Spam detected — skipping Slack + previous summarization",
                                    msg_id=msg_id,
                                    ticket_id=ticket_id,
                                    route=route["name"],
                                )
                                continue  # 🚨 HARD STOP

                            # -------- AI CALL 1: Latest email --------
                            summary, score = summarize_emails_with_claude(email_items[:1])

                            # -------- AI CALL 3: Previous emails --------
                            if len(email_items) > 1:
                                summary_prev, score_prev = summarize_previous_emails_with_claude(email_items[1:EMAIL_LIMIT])
                            else:
                                summary_prev, score_prev = "", score

                            goto_slack_company = get_company_name(company_id) or f"Company ID {company_id}"
                            goto_company_id = company_id
                            goto_identity = contact_identity or "(unknown contact)"
                            goto_mode = "company_emails"

                # -------- Path B: No Company -> summarize ticket content --------
                else:
                    log("INFO", "No company resolved; summarizing ticket only", msg_id=msg_id, ticket_id=ticket_id, contact_id=contact_id)
                    if is_obvious_spam_text(ticket_subject, ticket_content):
                        log(
                            "INFO",
                            "Spam detected in ticket-only path (heuristic) — skipping Slack",
                            msg_id=msg_id,
                            ticket_id=ticket_id,
                            route=route["name"],
                        )
                        continue

                    summary, score = summarize_ticket_with_claude(ticket_subject, ticket_content)
                    if detect_spam_with_claude(ticket_subject, ticket_content, summary):
                        log(
                            "INFO",
                            "Spam detected in ticket-only path (AI) — skipping Slack",
                            msg_id=msg_id,
                            ticket_id=ticket_id,
                            route=route["name"],
                        )
                        continue

                    summary_prev, score_prev = summary, score
                    goto_slack_company = None
                    goto_company_id = None
                    goto_identity = contact_identity or "(unknown contact)"
                    goto_mode = "ticket_only"

                score_txt = f"{score:.1f}"
                emoji = "🟢" if score >= 7 else "🟡" if score >= 5 else "🔴"
                score_prev_txt = f"{score_prev:.1f}"
                emoji_prev = "🟢" if score_prev >= 7 else "🟡" if score_prev >= 5 else "🔴"

                ticket_title = ticket_subject or f"Ticket {ticket_id}"

                ticket_url = hubspot_ticket_url(ticket_id)
                ticket_link = f"<{ticket_url}|Ticket>" if ticket_url else "Ticket"

                lines = [
                            f"*Support Ticket Churn Summary* {emoji}",
                            f"*Mode:* {goto_mode}",
                            f"*Ticket:* {ticket_title} (`{ticket_id}`) — {ticket_link}",
                            f"*Owner:* {owner_name}",
                        ]

                if ticket_created:
                    lines.append(f"*Ticket created:* {ticket_created}")

                if goto_company_id:
                    lines.append(f"*Company:* {goto_slack_company}")
                    lines.append(f"*Company ID:* {goto_company_id}")
                else:
                    lines.append(f"*Sender / Contact:* {goto_identity}")

                lines.append(f"*Latest email sentiment score:* {score_txt} {emoji}")
                lines.append(f"*Latest email summary:* {summary}")
                lines.append(f"*Previous emails sentiment score:* {score_prev_txt} {emoji_prev}")
                lines.append(f"*Previous emails summary:* {summary_prev}")

                slack_text = truncate("\n".join(lines), 3500)
                post_to_slack_text(slack_text, channel_id=route["slack_channel"])

                log(
                    "INFO",
                    "Posted to Slack",
                    msg_id=msg_id,
                    ticket_id=ticket_id,
                    latest_score=score,
                    previous_score=score_prev,
                    route=route["name"],
                )


        except Exception as e:
            log(
                "ERROR",
                "Failed processing record",
                msg_id=msg_id,
                error=repr(e),
                traceback=traceback.format_exc()[:6000],
            )
            failures.append({"itemIdentifier": msg_id})

    log("INFO", "Worker finished", failure_count=len(failures))
    return {"batchItemFailures": failures}
