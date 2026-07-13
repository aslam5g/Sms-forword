"""
Telegram Auto-Forwarder Backend (multi-rule + keyword filter + history + hide-source)
------------------------------------------------------------------------------------
New in this version:
- `hide_source` option per rule. When True, messages are sent as a fresh message
  (copy) instead of a native Telegram "forward", so the "Forwarded from X" tag
  does not appear. This already happens automatically when sender="bot" (bots
  always send fresh messages). This flag makes it available for sender="account" too.

Run locally:
    pip install telethon fastapi uvicorn python-multipart asyncpg
    uvicorn main:app --reload

Deploy on Render:
    - Start command: uvicorn main:app --host 0.0.0.0 --port $PORT
    - Env vars required: TG_API_ID, TG_API_HASH, DATABASE_URL
"""

import os
import asyncio
import hashlib
import json
import re
import secrets
from collections import deque
import asyncpg
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.errors import SessionPasswordNeededError

app = FastAPI(title="Telegram Auto-Forwarder")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---- Config ----
API_ID = int(os.environ.get("TG_API_ID", "0"))
API_HASH = os.environ.get("TG_API_HASH", "")
DATABASE_URL = os.environ.get("DATABASE_URL", "")

db_pool: asyncpg.Pool | None = None

# ---- In-memory state ----
SESSIONS = {}        # phone -> Telethon client instance (during login only)
USER_SESSIONS = {}   # phone -> saved session string (after login)
AUTH_TOKENS = {}     # token -> phone (persistent login, survives app restarts on the device)
READERS = {}         # phone -> a single shared TelegramClient used to listen for that phone
BOT_CLIENTS = {}     # bot_token -> TelegramClient (reused across rules using the same bot)
RULES = {}           # rule_id (int) -> rule dict
TARGET_LABEL_CACHE = {}  # target_id -> "Title\nhttps://t.me/username" (or without link if private)
SEEN_HASHES = {}     # rule_id -> deque of recent content hashes (bounded window)
SEEN_SETS = {}       # rule_id -> set of the same hashes, for O(1) duplicate lookup
DUPLICATE_WINDOW = 200  # how many recent messages per rule we remember for dedup


# ---------- Models ----------
class PhoneRequest(BaseModel):
    phone: str


class CodeVerifyRequest(BaseModel):
    phone: str
    code: str
    phone_code_hash: str
    password: str | None = None


class ReplaceRule(BaseModel):
    find: str
    replace: str = ""


class CleanerOptions(BaseModel):
    remove_links: bool = False
    remove_mentions: bool = False
    remove_emojis: bool = False
    remove_hashtags: bool = False


class RuleCreateRequest(BaseModel):
    phone: str
    token: str
    source_ids: list[int]
    target_ids: list[int]
    sender: str  # "account" or "bot"
    bot_token: str | None = None
    keywords: list[str] | None = None
    label: str | None = None
    hide_source: bool = False  # NEW: send as a fresh copy instead of a native forward
    attribution_label: str | None = None  # NEW: custom text to show instead of original source
                                            # (e.g. "via @MyBot"). If empty, auto-detected from
                                            # the sender account/bot's own name.
    attribution_mode: str = "sender"  # NEW: "sender" = show sending account/bot's name (old default),
                                        # "target" = show the destination channel's OWN name + t.me
                                        # link instead (like a manual post would look). Ignored if
                                        # attribution_label is set (that always wins).
    duplicate_filter: bool = False  # NEW: skip forwarding a message whose content was already
                                      # forwarded recently by this same rule
    header_text: str | None = None  # NEW: text prepended to every forwarded message
    footer_text: str | None = None  # NEW: text appended to every forwarded message
    replace_rules: list[ReplaceRule] | None = None  # NEW: find/replace pairs applied to text
    cleaner_options: CleanerOptions | None = None  # NEW: strip links/mentions/emojis/hashtags


# ---------- Helpers ----------
def _require_config():
    if not API_ID or not API_HASH:
        raise HTTPException(500, "Server missing TG_API_ID / TG_API_HASH env vars.")


def _matches_keywords(text: str, keywords: list[str]) -> bool:
    if not keywords:
        return True
    if not text:
        return False
    lowered = text.lower()
    return any(kw.lower() in lowered for kw in keywords)


_LINK_PATTERN = re.compile(r'(https?://\S+|www\.\S+|t\.me/\S+)', re.IGNORECASE)
_MENTION_PATTERN = re.compile(r'@\w+')
_HASHTAG_PATTERN = re.compile(r'#\w+')
_EMOJI_PATTERN = re.compile(
    "["
    "\U0001F300-\U0001FAFF"  # symbols & pictographs, supplemental symbols, emoticons, etc.
    "\U00002600-\U000027BF"  # misc symbols & dingbats
    "\U0001F1E6-\U0001F1FF"  # regional indicator (flags)
    "\U00002700-\U000027BF"
    "\U0001F900-\U0001F9FF"
    "]+",
    flags=re.UNICODE,
)


def _apply_cleaner(text: str, options: dict | None) -> str:
    if not options:
        return text
    if options.get("remove_links"):
        text = _LINK_PATTERN.sub("", text)
    if options.get("remove_mentions"):
        text = _MENTION_PATTERN.sub("", text)
    if options.get("remove_hashtags"):
        text = _HASHTAG_PATTERN.sub("", text)
    if options.get("remove_emojis"):
        text = _EMOJI_PATTERN.sub("", text)
    # collapse extra blank lines/spaces left behind by removals
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def _apply_replacements(text: str, replace_rules: list) -> str:
    for r in (replace_rules or []):
        find = r.get("find", "")
        repl = r.get("replace", "")
        if find:
            text = text.replace(find, repl)
    return text


def _content_hash(event) -> str:
    """
    A fingerprint for the message's content, used for duplicate detection.
    Text messages are hashed by their normalized text; media-only messages
    fall back to the file's unique id (or the message id as a last resort).
    """
    text = (event.message.message or "").strip().lower()
    if text:
        basis = text
    else:
        file_id = getattr(getattr(event.message, "file", None), "id", None)
        basis = f"media:{file_id}" if file_id else f"msgid:{event.message.id}"
    return hashlib.md5(basis.encode("utf-8", errors="ignore")).hexdigest()


def _is_duplicate(rule_id: int, content_hash: str) -> bool:
    return content_hash in SEEN_SETS.get(rule_id, set())


def _mark_seen(rule_id: int, content_hash: str):
    dq = SEEN_HASHES.setdefault(rule_id, deque(maxlen=DUPLICATE_WINDOW))
    s = SEEN_SETS.setdefault(rule_id, set())
    if len(dq) == dq.maxlen:
        s.discard(dq[0])  # about to be evicted by the append below
    dq.append(content_hash)
    s.add(content_hash)


# ---------- DB lifecycle ----------
@app.on_event("startup")
async def on_startup():
    global db_pool
    if not DATABASE_URL:
        print("[warning] DATABASE_URL not set — nothing will survive restarts.")
        return

    db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    async with db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS telegram_users (
                phone TEXT PRIMARY KEY,
                session_string TEXT NOT NULL
            );
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS auth_tokens (
                token TEXT PRIMARY KEY,
                phone TEXT NOT NULL
            );
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS forward_rules (
                id SERIAL PRIMARY KEY,
                phone TEXT NOT NULL,
                label TEXT,
                source_ids TEXT NOT NULL,
                target_ids TEXT NOT NULL,
                sender TEXT NOT NULL,
                bot_token TEXT,
                keywords TEXT,
                hide_source BOOLEAN DEFAULT FALSE,
                running BOOLEAN DEFAULT TRUE
            );
        """)
        # In case this table already existed from the previous version, add the new column.
        await conn.execute("""
            ALTER TABLE forward_rules ADD COLUMN IF NOT EXISTS hide_source BOOLEAN DEFAULT FALSE;
        """)
        await conn.execute("""
            ALTER TABLE forward_rules ADD COLUMN IF NOT EXISTS attribution_label TEXT;
        """)
        await conn.execute("""
            ALTER TABLE forward_rules ADD COLUMN IF NOT EXISTS duplicate_filter BOOLEAN DEFAULT FALSE;
        """)
        await conn.execute("""
            ALTER TABLE forward_rules ADD COLUMN IF NOT EXISTS header_text TEXT;
        """)
        await conn.execute("""
            ALTER TABLE forward_rules ADD COLUMN IF NOT EXISTS footer_text TEXT;
        """)
        await conn.execute("""
            ALTER TABLE forward_rules ADD COLUMN IF NOT EXISTS replace_rules TEXT;
        """)
        await conn.execute("""
            ALTER TABLE forward_rules ADD COLUMN IF NOT EXISTS cleaner_options TEXT;
        """)
        await conn.execute("""
            ALTER TABLE forward_rules ADD COLUMN IF NOT EXISTS attribution_mode TEXT DEFAULT 'sender';
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS forward_history (
                id SERIAL PRIMARY KEY,
                rule_id INTEGER,
                phone TEXT,
                source_id BIGINT,
                target_id BIGINT,
                preview TEXT,
                created_at TIMESTAMPTZ DEFAULT now()
            );
        """)

    async with db_pool.acquire() as conn:
        user_rows = await conn.fetch("SELECT * FROM telegram_users;")
        token_rows = await conn.fetch("SELECT * FROM auth_tokens;")
        rule_rows = await conn.fetch("SELECT * FROM forward_rules WHERE running = TRUE;")

    for row in user_rows:
        USER_SESSIONS[row["phone"]] = row["session_string"]

    for row in token_rows:
        AUTH_TOKENS[row["token"]] = row["phone"]

    for row in rule_rows:
        rule = {
            "phone": row["phone"],
            "label": row["label"],
            "source_ids": [int(x) for x in row["source_ids"].split(",") if x],
            "target_ids": [int(x) for x in row["target_ids"].split(",") if x],
            "sender": row["sender"],
            "bot_token": row["bot_token"],
            "keywords": row["keywords"].split(",") if row["keywords"] else [],
            "hide_source": row["hide_source"],
            "attribution_label": row["attribution_label"],
            "duplicate_filter": row["duplicate_filter"],
            "header_text": row["header_text"],
            "footer_text": row["footer_text"],
            "replace_rules": json.loads(row["replace_rules"]) if row["replace_rules"] else [],
            "cleaner_options": json.loads(row["cleaner_options"]) if row["cleaner_options"] else None,
            "attribution_mode": row["attribution_mode"] or "sender",
            "running": True,
        }
        RULES[row["id"]] = rule
        asyncio.create_task(_start_rule_listener(row["id"]))
        print(f"[resume] auto-resumed rule id={row['id']} phone={row['phone']}")

    print(f"[startup] loaded {len(user_rows)} user(s) and {len(rule_rows)} active rule(s) from database.")


@app.on_event("shutdown")
async def on_shutdown():
    if db_pool:
        await db_pool.close()


async def _save_session(phone: str, session_string: str):
    if not db_pool:
        return
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO telegram_users (phone, session_string)
            VALUES ($1, $2)
            ON CONFLICT (phone) DO UPDATE SET session_string = $2;
        """, phone, session_string)


async def _create_token(phone: str) -> str:
    """Creates a persistent login token for this phone. The app stores this on
    the device and sends it with every request instead of trusting a bare phone
    number, so a stranger who merely knows the phone number can't hijack the
    account. The token survives app restarts and server restarts (saved to DB)."""
    token = secrets.token_urlsafe(32)
    AUTH_TOKENS[token] = phone
    if db_pool:
        async with db_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO auth_tokens (token, phone) VALUES ($1, $2) ON CONFLICT (token) DO NOTHING;",
                token, phone,
            )
    return token


def _phone_for_token(token: str) -> str:
    phone = AUTH_TOKENS.get(token)
    if not phone:
        raise HTTPException(401, "Invalid or expired session. Please log in again.")
    return phone


async def _delete_token(token: str):
    AUTH_TOKENS.pop(token, None)
    if db_pool:
        async with db_pool.acquire() as conn:
            await conn.execute("DELETE FROM auth_tokens WHERE token = $1;", token)


async def _insert_rule_db(rule: dict) -> int:
    if not db_pool:
        return -1
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO forward_rules (phone, label, source_ids, target_ids, sender, bot_token, keywords, hide_source, attribution_label, duplicate_filter, header_text, footer_text, replace_rules, cleaner_options, attribution_mode, running)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, TRUE)
            RETURNING id;
        """,
            rule["phone"], rule.get("label"),
            ",".join(str(x) for x in rule["source_ids"]),
            ",".join(str(x) for x in rule["target_ids"]),
            rule["sender"], rule["bot_token"],
            ",".join(rule["keywords"]) if rule["keywords"] else None,
            rule["hide_source"],
            rule.get("attribution_label"),
            rule.get("duplicate_filter", False),
            rule.get("header_text"),
            rule.get("footer_text"),
            json.dumps(rule["replace_rules"]) if rule.get("replace_rules") else None,
            json.dumps(rule["cleaner_options"]) if rule.get("cleaner_options") else None,
            rule.get("attribution_mode", "sender"),
        )
        return row["id"]


async def _mark_rule_stopped(rule_id: int):
    if not db_pool:
        return
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE forward_rules SET running = FALSE WHERE id = $1;", rule_id)


async def _delete_rule_db(rule_id: int):
    if not db_pool:
        return
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM forward_rules WHERE id = $1;", rule_id)


async def _log_history(rule_id: int, phone: str, source_id: int, target_id: int, preview: str):
    if not db_pool:
        return
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO forward_history (rule_id, phone, source_id, target_id, preview)
            VALUES ($1, $2, $3, $4, $5);
        """, rule_id, phone, source_id, target_id, (preview or "")[:200])


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "active_rules": len([r for r in RULES.values() if r["running"]]),
        "db_connected": db_pool is not None,
    }


# ---------- 1. Login: send code ----------
@app.post("/login/send-code")
async def send_code(req: PhoneRequest):
    _require_config()
    client = TelegramClient(StringSession(), API_ID, API_HASH)
    await client.connect()
    try:
        sent = await client.send_code_request(req.phone)
    except Exception as e:
        await client.disconnect()
        raise HTTPException(400, f"Could not send code: {e}")
    SESSIONS[req.phone] = client
    return {"phone_code_hash": sent.phone_code_hash, "message": "Code sent to Telegram app."}


# ---------- 2. Login: verify code ----------
@app.post("/login/verify")
async def verify_code(req: CodeVerifyRequest):
    client = SESSIONS.get(req.phone)
    if not client:
        raise HTTPException(400, "Call /login/send-code first.")

    try:
        await client.sign_in(
            phone=req.phone,
            code=req.code,
            phone_code_hash=req.phone_code_hash,
        )
    except SessionPasswordNeededError:
        if not req.password:
            raise HTTPException(401, "Account has 2FA enabled. Provide 'password' field.")
        try:
            await client.sign_in(password=req.password)
        except Exception as e:
            raise HTTPException(401, f"2FA password rejected: {e}")
    except Exception as e:
        raise HTTPException(400, f"Verification failed: {e}")

    session_string = client.session.save()
    USER_SESSIONS[req.phone] = session_string
    await client.disconnect()
    del SESSIONS[req.phone]
    await _save_session(req.phone, session_string)
    token = await _create_token(req.phone)

    return {"message": "Login successful.", "session_saved": True, "token": token, "phone": req.phone}


@app.get("/me")
async def me(token: str):
    """The app calls this on launch to check if a saved token is still valid,
    so it can skip straight to the dashboard instead of showing the login screen."""
    phone = _phone_for_token(token)
    return {"phone": phone}


@app.post("/logout")
async def logout(req: dict):
    token = req.get("token")
    if token:
        await _delete_token(token)
    return {"message": "Logged out."}


# ---------- 3. List dialogs ----------
@app.get("/dialogs")
async def list_dialogs(phone: str, token: str):
    if _phone_for_token(token) != phone:
        raise HTTPException(401, "Token does not match this phone.")
    session_string = USER_SESSIONS.get(phone)
    if not session_string:
        raise HTTPException(401, "Not logged in. Complete /login/verify first.")

    client = TelegramClient(StringSession(session_string), API_ID, API_HASH)
    await client.connect()

    dialogs = []
    async for d in client.iter_dialogs():
        entity_username = getattr(d.entity, "username", None)
        dialogs.append({
            "id": d.id,
            "name": d.name,
            "username": entity_username,
            "is_group": d.is_group,
            "is_channel": d.is_channel,
            "is_user": d.is_user,
        })

    await client.disconnect()
    return {"dialogs": dialogs}


# ---------- 4. Forwarding rules (multi-rule) ----------
@app.get("/forward/rules")
async def get_rules(phone: str, token: str):
    if _phone_for_token(token) != phone:
        raise HTTPException(401, "Token does not match this phone.")
    result = []
    for rule_id, rule in RULES.items():
        if rule["phone"] == phone:
            result.append({
                "id": rule_id,
                "label": rule.get("label"),
                "source_ids": rule["source_ids"],
                "target_ids": rule["target_ids"],
                "sender": rule["sender"],
                "keywords": rule["keywords"],
                "hide_source": rule.get("hide_source", False),
                "attribution_label": rule.get("attribution_label"),
                "duplicate_filter": rule.get("duplicate_filter", False),
                "header_text": rule.get("header_text"),
                "footer_text": rule.get("footer_text"),
                "replace_rules": rule.get("replace_rules") or [],
                "cleaner_options": rule.get("cleaner_options"),
                "attribution_mode": rule.get("attribution_mode", "sender"),
                "running": rule["running"],
            })
    return {"rules": result}


@app.post("/forward/rules")
async def create_rule(req: RuleCreateRequest):
    if _phone_for_token(req.token) != req.phone:
        raise HTTPException(401, "Token does not match this phone.")
    if req.phone not in USER_SESSIONS:
        raise HTTPException(401, "Not logged in.")
    if req.sender == "bot" and not req.bot_token:
        raise HTTPException(400, "bot_token required when sender='bot'.")

    rule = {
        "phone": req.phone,
        "label": req.label or f"Rule ({len(req.source_ids)} source -> {len(req.target_ids)} target)",
        "source_ids": req.source_ids,
        "target_ids": req.target_ids,
        "sender": req.sender,
        "bot_token": req.bot_token,
        "keywords": [k.strip() for k in (req.keywords or []) if k.strip()],
        "hide_source": req.hide_source,
        "attribution_label": (req.attribution_label or "").strip() or None,
        "duplicate_filter": req.duplicate_filter,
        "header_text": (req.header_text or "").strip() or None,
        "footer_text": (req.footer_text or "").strip() or None,
        "replace_rules": [r.dict() for r in (req.replace_rules or [])],
        "cleaner_options": req.cleaner_options.dict() if req.cleaner_options else None,
        "attribution_mode": req.attribution_mode if req.attribution_mode in ("sender", "target") else "sender",
        "running": True,
    }

    rule_id = await _insert_rule_db(rule)
    if rule_id == -1:
        rule_id = (max(RULES.keys()) + 1) if RULES else 1

    RULES[rule_id] = rule
    asyncio.create_task(_start_rule_listener(rule_id))

    return {"message": "Rule created and started.", "rule_id": rule_id}


@app.post("/forward/rules/{rule_id}/stop")
async def stop_rule(rule_id: int, token: str):
    rule = RULES.get(rule_id)
    if not rule:
        raise HTTPException(404, "Rule not found.")
    if _phone_for_token(token) != rule["phone"]:
        raise HTTPException(401, "Not authorized for this rule.")
    rule["running"] = False
    await _mark_rule_stopped(rule_id)
    return {"message": "Rule stopped."}


@app.delete("/forward/rules/{rule_id}")
async def delete_rule(rule_id: int, token: str):
    rule = RULES.get(rule_id)
    if not rule:
        raise HTTPException(404, "Rule not found.")
    if _phone_for_token(token) != rule["phone"]:
        raise HTTPException(401, "Not authorized for this rule.")
    rule["running"] = False
    await _delete_rule_db(rule_id)
    del RULES[rule_id]
    SEEN_HASHES.pop(rule_id, None)
    SEEN_SETS.pop(rule_id, None)
    return {"message": "Rule deleted."}


# ---------- 5. History ----------
@app.get("/forward/history")
async def get_history(phone: str, token: str, limit: int = 50):
    if _phone_for_token(token) != phone:
        raise HTTPException(401, "Token does not match this phone.")
    if not db_pool:
        return {"history": []}
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT id, rule_id, source_id, target_id, preview, created_at
            FROM forward_history
            WHERE phone = $1
            ORDER BY created_at DESC
            LIMIT $2;
        """, phone, limit)
    return {
        "history": [
            {
                "id": r["id"],
                "rule_id": r["rule_id"],
                "source_id": r["source_id"],
                "target_id": r["target_id"],
                "preview": r["preview"],
                "created_at": r["created_at"].isoformat(),
            }
            for r in rows
        ]
    }


# ---------- Background listener ----------
async def _get_reader(phone: str) -> TelegramClient:
    if phone in READERS:
        return READERS[phone]
    session_string = USER_SESSIONS[phone]
    reader = TelegramClient(StringSession(session_string), API_ID, API_HASH)
    await reader.connect()
    # CRITICAL: populate this client's entity cache (id -> access_hash) by loading
    # its dialogs. Without this, Telegram channels/supergroups (IDs starting -100)
    # cannot be resolved by this fresh client instance, and forwarding/sending to
    # them fails silently — this was why brand-new rules never forwarded even once.
    try:
        await reader.get_dialogs(limit=None)
    except Exception as e:
        print(f"[reader] warning: could not preload dialogs for {phone}: {e}")
    READERS[phone] = reader
    return reader


async def _get_bot_client(bot_token: str) -> TelegramClient:
    if bot_token in BOT_CLIENTS:
        return BOT_CLIENTS[bot_token]
    bot = TelegramClient(StringSession(), API_ID, API_HASH)
    await bot.start(bot_token=bot_token)
    try:
        await bot.get_dialogs(limit=None)
    except Exception as e:
        print(f"[bot] warning: could not preload dialogs: {e}")
    BOT_CLIENTS[bot_token] = bot
    return bot


async def _get_target_label(client: TelegramClient, target_id: int) -> str:
    """
    Returns a label for the TARGET chat itself — its title, plus a t.me link if
    it's public — so a forwarded message can show "posted in <this channel>"
    instead of "Forwarded from <original source>". Cached since it rarely changes.
    """
    if target_id in TARGET_LABEL_CACHE:
        return TARGET_LABEL_CACHE[target_id]
    try:
        entity = await client.get_entity(target_id)
        title = getattr(entity, "title", None) or getattr(entity, "first_name", None) or "Channel"
        username = getattr(entity, "username", None)
        label = f"{title}\nhttps://t.me/{username}" if username else title
    except Exception as e:
        print(f"[target label lookup failed] target={target_id}: {e}")
        label = ""
    TARGET_LABEL_CACHE[target_id] = label
    return label


async def _get_sender_display_name(sender_client: TelegramClient) -> str:
    """Returns a human-readable label for whichever account/bot is doing the sending."""
    me = await sender_client.get_me()
    if getattr(me, "username", None):
        return f"@{me.username}"
    first = getattr(me, "first_name", "") or ""
    last = getattr(me, "last_name", "") or ""
    return (first + " " + last).strip() or "Unknown"


def _compose_final_text(rule: dict, attribution: str | None, text: str) -> str:
    """Builds the outgoing message: attribution line, then header, then the
    cleaned + find/replace-processed original text, then footer — each optional."""
    text = _apply_cleaner(text, rule.get("cleaner_options"))
    text = _apply_replacements(text, rule.get("replace_rules"))
    parts = []
    if attribution:
        parts.append(attribution)
    if rule.get("header_text"):
        parts.append(rule["header_text"])
    parts.append(text)
    if rule.get("footer_text"):
        parts.append(rule["footer_text"])
    return "\n\n".join(p for p in parts if p)


async def _deliver_message(rule: dict, reader: TelegramClient, sender_client: TelegramClient,
                            event, target_id: int, text: str, attribution: str | None):
    """
    Sends the message to target_id either as:
    - a native Telegram forward (keeps "Forwarded from <original source>" tag), or
    - a fresh copy if rule['hide_source'] is True, sender is a bot, or a header/footer/
      replace rule/cleaner option is configured (native forwards can't be edited, so
      any of these force copy mode).
    """
    use_copy_mode = (
        rule["sender"] == "bot"
        or rule.get("hide_source", False)
        or bool(rule.get("header_text"))
        or bool(rule.get("footer_text"))
        or bool(rule.get("replace_rules"))
        or bool(rule.get("cleaner_options") and any(rule["cleaner_options"].values()))
    )

    if use_copy_mode:
        final_text = _compose_final_text(rule, attribution, text)
        if event.message.media:
            await sender_client.send_file(target_id, event.message.media, caption=final_text)
        else:
            await sender_client.send_message(target_id, final_text)
    else:
        await reader.forward_messages(target_id, event.message)


async def _start_rule_listener(rule_id: int):
    rule = RULES.get(rule_id)
    if not rule:
        return
    phone = rule["phone"]

    reader = await _get_reader(phone)
    sender_client = reader
    if rule["sender"] == "bot":
        sender_client = await _get_bot_client(rule["bot_token"])

    # Resolve the attribution text once if it doesn't depend on the target:
    # a custom label always wins; otherwise "sender" mode auto-detects the
    # sending account/bot's own name up front. "target" mode is resolved
    # per-target inside the handler below, since each target has its own name.
    attribution = rule.get("attribution_label")
    if not attribution and rule.get("hide_source") and rule.get("attribution_mode", "sender") == "sender":
        try:
            attribution = "via " + await _get_sender_display_name(sender_client)
        except Exception as e:
            print(f"[attribution lookup failed] rule_id={rule_id}: {e}")
            attribution = None

    @reader.on(events.NewMessage(chats=rule["source_ids"]))
    async def handler(event, rule_id=rule_id):
        current = RULES.get(rule_id)
        if not current or not current["running"]:
            return
        text = event.message.message or ""
        if not _matches_keywords(text, current["keywords"]):
            return

        if current.get("duplicate_filter"):
            content_hash = _content_hash(event)
            if _is_duplicate(rule_id, content_hash):
                print(f"[duplicate skipped] rule_id={rule_id} hash={content_hash}")
                return
            _mark_seen(rule_id, content_hash)

        for target_id in current["target_ids"]:
            try:
                target_attribution = attribution
                if (not current.get("attribution_label")
                        and current.get("hide_source")
                        and current.get("attribution_mode", "sender") == "target"):
                    target_attribution = await _get_target_label(sender_client, target_id)
                await _deliver_message(current, reader, sender_client, event, target_id, text, target_attribution)
                await _log_history(rule_id, phone, event.chat_id, target_id, text)
            except Exception as e:
                print(f"[forward error] rule_id={rule_id} target={target_id}: {e}")

    print(f"[listener] rule_id={rule_id} phone={phone} sources={rule['source_ids']} "
          f"hide_source={rule.get('hide_source')} attribution={attribution!r} keywords={rule['keywords']}")
    while RULES.get(rule_id, {}).get("running"):
        await asyncio.sleep(1)

    print(f"[listener] rule_id={rule_id} stopped.")
