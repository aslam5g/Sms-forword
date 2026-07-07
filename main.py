"""
Telegram Auto-Forwarder Backend
--------------------------------
Flow:
1. User logs in with their own Telegram account (phone number + OTP) using Telethon.
2. We list all their dialogs (groups/channels).
3. User picks source(s) and target(s).
4. User picks sender type: their own account, or a bot (bot must be admin in target).
5. A background listener forwards new messages from source -> target.

Run locally:
    pip install telethon fastapi uvicorn python-multipart
    uvicorn main:app --reload

Deploy on Render exactly like your Instagram downloader:
    - Add a `requirements.txt` (included)
    - Start command: uvicorn main:app --host 0.0.0.0 --port $PORT
"""

import os
import asyncio
import asyncpg
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.errors import SessionPasswordNeededError

app = FastAPI(title="Telegram Auto-Forwarder")

# Allow the test frontend (or any frontend) to call this API from the browser.
# For real production use, replace "*" with your actual frontend's domain.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---- Config ----
# Get these for free at https://my.telegram.org -> API Development Tools
API_ID = int(os.environ.get("TG_API_ID", "0"))
API_HASH = os.environ.get("TG_API_HASH", "")

# Supabase/Postgres connection string. Set this in Render's Environment tab.
DATABASE_URL = os.environ.get("DATABASE_URL", "")

db_pool: asyncpg.Pool | None = None

# In-memory store for demo purposes only.
# In production: put this in a real database (Postgres/SQLite) and ENCRYPT session strings.
# session string = full access to a user's Telegram account. Treat it like a password.
SESSIONS = {}          # phone -> Telethon client instance (during login)
USER_SESSIONS = {}      # phone -> saved session string (after login)
FORWARD_TASKS = {}      # phone -> {"source_ids": [...], "target_ids": [...], "sender": "account"|"bot", "bot_token": str, "running": bool}


@app.on_event("startup")
async def on_startup():
    global db_pool
    if not DATABASE_URL:
        print("[warning] DATABASE_URL not set — sessions will NOT survive restarts.")
        return

    db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    async with db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS telegram_users (
                phone TEXT PRIMARY KEY,
                session_string TEXT NOT NULL,
                source_ids TEXT,
                target_ids TEXT,
                sender TEXT,
                bot_token TEXT,
                running BOOLEAN DEFAULT FALSE
            );
        """)

    # Load everything back into memory and resume any forwarding that was running.
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM telegram_users;")

    for row in rows:
        USER_SESSIONS[row["phone"]] = row["session_string"]
        if row["running"] and row["source_ids"] and row["target_ids"]:
            FORWARD_TASKS[row["phone"]] = {
                "source_ids": [int(x) for x in row["source_ids"].split(",") if x],
                "target_ids": [int(x) for x in row["target_ids"].split(",") if x],
                "sender": row["sender"] or "account",
                "bot_token": row["bot_token"],
                "running": True,
            }
            asyncio.create_task(_run_forward_listener(row["phone"]))
            print(f"[resume] auto-resumed forwarding for phone={row['phone']}")

    print(f"[startup] loaded {len(rows)} saved user(s) from database.")


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


async def _save_forward_task(phone: str, task: dict):
    if not db_pool:
        return
    async with db_pool.acquire() as conn:
        await conn.execute("""
            UPDATE telegram_users
            SET source_ids = $2, target_ids = $3, sender = $4, bot_token = $5, running = $6
            WHERE phone = $1;
        """,
            phone,
            ",".join(str(x) for x in task["source_ids"]),
            ",".join(str(x) for x in task["target_ids"]),
            task["sender"],
            task["bot_token"],
            task["running"],
        )


async def _mark_forward_stopped(phone: str):
    if not db_pool:
        return
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE telegram_users SET running = FALSE WHERE phone = $1;", phone)


@app.get("/health")
async def health():
    """
    Lightweight endpoint for uptime pingers (UptimeRobot, cron-job.org, etc).
    Pinging this every 10-14 minutes keeps the free Render instance from
    spinning down, which also keeps active forward listeners alive.
    """
    return {"status": "ok", "active_forwards": len(FORWARD_TASKS), "db_connected": db_pool is not None}


# ---------- Models ----------
class PhoneRequest(BaseModel):
    phone: str


class CodeVerifyRequest(BaseModel):
    phone: str
    code: str
    phone_code_hash: str
    password: str | None = None  # only needed if user has 2FA enabled


class ForwardSetupRequest(BaseModel):
    phone: str
    source_ids: list[int]
    target_ids: list[int]
    sender: str  # "account" or "bot"
    bot_token: str | None = None


# ---------- Helpers ----------
def _require_config():
    if not API_ID or not API_HASH:
        raise HTTPException(500, "Server missing TG_API_ID / TG_API_HASH env vars.")


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
    SESSIONS[req.phone] = client  # keep connection open until verified
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

    return {"message": "Login successful.", "session_saved": True}


# ---------- 3. List dialogs (groups/channels) ----------
@app.get("/dialogs")
async def list_dialogs(phone: str):
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


# ---------- 4. Setup + start forwarding ----------
@app.post("/forward/start")
async def start_forwarding(req: ForwardSetupRequest):
    session_string = USER_SESSIONS.get(req.phone)
    if not session_string:
        raise HTTPException(401, "Not logged in.")

    if req.sender == "bot" and not req.bot_token:
        raise HTTPException(400, "bot_token required when sender='bot'.")

    FORWARD_TASKS[req.phone] = {
        "source_ids": req.source_ids,
        "target_ids": req.target_ids,
        "sender": req.sender,
        "bot_token": req.bot_token,
        "running": True,
    }
    await _save_forward_task(req.phone, FORWARD_TASKS[req.phone])

    asyncio.create_task(_run_forward_listener(req.phone))
    return {"message": "Forwarding started.", "source_ids": req.source_ids, "target_ids": req.target_ids}


@app.post("/forward/stop")
async def stop_forwarding(phone: str):
    task = FORWARD_TASKS.get(phone)
    if not task:
        raise HTTPException(404, "No active forwarding task for this phone.")
    task["running"] = False
    await _mark_forward_stopped(phone)
    return {"message": "Forwarding stopped."}


# ---------- Background listener ----------
async def _run_forward_listener(phone: str):
    """
    Reads new messages from source dialogs (using the user's own account,
    since public/private dialogs the user is already a member of are readable
    without needing a bot to be added) and forwards them either via the same
    account or via a bot, into the target dialogs.
    """
    task = FORWARD_TASKS[phone]
    session_string = USER_SESSIONS[phone]

    reader = TelegramClient(StringSession(session_string), API_ID, API_HASH)
    await reader.connect()

    sender_client = reader
    if task["sender"] == "bot":
        sender_client = TelegramClient(StringSession(), API_ID, API_HASH)
        await sender_client.start(bot_token=task["bot_token"])

    @reader.on(events.NewMessage(chats=task["source_ids"]))
    async def handler(event):
        if not FORWARD_TASKS.get(phone, {}).get("running"):
            return
        for target_id in task["target_ids"]:
            try:
                if task["sender"] == "bot":
                    # Bot must already be an admin/member of the target chat.
                    await sender_client.send_message(target_id, event.message.message)
                else:
                    await reader.forward_messages(target_id, event.message)
            except Exception as e:
                print(f"[forward error] phone={phone} target={target_id}: {e}")

    print(f"Listening for phone={phone} on sources={task['source_ids']}")
    while FORWARD_TASKS.get(phone, {}).get("running"):
        await asyncio.sleep(1)

    await reader.disconnect()
    if task["sender"] == "bot":
        await sender_client.disconnect()
