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
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.errors import SessionPasswordNeededError

app = FastAPI(title="Telegram Auto-Forwarder")

# ---- Config ----
# Get these for free at https://my.telegram.org -> API Development Tools
API_ID = int(os.environ.get("38237652", "0"))
API_HASH = os.environ.get("626e9d50a55694d91de36bf7240f4894", "")

# In-memory store for demo purposes only.
# In production: put this in a real database (Postgres/SQLite) and ENCRYPT session strings.
# session string = full access to a user's Telegram account. Treat it like a password.
SESSIONS = {}          # phone -> Telethon client instance (during login)
USER_SESSIONS = {}      # phone -> saved session string (after login)
FORWARD_TASKS = {}      # phone -> {"source_ids": [...], "target_ids": [...], "sender": "account"|"bot", "bot_token": str, "running": bool}


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
    sent = await client.send_code_request(req.phone)
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
        await client.sign_in(password=req.password)

    session_string = client.session.save()
    USER_SESSIONS[req.phone] = session_string
    await client.disconnect()
    del SESSIONS[req.phone]

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
        dialogs.append({
            "id": d.id,
            "name": d.name,
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

    asyncio.create_task(_run_forward_listener(req.phone))
    return {"message": "Forwarding started.", "source_ids": req.source_ids, "target_ids": req.target_ids}


@app.post("/forward/stop")
async def stop_forwarding(phone: str):
    task = FORWARD_TASKS.get(phone)
    if not task:
        raise HTTPException(404, "No active forwarding task for this phone.")
    task["running"] = False
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
