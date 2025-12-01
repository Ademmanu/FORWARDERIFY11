#!/usr/bin/env python3
"""
Full integrated forward.py with interactive /forwadd, emojified task menus, persistent per-task settings
and fixed handler registrations (no lambda wrappers). This file includes:
 - multi-step /forwadd (name -> sources -> targets)
 - /fortasks shows inline buttons for each task
 - Task menu with Filters, Outgoing, Forward Tag, Control (toggles), Delete flow
 - Filters submenu with Raw, Numbers only, Alphabets only, Removed Alphabetic, Removed Numeric
 - Prefix/Suffix set flow
 - Persistent task settings in DB via settings_json (database.py must support update_task_settings)
 - Proper command handler registrations: getallid_command, adduser_command, removeuser_command, listusers_command
 - Robust shutdown_cleanup guarding against closed event loop errors
"""
import os
import asyncio
import logging
import functools
import gc
from typing import Dict, List, Optional, Tuple, Set, Callable
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.errors import SessionPasswordNeededError, FloodWaitError
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    filters,
)
from database import Database
from webserver import start_server_thread, register_monitoring

# Basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("forward")

# Environment configs
BOT_TOKEN = os.getenv("BOT_TOKEN")
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")

OWNER_IDS: Set[int] = set()
owner_env = os.getenv("OWNER_IDS", "").strip()
if owner_env:
    for part in owner_env.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            OWNER_IDS.add(int(part))
        except ValueError:
            logger.warning("Invalid OWNER_IDS value skipped: %s", part)

ALLOWED_USERS: Set[int] = set()
allowed_env = os.getenv("ALLOWED_USERS", "").strip()
if allowed_env:
    for part in allowed_env.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            ALLOWED_USERS.add(int(part))
        except ValueError:
            logger.warning("Invalid ALLOWED_USERS value skipped: %s", part)

# Tunables
SEND_WORKER_COUNT = int(os.getenv("SEND_WORKER_COUNT", "15"))
SEND_QUEUE_MAXSIZE = int(os.getenv("SEND_QUEUE_MAXSIZE", "10000"))
TARGET_RESOLVE_RETRY_SECONDS = int(os.getenv("TARGET_RESOLVE_RETRY_SECONDS", "30"))
MAX_CONCURRENT_USERS = int(os.getenv("MAX_CONCURRENT_USERS", "50"))
MESSAGE_PROCESS_BATCH_SIZE = int(os.getenv("MESSAGE_PROCESS_BATCH_SIZE", "5"))

# DB
db = Database()

# In-memory runtime state
user_clients: Dict[int, TelegramClient] = {}
login_states: Dict[int, Dict] = {}
logout_states: Dict[int, Dict] = {}
tasks_cache: Dict[int, List[Dict]] = {}
target_entity_cache: Dict[int, Dict[int, object]] = {}
handler_registered: Dict[int, Callable] = {}

send_queue: Optional[asyncio.Queue[Tuple[int, TelegramClient, int, str]]] = None
worker_tasks: List[asyncio.Task] = []
_send_workers_started = False

MAIN_LOOP: Optional[asyncio.AbstractEventLoop] = None
_last_gc_run = 0
GC_INTERVAL = 300

UNAUTHORIZED_MESSAGE = """🚫 Access Denied!

You are not authorized to use this bot.

📞 Call this number: 07089430305

Or

🗨️ Message Developer: https://t.me/justmemmy
"""

# Task settings defaults (On by default for outgoing/forward_tag/control)
TASK_SETTINGS_DEFAULT = {
    "filter_raw": False,
    "filter_numbers": False,
    "filter_alpha": False,
    "filter_remove_alpha": False,
    "filter_remove_numeric": False,
    "filter_prefix": None,
    "filter_suffix": None,
    "outgoing": True,
    "forward_tag": True,
    "control": True,
}

# forwadd steps
FORWADD_STATE_NAME = 1
FORWADD_STATE_SOURCES = 2
FORWADD_STATE_TARGETS = 3
forwadd_states: Dict[int, Dict] = {}
task_settings_state: Dict[int, Dict] = {}

# Helper to run blocking DB calls off the loop
async def db_call(func, *args, **kwargs):
    return await asyncio.to_thread(functools.partial(func, *args, **kwargs))


async def optimized_gc():
    global _last_gc_run
    current_time = asyncio.get_event_loop().time()
    if current_time - _last_gc_run > GC_INTERVAL:
        collected = gc.collect()
        logger.debug("Garbage collection freed %s objects", collected)
        _last_gc_run = current_time


# ---------- Authorization ----------
async def check_authorization(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    user_id = update.effective_user.id
    try:
        is_allowed_db = await db_call(db.is_user_allowed, user_id)
    except Exception:
        logger.exception("Error checking DB allowed users for %s", user_id)
        is_allowed_db = False
    is_allowed_env = (user_id in ALLOWED_USERS) or (user_id in OWNER_IDS)
    if not (is_allowed_db or is_allowed_env):
        if update.message:
            await update.message.reply_text(UNAUTHORIZED_MESSAGE, disable_web_page_preview=True)
        elif update.callback_query:
            await update.callback_query.answer()
            await update.callback_query.message.reply_text(UNAUTHORIZED_MESSAGE, disable_web_page_preview=True)
        return False
    return True


# ---------- Simple UI ----------

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not await check_authorization(update, context):
        return
    user = await db_call(db.get_user, user_id)
    user_name = update.effective_user.first_name or "User"
    user_phone = user["phone"] if user and user["phone"] else "Not connected"
    is_logged_in = user and user["is_logged_in"]
    status_emoji = "🟢" if is_logged_in else "🔴"
    status_text = "Online" if is_logged_in else "Offline"
    message_text = f"""
╔═══════════════════════════╗
║   📨 FORWARDER BOT 📨   ║
╚═══════════════════════════╝

👤 User: {user_name}
📱 Phone: {user_phone}
{status_emoji} Status: {status_text}

📋 COMMANDS:
  /login - Connect your Telegram account
  /logout - Disconnect your account
  /forwadd - Create a forwarding task
  /fortasks - List your tasks
  /getallid - Get chat IDs
"""
    keyboard = []
    if is_logged_in:
        keyboard.append([InlineKeyboardButton("📋 My Tasks", callback_data="show_tasks")])
        keyboard.append([InlineKeyboardButton("🔴 Disconnect", callback_data="logout")])
    else:
        keyboard.append([InlineKeyboardButton("🟢 Connect Account", callback_data="login")])
    await update.message.reply_text(message_text, reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None)


# ---------- Login/Logout flows ----------

async def login_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id if update.effective_user else update.callback_query.from_user.id
    if not await check_authorization(update, context):
        return
    message = update.message if update.message else update.callback_query.message
    if len(user_clients) >= MAX_CONCURRENT_USERS:
        await message.reply_text("❌ Server at capacity! Try again later.")
        return
    client = TelegramClient(StringSession(), API_ID, API_HASH)
    await client.connect()
    login_states[user_id] = {"client": client, "step": "waiting_phone"}
    await message.reply_text("📱 Enter your phone number (with country code). Example: +1234567890")

async def handle_login_process(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id in logout_states:
        handled = await handle_logout_confirmation(update, context)
        if handled:
            return
    if user_id not in login_states:
        return
    state = login_states[user_id]
    text = update.message.text.strip()
    client = state["client"]
    try:
        if state["step"] == "waiting_phone":
            msg = await update.message.reply_text("⏳ Requesting code...")
            result = await client.send_code_request(text)
            state["phone"] = text
            state["phone_code_hash"] = result.phone_code_hash
            state["step"] = "waiting_code"
            await msg.edit_text("✅ Code sent. Reply with: verify12345")
        elif state["step"] == "waiting_code":
            if not text.startswith("verify"):
                await update.message.reply_text("❌ Use format: verify12345")
                return
            code = text[6:]
            verifying_msg = await update.message.reply_text("🔄 Verifying...")
            try:
                await client.sign_in(state["phone"], code, phone_code_hash=state["phone_code_hash"])
                me = await client.get_me()
                session_string = client.session.save()
                await db_call(db.save_user, user_id, state["phone"], me.first_name, session_string, True)
                user_clients[user_id] = client
                tasks_cache.setdefault(user_id, [])
                target_entity_cache.setdefault(user_id, {})
                await start_forwarding_for_user(user_id)
                del login_states[user_id]
                await verifying_msg.edit_text(f"✅ Connected as {me.first_name}. You can create tasks with /forwadd")
            except SessionPasswordNeededError:
                state["step"] = "waiting_2fa"
                await verifying_msg.edit_text("🔐 2FA required. Reply with: passwordYourPassword123")
        elif state["step"] == "waiting_2fa":
            if not text.startswith("password"):
                await update.message.reply_text("❌ Use format: passwordYourPassword123")
                return
            password = text[8:]
            verifying_msg = await update.message.reply_text("🔄 Verifying 2FA...")
            await client.sign_in(password=password)
            me = await client.get_me()
            session_string = client.session.save()
            await db_call(db.save_user, user_id, state["phone"], me.first_name, session_string, True)
            user_clients[user_id] = client
            tasks_cache.setdefault(user_id, [])
            target_entity_cache.setdefault(user_id, {})
            await start_forwarding_for_user(user_id)
            del login_states[user_id]
            await verifying_msg.edit_text("✅ Connected with 2FA. You can create tasks now.")
    except Exception as e:
        logger.exception("Error during login process for %s", user_id)
        await update.message.reply_text(f"❌ Login error: {e}")
        if user_id in login_states:
            try:
                c = login_states[user_id].get("client")
                if c:
                    await c.disconnect()
            except Exception:
                logger.exception("Error disconnecting client after failed login for %s", user_id)
            del login_states[user_id]


async def logout_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id if update.effective_user else update.callback_query.from_user.id
    if not await check_authorization(update, context):
        return
    message = update.message if update.message else update.callback_query.message
    user = await db_call(db.get_user, user_id)
    if not user or not user["is_logged_in"]:
        await message.reply_text("❌ You're not connected. Use /login.")
        return
    logout_states[user_id] = {"phone": user["phone"]}
    await message.reply_text(f"⚠️ Confirm logout. Type your phone number: {user['phone']}")


async def handle_logout_confirmation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    user_id = update.effective_user.id
    if user_id not in logout_states:
        return False
    text = update.message.text.strip()
    stored_phone = logout_states[user_id]["phone"]
    if text != stored_phone:
        await update.message.reply_text("❌ Phone doesn't match. Try /start to cancel.")
        return True
    if user_id in user_clients:
        client = user_clients[user_id]
        try:
            handler = handler_registered.get(user_id)
            if handler:
                try:
                    client.remove_event_handler(handler)
                except Exception:
                    logger.exception("Error removing handler for user %s", user_id)
                handler_registered.pop(user_id, None)
            await client.disconnect()
        except Exception:
            logger.exception("Error disconnecting client for user %s", user_id)
        finally:
            user_clients.pop(user_id, None)
    try:
        await db_call(db.save_user, user_id, None, None, None, False)
    except Exception:
        logger.exception("Error saving logout state for %s", user_id)
    tasks_cache.pop(user_id, None)
    target_entity_cache.pop(user_id, None)
    logout_states.pop(user_id, None)
    await update.message.reply_text("👋 Disconnected. Use /login to connect again.")
    return True


# ---------- /forwadd interactive flow ----------
async def forwadd_command_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not await check_authorization(update, context):
        return
    user = await db_call(db.get_user, user_id)
    if not user or not user["is_logged_in"]:
        await update.message.reply_text("❌ Connect your account first with /login.")
        return
    forwadd_states[user_id] = {"step": FORWADD_STATE_NAME}
    await update.message.reply_text("🎯 Task creation: Send the name for the new task.")


async def handle_forwadd_steps(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in forwadd_states:
        return
    state = forwadd_states[user_id]
    text = update.message.text.strip()
    if state["step"] == FORWADD_STATE_NAME:
        if not text:
            await update.message.reply_text("Please enter a name.")
            return
        state["name"] = text
        state["step"] = FORWADD_STATE_SOURCES
        await update.message.reply_text(f"Name set to '{text}'. Now enter source chat ID(s) separated by spaces. Use /getallid to list IDs.")
    elif state["step"] == FORWADD_STATE_SOURCES:
        try:
            sources = [int(x) for x in text.replace(",", " ").split() if x.strip()]
            if not sources:
                raise ValueError
            state["sources"] = sources
            state["step"] = FORWADD_STATE_TARGETS
            await update.message.reply_text(f"Sources: {', '.join(map(str, sources))}. Now enter target ID(s).")
        except Exception:
            await update.message.reply_text("Please enter valid numeric source IDs.")
    elif state["step"] == FORWADD_STATE_TARGETS:
        try:
            targets = [int(x) for x in text.replace(",", " ").split() if x.strip()]
            if not targets:
                raise ValueError
            name = state["name"]
            sources = state["sources"]
            # Try DB signature with settings
            try:
                added = await db_call(db.add_forwarding_task, user_id, name, sources, targets, TASK_SETTINGS_DEFAULT.copy())
            except TypeError:
                added = await db_call(db.add_forwarding_task, user_id, name, sources, targets)
            if added:
                tasks_cache.setdefault(user_id, [])
                tasks_cache[user_id].append({
                    "id": None,
                    "label": name,
                    "source_ids": sources,
                    "target_ids": targets,
                    "is_active": 1,
                    "settings": TASK_SETTINGS_DEFAULT.copy(),
                })
                try:
                    asyncio.create_task(resolve_targets_for_user(user_id, targets))
                except Exception:
                    logger.exception("resolve_targets_for_user scheduling failed")
                await update.message.reply_text(f"🎉 Task '{name}' created! Sources: {sources} Targets: {targets}")
            else:
                await update.message.reply_text(f"❌ Task '{name}' already exists.")
            del forwadd_states[user_id]
        except Exception:
            await update.message.reply_text("Please enter valid numeric target IDs.")


# ---------- /fortasks: show tasks as inline buttons ----------
async def fortasks_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id if update.effective_user else update.callback_query.from_user.id
    if not await check_authorization(update, context):
        return
    message = update.message if update.message else update.callback_query.message
    # Ensure tasks_cache has DB tasks
    if not tasks_cache.get(user_id):
        try:
            db_tasks = await db_call(db.get_user_tasks, user_id)
            tasks_cache[user_id] = []
            for t in db_tasks:
                tasks_cache[user_id].append({
                    "id": t["id"],
                    "label": t["label"],
                    "source_ids": t["source_ids"],
                    "target_ids": t["target_ids"],
                    "is_active": t.get("is_active", 1),
                    "settings": t.get("settings", TASK_SETTINGS_DEFAULT.copy()) if isinstance(t.get("settings", None), dict) else TASK_SETTINGS_DEFAULT.copy(),
                })
        except Exception:
            logger.exception("Failed loading tasks for user %s", user_id)
    tasks = tasks_cache.get(user_id) or []
    if not tasks:
        await message.reply_text("🗂️ You have no tasks. Create one with /forwadd")
        return
    keyboard = []
    for i, task in enumerate(tasks, 1):
        keyboard.append([InlineKeyboardButton(f"{i}. {task['label']}", callback_data=f"taskmenu_{task['label']}")])
    header = "🗂️ Your Forwarding Tasks\nTap a task to manage it."
    footer = f"\nTotal: {len(tasks)}"
    await message.reply_text(header + footer, reply_markup=InlineKeyboardMarkup(keyboard))


# ---------- getallid - fetch and display chat ids ----------
async def getallid_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not await check_authorization(update, context):
        return
    user = await db_call(db.get_user, user_id)
    if not user or not user["is_logged_in"]:
        await update.message.reply_text("❌ Connect first with /login")
        return
    await update.message.reply_text("🔄 Fetching your chats...")
    await show_chat_categories(user_id, update.message.chat.id, None, context)


# ---------- Admin commands ----------
async def adduser_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not await check_authorization(update, context):
        return
    is_admin_caller = await db_call(db.is_user_admin, user_id)
    if not is_admin_caller:
        await update.message.reply_text("❌ Admin only")
        return
    text = update.message.text.strip()
    parts = text.split()
    if len(parts) < 2:
        await update.message.reply_text("Usage: /adduser [USER_ID] [admin?]")
        return
    try:
        new_id = int(parts[1])
        is_admin = len(parts) > 2 and parts[2].lower() == "admin"
        added = await db_call(db.add_allowed_user, new_id, None, is_admin, user_id)
        if added:
            await update.message.reply_text(f"✅ Added user {new_id} as {'admin' if is_admin else 'user'}")
            try:
                await context.bot.send_message(new_id, "✅ You have been added. Send /start to begin.")
            except Exception:
                logger.exception("Notify new user failed")
        else:
            await update.message.reply_text("❌ User already exists")
    except ValueError:
        await update.message.reply_text("User ID must be numeric")


async def removeuser_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not await check_authorization(update, context):
        return
    is_admin_caller = await db_call(db.is_user_admin, user_id)
    if not is_admin_caller:
        await update.message.reply_text("❌ Admin only")
        return
    text = update.message.text.strip()
    parts = text.split()
    if len(parts) < 2:
        await update.message.reply_text("Usage: /removeuser [USER_ID]")
        return
    try:
        rem_id = int(parts[1])
        removed = await db_call(db.remove_allowed_user, rem_id)
        if removed:
            # Best-effort disconnect user's client
            if rem_id in user_clients:
                try:
                    client = user_clients[rem_id]
                    handler = handler_registered.get(rem_id)
                    if handler:
                        try:
                            client.remove_event_handler(handler)
                        except Exception:
                            logger.exception("Error removing handler")
                        handler_registered.pop(rem_id, None)
                    await client.disconnect()
                except Exception:
                    logger.exception("Error disconnecting removed user")
                finally:
                    user_clients.pop(rem_id, None)
            await db_call(db.save_user, rem_id, None, None, None, False)
            tasks_cache.pop(rem_id, None)
            target_entity_cache.pop(rem_id, None)
            await update.message.reply_text(f"✅ Removed user {rem_id}")
            try:
                await context.bot.send_message(rem_id, "❌ You have been removed.")
            except Exception:
                logger.exception("Notify removed user failed")
        else:
            await update.message.reply_text("❌ User not found")
    except ValueError:
        await update.message.reply_text("User ID must be numeric")


async def listusers_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not await check_authorization(update, context):
        return
    is_admin_caller = await db_call(db.is_user_admin, user_id)
    if not is_admin_caller:
        await update.message.reply_text("❌ Admin only")
        return
    users = await db_call(db.get_all_allowed_users)
    if not users:
        await update.message.reply_text("No allowed users")
        return
    lines = []
    for u in users:
        role = "Admin" if u["is_admin"] else "User"
        lines.append(f"{u['user_id']} — {role}")
    await update.message.reply_text("👥 Allowed users:\n" + "\n".join(lines))


# ---------- Chat listing (getallid helpers) ----------
async def show_chat_categories(user_id: int, chat_id: int, message_id: int, context: ContextTypes.DEFAULT_TYPE):
    if user_id not in user_clients:
        return
    message_text = (
        "🗂️ Chat ID Categories\n\n"
        "Choose which type of chat IDs you want to see:"
    )
    keyboard = [
        [InlineKeyboardButton("🤖 Bots", callback_data="chatids_bots_0"), InlineKeyboardButton("📢 Channels", callback_data="chatids_channels_0")],
        [InlineKeyboardButton("👥 Groups", callback_data="chatids_groups_0"), InlineKeyboardButton("👤 Private", callback_data="chatids_private_0")],
    ]
    try:
        if message_id:
            await context.bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=message_text, reply_markup=InlineKeyboardMarkup(keyboard))
        else:
            await context.bot.send_message(chat_id=chat_id, text=message_text, reply_markup=InlineKeyboardMarkup(keyboard))
    except Exception:
        try:
            await context.bot.send_message(chat_id=chat_id, text=message_text, reply_markup=InlineKeyboardMarkup(keyboard))
        except Exception:
            logger.exception("Failed to send chat categories")


async def show_categorized_chats(user_id: int, chat_id: int, message_id: int, category: str, page: int, context: ContextTypes.DEFAULT_TYPE):
    from telethon.tl.types import User, Channel, Chat
    if user_id not in user_clients:
        return
    client = user_clients[user_id]
    categorized_dialogs = []
    async for dialog in client.iter_dialogs():
        ent = dialog.entity
        if category == "bots":
            if isinstance(ent, User) and ent.bot:
                categorized_dialogs.append(dialog)
        elif category == "channels":
            if isinstance(ent, Channel) and getattr(ent, "broadcast", False):
                categorized_dialogs.append(dialog)
        elif category == "groups":
            if isinstance(ent, (Channel, Chat)) and not (isinstance(ent, Channel) and getattr(ent, "broadcast", False)):
                categorized_dialogs.append(dialog)
        elif category == "private":
            if isinstance(ent, User) and not ent.bot:
                categorized_dialogs.append(dialog)
    PAGE_SIZE = 10
    total_pages = max(1, (len(categorized_dialogs) + PAGE_SIZE - 1) // PAGE_SIZE)
    start = page * PAGE_SIZE
    page_dialogs = categorized_dialogs[start:start + PAGE_SIZE]
    category_emoji = {"bots": "🤖", "channels": "📢", "groups": "👥", "private": "👤"}
    category_name = {"bots": "Bots", "channels": "Channels", "groups": "Groups", "private": "Private Chats"}
    emoji = category_emoji.get(category, "💬")
    name = category_name.get(category, "Chats")
    if not categorized_dialogs:
        chat_list = f"{emoji} {name}\n\nNo {name.lower()} found!"
    else:
        chat_list = f"{emoji} {name} (Page {page + 1}/{total_pages})\n\n"
        for i, dialog in enumerate(page_dialogs, start + 1):
            chat_list += f"{i}. {dialog.name or 'Unknown'}\n   ID: {dialog.id}\n\n"
        chat_list += f"📊 Total: {len(categorized_dialogs)}\n"
    keyboard = []
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"chatids_{category}_{page - 1}"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"chatids_{category}_{page + 1}"))
    if nav:
        keyboard.append(nav)
    keyboard.append([InlineKeyboardButton("🔙 Back to Categories", callback_data="chatids_back")])
    try:
        if message_id:
            await context.bot.edit_message_text(chat_list, chat_id=chat_id, message_id=message_id, reply_markup=InlineKeyboardMarkup(keyboard))
        else:
            await context.bot.send_message(chat_id=chat_id, text=chat_list, reply_markup=InlineKeyboardMarkup(keyboard))
    except Exception:
        try:
            await context.bot.send_message(chat_id=chat_id, text=chat_list, reply_markup=InlineKeyboardMarkup(keyboard))
        except Exception:
            logger.exception("Failed to send categorized chats")


# ---------- Forwarding core ----------
def ensure_handler_registered_for_user(user_id: int, client: TelegramClient):
    if handler_registered.get(user_id):
        return
    async def _hot_message_handler(event):
        try:
            await optimized_gc()
            message_text = getattr(event, "raw_text", None) or getattr(getattr(event, "message", None), "message", None)
            if not message_text:
                return
            chat_id = getattr(event, "chat_id", None) or getattr(getattr(event, "message", None), "chat_id", None)
            if chat_id is None:
                return
            user_tasks = tasks_cache.get(user_id)
            if not user_tasks:
                return
            for task in user_tasks:
                if chat_id in task.get("source_ids", []):
                    settings = task.get("settings", {}) or {}
                    if settings.get("control") is False:
                        continue
                    is_outgoing = getattr(event, "out", False)
                    if not settings.get("outgoing", True) and is_outgoing:
                        continue
                    outputs = apply_filters_to_text(message_text, settings)
                    for target_id in task.get("target_ids", []):
                        for out_text in outputs:
                            try:
                                global send_queue
                                if send_queue is None:
                                    logger.debug("Send queue not initialized; dropping forward job")
                                    continue
                                await send_queue.put((user_id, client, int(target_id), out_text))
                            except asyncio.QueueFull:
                                logger.warning("Send queue full, dropping forward job")
        except Exception:
            logger.exception("Error in message handler for user %s", user_id)
    try:
        client.add_event_handler(_hot_message_handler, events.NewMessage())
        handler_registered[user_id] = _hot_message_handler
        logger.info("Registered NewMessage handler for user %s", user_id)
    except Exception:
        logger.exception("Failed to add event handler for user %s", user_id)


def apply_filters_to_text(message_text: str, settings: Dict) -> List[str]:
    text = message_text or ""
    words = text.split()
    has_alpha = any(any(ch.isalpha() for ch in w) for w in words) if words else False
    has_digit = any(any(ch.isdigit() for ch in w) for w in words) if words else False
    results: List[str] = []
    # Raw
    if settings.get("filter_raw"):
        prefix = settings.get("filter_prefix") or ""
        suffix = settings.get("filter_suffix") or ""
        results.append(f"{prefix}{text}{suffix}")
        return results
    # Numbers only
    if settings.get("filter_numbers"):
        if any(ch.isdigit() for ch in text):
            prefix = settings.get("filter_prefix") or ""
            suffix = settings.get("filter_suffix") or ""
            results.append(f"{prefix}{text}{suffix}")
        return results
    # Alphabets only
    if settings.get("filter_alpha"):
        if any(ch.isalpha() for ch in text):
            prefix = settings.get("filter_prefix") or ""
            suffix = settings.get("filter_suffix") or ""
            results.append(f"{prefix}{text}{suffix}")
        return results
    # Removed Alphabetic (forward alphabetic words from mixed)
    if settings.get("filter_remove_alpha"):
        if has_alpha and has_digit:
            for w in words:
                if any(ch.isalpha() for ch in w) and not any(ch.isdigit() for ch in w):
                    prefix = settings.get("filter_prefix") or ""
                    suffix = settings.get("filter_suffix") or ""
                    results.append(f"{prefix}{w}{suffix}")
        return results
    # Removed Numeric (forward numeric words from mixed)
    if settings.get("filter_remove_numeric"):
        if has_alpha and has_digit:
            for w in words:
                if any(ch.isdigit() for ch in w) and not any(ch.isalpha() for ch in w):
                    prefix = settings.get("filter_prefix") or ""
                    suffix = settings.get("filter_suffix") or ""
                    results.append(f"{prefix}{w}{suffix}")
        return results
    # Legacy: only forward pure numeric
    active_filters = any(settings.get(k) for k in ["filter_raw", "filter_numbers", "filter_alpha", "filter_remove_alpha", "filter_remove_numeric"])
    if not active_filters:
        if text.strip().isdigit():
            prefix = settings.get("filter_prefix") or ""
            suffix = settings.get("filter_suffix") or ""
            results.append(f"{prefix}{text.strip()}{suffix}")
    return results


async def resolve_target_entity_once(user_id: int, client: TelegramClient, target_id: int) -> Optional[object]:
    if user_id not in target_entity_cache:
        target_entity_cache[user_id] = {}
    if target_id in target_entity_cache[user_id]:
        return target_entity_cache[user_id][target_id]
    try:
        ent = await client.get_input_entity(int(target_id))
        target_entity_cache[user_id][target_id] = ent
        return ent
    except Exception:
        logger.debug("Could not resolve target %s for user %s", target_id, user_id)
        return None


async def resolve_targets_for_user(user_id: int, target_ids: List[int]):
    client = user_clients.get(user_id)
    if not client:
        return
    for tid in target_ids:
        for attempt in range(3):
            ent = await resolve_target_entity_once(user_id, client, tid)
            if ent:
                logger.info("Resolved target %s for user %s", tid, user_id)
                break
            await asyncio.sleep(TARGET_RESOLVE_RETRY_SECONDS)


async def send_worker_loop(worker_id: int):
    logger.info("Send worker %d started", worker_id)
    global send_queue
    if send_queue is None:
        logger.error("send_worker_loop started before send_queue initialized")
        return
    while True:
        try:
            user_id, client, target_id, message_text = await send_queue.get()
        except asyncio.CancelledError:
            break
        except Exception:
            logger.exception("Error getting from queue")
            break
        try:
            entity = None
            if user_id in target_entity_cache:
                entity = target_entity_cache[user_id].get(target_id)
            if not entity:
                entity = await resolve_target_entity_once(user_id, client, target_id)
            if not entity:
                logger.debug("Unresolved target %s for user %s", target_id, user_id)
                continue
            try:
                await client.send_message(entity, message_text)
                logger.debug("Forwarded message for user %s to %s", user_id, target_id)
            except FloodWaitError as fwe:
                wait = int(getattr(fwe, "seconds", 10))
                logger.warning("FloodWait %s seconds on worker %d", wait, worker_id)
                await asyncio.sleep(wait + 1)
                try:
                    await send_queue.put((user_id, client, target_id, message_text))
                except asyncio.QueueFull:
                    logger.warning("Queue full re-enqueue after FloodWait")
            except Exception:
                logger.exception("Error sending message")
        except Exception:
            logger.exception("Unexpected worker error")
        finally:
            try:
                send_queue.task_done()
            except Exception:
                pass


async def start_send_workers():
    global _send_workers_started, send_queue, worker_tasks
    if _send_workers_started:
        return
    if send_queue is None:
        send_queue = asyncio.Queue(maxsize=SEND_QUEUE_MAXSIZE)
    for i in range(SEND_WORKER_COUNT):
        t = asyncio.create_task(send_worker_loop(i + 1))
        worker_tasks.append(t)
    _send_workers_started = True
    logger.info("Spawned %d workers", SEND_WORKER_COUNT)


async def start_forwarding_for_user(user_id: int):
    if user_id not in user_clients:
        return
    client = user_clients[user_id]
    tasks_cache.setdefault(user_id, [])
    target_entity_cache.setdefault(user_id, {})
    ensure_handler_registered_for_user(user_id, client)


# ---------- Restore sessions ----------
async def restore_sessions():
    logger.info("Restoring sessions")
    def _fetch_logged_in_users():
        conn = db.get_connection()
        cur = conn.cursor()
        cur.execute("SELECT user_id, session_data FROM users WHERE is_logged_in = 1")
        return cur.fetchall()
    try:
        users = await asyncio.to_thread(_fetch_logged_in_users)
    except Exception:
        logger.exception("fetch_logged_in_users failed")
        users = []
    try:
        all_active = await db_call(db.get_all_active_tasks)
    except Exception:
        logger.exception("get_all_active_tasks failed")
        all_active = []
    tasks_cache.clear()
    for t in all_active:
        uid = t["user_id"]
        tasks_cache.setdefault(uid, [])
        tasks_cache[uid].append({
            "id": t["id"],
            "label": t["label"],
            "source_ids": t["source_ids"],
            "target_ids": t["target_ids"],
            "is_active": 1,
            "settings": t.get("settings", TASK_SETTINGS_DEFAULT.copy())
        })
    logger.info("Found %d logged-in users", len(users))
    batch_size = 5
    for i in range(0, len(users), batch_size):
        batch = users[i:i + batch_size]
        restore_tasks = []
        for row in batch:
            try:
                user_id = row["user_id"] if isinstance(row, dict) or hasattr(row, "keys") else row[0]
                session_data = row["session_data"] if isinstance(row, dict) or hasattr(row, "keys") else row[1]
            except Exception:
                try:
                    user_id, session_data = row[0], row[1]
                except Exception:
                    continue
            if session_data:
                restore_tasks.append(restore_single_session(user_id, session_data))
        if restore_tasks:
            await asyncio.gather(*restore_tasks, return_exceptions=True)
            await asyncio.sleep(1)


async def restore_single_session(user_id: int, session_data: str):
    try:
        client = TelegramClient(StringSession(session_data), API_ID, API_HASH)
        await client.connect()
        if await client.is_user_authorized():
            user_clients[user_id] = client
            target_entity_cache.setdefault(user_id, {})
            user_tasks = tasks_cache.get(user_id, [])
            all_targets = []
            for tt in user_tasks:
                all_targets.extend(tt.get("target_ids", []))
            if all_targets:
                try:
                    asyncio.create_task(resolve_targets_for_user(user_id, list(set(all_targets))))
                except Exception:
                    logger.exception("resolve schedule error")
            await start_forwarding_for_user(user_id)
            logger.info("Restored session for user %s", user_id)
        else:
            await db_call(db.save_user, user_id, None, None, None, False)
            logger.warning("Session expired for user %s", user_id)
    except Exception as e:
        logger.exception("Failed to restore session for %s: %s", user_id, e)
        try:
            await db_call(db.save_user, user_id, None, None, None, False)
        except Exception:
            logger.exception("Failed marking logged out for %s", user_id)


# ---------- Shutdown cleanup (robust against closed loops) ----------
async def shutdown_cleanup():
    logger.info("Shutdown cleanup: cancelling worker tasks and disconnecting clients...")
    # Cancel worker tasks safely
    for t in list(worker_tasks):
        try:
            loop = None
            try:
                loop = t.get_loop()
            except Exception:
                loop = None
            if loop and getattr(loop, "is_closed", lambda: False)():
                logger.warning("Worker task loop closed; skipping cancel")
                continue
            try:
                if loop and getattr(loop, "call_soon_threadsafe", None):
                    loop.call_soon_threadsafe(t.cancel)
                else:
                    t.cancel()
            except RuntimeError:
                logger.warning("Event loop closed while cancelling task")
            except Exception:
                logger.exception("Error cancelling worker task")
        except Exception:
            logger.exception("Unexpected error cancelling worker task")
    worker_tasks.clear()
    # Disconnect Telethon clients
    user_ids = list(user_clients.keys())
    batch_size = 5
    for i in range(0, len(user_ids), batch_size):
        batch = user_ids[i:i + batch_size]
        tasks = []
        for uid in batch:
            client = user_clients.get(uid)
            if client:
                handler = handler_registered.get(uid)
                if handler:
                    try:
                        client.remove_event_handler(handler)
                    except Exception:
                        logger.exception("Error removing handler during shutdown for %s", uid)
                    handler_registered.pop(uid, None)
                tasks.append(client.disconnect())
        if tasks:
            try:
                await asyncio.gather(*tasks, return_exceptions=True)
            except Exception:
                logger.exception("Error disconnecting clients during shutdown")
    user_clients.clear()
    try:
        db.close_connection()
    except Exception:
        logger.exception("Error closing DB connection")
    logger.info("Shutdown cleanup complete.")


# ---------- post_init ----------
async def post_init(application: Application):
    global MAIN_LOOP
    MAIN_LOOP = asyncio.get_running_loop()
    logger.info("Initializing bot...")
    await application.bot.delete_webhook(drop_pending_updates=True)
    # Ensure owners/allowed users in DB
    if OWNER_IDS:
        for oid in OWNER_IDS:
            try:
                is_admin = await db_call(db.is_user_admin, oid)
                if not is_admin:
                    await db_call(db.add_allowed_user, oid, None, True, None)
                    logger.info("Added owner admin %s", oid)
            except Exception:
                logger.exception("Error adding owner %s", oid)
    if ALLOWED_USERS:
        for au in ALLOWED_USERS:
            try:
                await db_call(db.add_allowed_user, au, None, False, None)
            except Exception:
                logger.exception("Error adding allowed %s", au)
    await start_send_workers()
    await restore_sessions()
    # Register monitoring callback
    async def _collect_metrics():
        try:
            q = None
            try:
                q = send_queue.qsize() if send_queue is not None else None
            except Exception:
                q = None
            return {
                "send_queue_size": q,
                "worker_count": len(worker_tasks),
                "active_user_clients_count": len(user_clients),
                "tasks_cache_counts": {uid: len(tasks_cache.get(uid, [])) for uid in list(tasks_cache.keys())},
                "memory_usage_mb": _get_memory_usage_mb(),
            }
        except Exception as e:
            return {"error": str(e)}
    def _forward_metrics():
        global MAIN_LOOP
        if MAIN_LOOP is None:
            return {"error": "main loop not available"}
        try:
            future = asyncio.run_coroutine_threadsafe(_collect_metrics(), MAIN_LOOP)
            return future.result(timeout=1.0)
        except Exception:
            logger.exception("Failed to collect metrics")
            return {"error": "failed to collect metrics"}
    try:
        register_monitoring(_forward_metrics)
    except Exception:
        logger.exception("Failed to register monitoring")
    logger.info("Bot initialized")


def _get_memory_usage_mb():
    try:
        import psutil
        proc = psutil.Process()
        return round(proc.memory_info().rss / 1024 / 1024, 2)
    except Exception:
        return None


# ---------- Inline callback & task settings handling ----------
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not await check_authorization(update, context):
        return
    await query.answer()
    data = query.data
    user_id = query.from_user.id

    # Start/login/logout navigation
    if data == "login":
        await query.message.delete()
        await login_command(update, context)
        return
    if data == "logout":
        await query.message.delete()
        await logout_command(update, context)
        return
    if data == "show_tasks":
        await query.message.delete()
        await fortasks_command(update, context)
        return

    # chat ids navigation
    if data.startswith("chatids_"):
        if data == "chatids_back":
            await show_chat_categories(user_id, query.message.chat.id, query.message.message_id, context)
            return
        parts = data.split("_")
        if len(parts) >= 3:
            category = parts[1]
            page = int(parts[2])
            await show_categorized_chats(user_id, query.message.chat.id, query.message.message_id, category, page, context)
            return

    # Task menu
    if data.startswith("taskmenu_"):
        label = data[len("taskmenu_"):]
        task = task_get(user_id, label)
        if not task:
            await query.message.reply_text("Task not found")
            return
        if "settings" not in task:
            task["settings"] = TASK_SETTINGS_DEFAULT.copy()
        s = task["settings"]
        emoji = lambda k: "✅" if s.get(k, False) else "❌"
        msg = f"🗂️ Task Menu for {label}\nManage this task:"
        buttons = [
            [InlineKeyboardButton("🧮 Filters", callback_data=f"tasksetfilter_{label}")],
            [InlineKeyboardButton(f"{emoji('outgoing')} Outgoing", callback_data=f"toggle_outgoing_{label}"),
             InlineKeyboardButton(f"{emoji('forward_tag')} Forward Tag", callback_data=f"toggle_forwardtag_{label}")],
            [InlineKeyboardButton(f"{emoji('control')} Control", callback_data=f"toggle_control_{label}")],
            [InlineKeyboardButton("🗑️ Delete", callback_data=f"taskdelete_{label}")],
            [InlineKeyboardButton("⬅️ Back", callback_data="show_tasks")],
        ]
        try:
            await query.message.edit_text(msg, reply_markup=InlineKeyboardMarkup(buttons))
        except Exception:
            await query.message.reply_text(msg, reply_markup=InlineKeyboardMarkup(buttons))
        task_settings_state[user_id] = {"label": label, "step": "menu"}
        return

    # Toggles: outgoing, forward_tag, control
    for setting, prefix in [("outgoing", "toggle_outgoing_"), ("forward_tag", "toggle_forwardtag_"), ("control", "toggle_control_")]:
        if data.startswith(prefix):
            label = data[len(prefix):]
            task = task_get(user_id, label)
            if not task:
                await query.message.reply_text("Task not found")
                return
            if "settings" not in task:
                task["settings"] = TASK_SETTINGS_DEFAULT.copy()
            task["settings"][setting] = not task["settings"].get(setting, True)
            try:
                await db_call(db.update_task_settings, user_id, label, task["settings"])
            except Exception:
                pass
            await button_handler(update, context)
            return

    # Delete initiation
    if data.startswith("taskdelete_"):
        label = data[len("taskdelete_"):]
        prompt = f"🗑️ Confirm deletion. Type the task name '{label}' to confirm."
        try:
            await query.message.edit_text(prompt)
        except Exception:
            await query.message.reply_text(prompt)
        task_settings_state[user_id] = {"label": label, "step": "delete_confirm"}
        return

    # Filters submenu
    if data.startswith("tasksetfilter_"):
        label = data[len("tasksetfilter_"):]
        task = task_get(user_id, label)
        if not task:
            await query.message.reply_text("Task not found")
            return
        if "settings" not in task:
            task["settings"] = TASK_SETTINGS_DEFAULT.copy()
        s = task["settings"]
        def fe(k, t): return ("✅" if s.get(k, False) else "❌") + " " + t
        buttons = [
            [InlineKeyboardButton(fe('filter_raw', 'Raw text'), callback_data=f"toggle_filterraw_{label}")],
            [InlineKeyboardButton(fe('filter_numbers', 'Numbers only'), callback_data=f"toggle_filternum_{label}")],
            [InlineKeyboardButton(fe('filter_alpha', 'Alphabets only'), callback_data=f"toggle_filteralpha_{label}")],
            [InlineKeyboardButton(fe('filter_remove_alpha', 'Removed Alphabetic'), callback_data=f"toggle_remalpha_{label}")],
            [InlineKeyboardButton(fe('filter_remove_numeric', 'Removed Numeric'), callback_data=f"toggle_remnum_{label}")],
            [InlineKeyboardButton(fe('filter_prefix', 'Prefix'), callback_data=f"set_prefix_{label}"), InlineKeyboardButton(fe('filter_suffix', 'Suffix'), callback_data=f"set_suffix_{label}")],
            [InlineKeyboardButton("⬅️ Back to Task", callback_data=f"taskmenu_{label}")]
        ]
        try:
            await query.message.edit_text(f"🧰 Filters for {label}", reply_markup=InlineKeyboardMarkup(buttons))
        except Exception:
            await query.message.reply_text(f"🧰 Filters for {label}", reply_markup=InlineKeyboardMarkup(buttons))
        task_settings_state[user_id] = {"label": label, "step": "filters"}
        return

    # Filter toggles mapping
    filter_map = {
        "toggle_filterraw_": "filter_raw",
        "toggle_filternum_": "filter_numbers",
        "toggle_filteralpha_": "filter_alpha",
        "toggle_remalpha_": "filter_remove_alpha",
        "toggle_remnum_": "filter_remove_numeric",
    }
    for prefix, key in filter_map.items():
        if data.startswith(prefix):
            label = data[len(prefix):]
            task = task_get(user_id, label)
            if not task:
                await query.message.reply_text("Task not found")
                return
            if "settings" not in task:
                task["settings"] = TASK_SETTINGS_DEFAULT.copy()
            task["settings"][key] = not task["settings"].get(key, False)
            try:
                await db_call(db.update_task_settings, user_id, label, task["settings"])
            except Exception:
                pass
            await button_handler(update, context)
            return

    # Prefix/suffix set triggers
    if data.startswith("set_prefix_"):
        label = data[len("set_prefix_"):]
        prompt = f"✏️ Send the prefix for '{label}' (send empty to clear)."
        try:
            await query.message.edit_text(prompt)
        except Exception:
            await query.message.reply_text(prompt)
        task_settings_state[user_id] = {"label": label, "step": "prefix"}
        return
    if data.startswith("set_suffix_"):
        label = data[len("set_suffix_"):]
        prompt = f"✏️ Send the suffix for '{label}' (send empty to clear)."
        try:
            await query.message.edit_text(prompt)
        except Exception:
            await query.message.reply_text(prompt)
        task_settings_state[user_id] = {"label": label, "step": "suffix"}
        return

    # Final delete confirm inline
    if data.startswith("confirm_del_"):
        label = data[len("confirm_del_"):]
        try:
            deleted = await db_call(db.remove_forwarding_task, user_id, label)
        except Exception:
            deleted = False
        if user_id in tasks_cache:
            tasks_cache[user_id] = [t for t in tasks_cache[user_id] if t.get("label") != label]
        if deleted:
            await query.message.edit_text(f"✅ Task '{label}' deleted.")
        else:
            await query.message.edit_text(f"❌ Failed to delete '{label}'.")
        task_settings_state.pop(user_id, None)
        return

    await query.message.reply_text("Unknown action.")


# ---------- task settings text handler ----------
async def task_settings_text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in task_settings_state:
        return
    state = task_settings_state[user_id]
    label = state.get("label")
    step = state.get("step")
    text = update.message.text.strip()
    task = task_get(user_id, label)
    if not task:
        await update.message.reply_text("Task not found")
        task_settings_state.pop(user_id, None)
        return
    if step == "prefix":
        val = text if text else None
        task["settings"]["filter_prefix"] = val
        try:
            await db_call(db.update_task_settings, user_id, label, task["settings"])
        except Exception:
            pass
        await update.message.reply_text(f"✅ Prefix set to: {val if val else '(none)'} for '{label}'")
        task_settings_state.pop(user_id, None)
        return
    if step == "suffix":
        val = text if text else None
        task["settings"]["filter_suffix"] = val
        try:
            await db_call(db.update_task_settings, user_id, label, task["settings"])
        except Exception:
            pass
        await update.message.reply_text(f"✅ Suffix set to: {val if val else '(none)'} for '{label}'")
        task_settings_state.pop(user_id, None)
        return
    if step == "delete_confirm":
        if text == label:
            buttons = [[InlineKeyboardButton("Yes — confirm deletion", callback_data=f"confirm_del_{label}"), InlineKeyboardButton("Cancel", callback_data=f"taskmenu_{label}")]]
            await update.message.reply_text(f"Confirm delete '{label}'?", reply_markup=InlineKeyboardMarkup(buttons))
            task_settings_state[user_id] = {"label": label, "step": "delete_final"}
        else:
            await update.message.reply_text("Name doesn't match. Type the exact task name to confirm deletion.")
        return


# ---------- Utility ----------
def task_get(user_id: int, label: str) -> Optional[Dict]:
    return next((t for t in tasks_cache.get(user_id, []) if t.get("label") == label), None)


# ---------- Unified message handler ----------
async def extended_unified_text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id in task_settings_state:
        await task_settings_text_handler(update, context)
        return
    if user_id in forwadd_states:
        await handle_forwadd_steps(update, context)
        return
    await handle_login_process(update, context)


# ---------- Main and handler registration ----------
def main():
    if not BOT_TOKEN:
        logger.error("BOT_TOKEN not set")
        return
    if not API_ID or not API_HASH:
        logger.error("API_ID/API_HASH not set")
        return
    logger.info("Starting bot...")
    start_server_thread()
    application = Application.builder().token(BOT_TOKEN).post_init(post_init).build()
    # Commands
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("login", login_command))
    application.add_handler(CommandHandler("logout", logout_command))
    application.add_handler(CommandHandler("forwadd", forwadd_command_start))
    application.add_handler(CommandHandler("fortasks", fortasks_command))
    application.add_handler(CommandHandler("getallid", getallid_command))
    application.add_handler(CommandHandler("adduser", adduser_command))
    application.add_handler(CommandHandler("removeuser", removeuser_command))
    application.add_handler(CommandHandler("listusers", listusers_command))
    # Callbacks and messages
    application.add_handler(CallbackQueryHandler(button_handler))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, extended_unified_text_handler))
    logger.info("Bot ready, polling...")
    try:
        application.run_polling(drop_pending_updates=True)
    finally:
        try:
            asyncio.run(shutdown_cleanup())
        except Exception:
            logger.exception("Shutdown cleanup failed")


if __name__ == "__main__":
    main()
