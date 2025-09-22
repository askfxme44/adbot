#!/usr/bin/env python3
"""
Telegram AdBot - Single-file production-ready implementation.

Features:
- Add multiple Telethon .session accounts (uploaded to the bot)
- Create campaigns by forwarding messages to the bot (so forwarded message preserves premium emojis)
- Forward campaign messages to groups/DMs, rotating accounts, with jitter and rate-limiting
- Join public and private groups (t.me/..., t.me/+invitehash, joinchat/ links)
- Robust DB (aiosqlite) to persist accounts, campaigns, messages, statistics
- Thread-per-user pattern to isolate account clients (safe use of Telethon clients)
- Advanced flood-wait handling, backoff, retries
- Admin/usage protections and UX flows (start/help/dashboard/accounts/campaigns/stats)
"""

import os
import sys
import logging
import asyncio
import json
import random
import pathlib
import time
import threading
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass, field
from datetime import datetime, timedelta

import aiosqlite
from telethon import TelegramClient, errors, events
from telethon.tl.types import Channel, Chat, User
from telethon.tl.custom import Button
from telethon.tl.functions.channels import JoinChannelRequest, GetParticipantRequest
from telethon.tl.functions.messages import ImportChatInviteRequest, ForwardMessagesRequest
from dotenv import load_dotenv

# ---------------------------
# Configuration
# ---------------------------
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
API_ID = os.getenv("TELEGRAM_API_ID")
API_HASH = os.getenv("TELEGRAM_API_HASH")
REQUIRED_CHANNEL = os.getenv("REQUIRED_CHANNEL", "@askadverts")

DB_PATH = pathlib.Path("adbot.db")
SESSIONS_DIR = pathlib.Path("sessions")
SESSIONS_DIR.mkdir(exist_ok=True)
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# Limits and tuning values
DEFAULT_INTERVAL_SECONDS = 5
MIN_INTERVAL_SECONDS = 3
MAX_RETRIES = 2
FLOOD_WAIT_BUFFER = 5  # seconds to add on top of FloodWait
JOIN_GROUP_BATCH_DELAY = (4.0, 9.0)  # delay between join attempts
FORWARD_JITTER = (0.5, 2.5)  # extra random delay added to campaign interval
PER_ACCOUNT_SEMAPHORE = 1  # concurrency per account (1 = sequential)


# ---------------------------
# Logging
# ---------------------------
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="[%(asctime)s] [%(levelname)s] - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logging.getLogger("telethon").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# ---------------------------
# Data models
# ---------------------------


@dataclass
class Account:
    name: str
    session_file: str
    user_id: int
    status: str = "active"  # active, flood_wait, banned, error
    phone_number: Optional[str] = None
    last_used: Optional[datetime] = None
    flood_wait_until: Optional[datetime] = None
    messages_sent: int = 0
    errors_count: int = 0


@dataclass
class Campaign:
    id: str
    name: str
    user_id: int
    message_ids: List[int]
    source_entity: str  # stored as string; will be resolved when forwarding
    mode: str  # 'groups', 'dms', 'both'
    interval: int
    active: bool = False
    filters: Dict[str, Any] = field(default_factory=dict)


# ---------------------------
# Database manager
# ---------------------------


class DatabaseManager:
    def __init__(self, db_path: pathlib.Path = DB_PATH):
        self.db_path = db_path

    async def initialize(self) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.executescript(
                """
                PRAGMA foreign_keys = ON;
                CREATE TABLE IF NOT EXISTS accounts (
                    name TEXT PRIMARY KEY,
                    session_file TEXT NOT NULL,
                    user_id INTEGER NOT NULL,
                    status TEXT,
                    phone_number TEXT,
                    last_used TEXT,
                    flood_wait_until TEXT,
                    messages_sent INTEGER,
                    errors_count INTEGER
                );
                CREATE TABLE IF NOT EXISTS campaigns (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    user_id INTEGER NOT NULL,
                    source_entity TEXT NOT NULL,
                    mode TEXT NOT NULL,
                    interval INTEGER NOT NULL,
                    active INTEGER,
                    filters TEXT
                );
                CREATE TABLE IF NOT EXISTS campaign_messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    campaign_id TEXT NOT NULL,
                    telegram_message_id INTEGER NOT NULL,
                    FOREIGN KEY (campaign_id) REFERENCES campaigns (id) ON DELETE CASCADE
                );
                CREATE TABLE IF NOT EXISTS statistics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    account_name TEXT,
                    campaign_id TEXT,
                    target_id TEXT,
                    target_type TEXT,
                    message_sent INTEGER,
                    timestamp TEXT,
                    error TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_accounts_user_id ON accounts(user_id);
                CREATE INDEX IF NOT EXISTS idx_campaigns_user_id ON campaigns(user_id);
                """
            )
            await db.commit()
        logger.info("Database initialized.")

    # Accounts
    async def save_account(self, account: Account) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                INSERT OR REPLACE INTO accounts
                (name, session_file, user_id, status, phone_number, last_used, flood_wait_until, messages_sent, errors_count)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    account.name,
                    account.session_file,
                    account.user_id,
                    account.status,
                    account.phone_number,
                    account.last_used.isoformat() if account.last_used else None,
                    account.flood_wait_until.isoformat() if account.flood_wait_until else None,
                    account.messages_sent,
                    account.errors_count,
                ),
            )
            await db.commit()

    async def get_accounts(self) -> List[Account]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("SELECT * FROM accounts")
            rows = await cur.fetchall()
            accounts: List[Account] = []
            for r in rows:
                try:
                    last_used = datetime.fromisoformat(r["last_used"]) if r["last_used"] else None
                    flood_wait_until = (
                        datetime.fromisoformat(r["flood_wait_until"]) if r["flood_wait_until"] else None
                    )
                    accounts.append(
                        Account(
                            name=r["name"],
                            session_file=r["session_file"],
                            user_id=r["user_id"],
                            status=r["status"],
                            phone_number=r["phone_number"],
                            last_used=last_used,
                            flood_wait_until=flood_wait_until,
                            messages_sent=r["messages_sent"] or 0,
                            errors_count=r["errors_count"] or 0,
                        )
                    )
                except Exception as e:
                    logger.error("Failed to parse account row: %s", e)
            return accounts

    async def delete_account(self, account_name: str) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("DELETE FROM accounts WHERE name = ?", (account_name,))
            await db.commit()

    # Campaigns
    async def save_campaign(self, campaign: Campaign) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("BEGIN")
            await db.execute(
                """
                INSERT OR REPLACE INTO campaigns (id, name, user_id, source_entity, mode, interval, active, filters)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    campaign.id,
                    campaign.name,
                    campaign.user_id,
                    campaign.source_entity,
                    campaign.mode,
                    campaign.interval,
                    1 if campaign.active else 0,
                    json.dumps(campaign.filters or {}),
                ),
            )
            # Replace messages
            await db.execute("DELETE FROM campaign_messages WHERE campaign_id = ?", (campaign.id,))
            if campaign.message_ids:
                await db.executemany(
                    "INSERT INTO campaign_messages (campaign_id, telegram_message_id) VALUES (?, ?)",
                    [(campaign.id, mid) for mid in campaign.message_ids],
                )
            await db.commit()

    async def get_campaigns(self) -> List[Campaign]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            campaigns: List[Campaign] = []
            cur = await db.execute("SELECT * FROM campaigns")
            rows = await cur.fetchall()
            for r in rows:
                try:
                    cur2 = await db.execute(
                        "SELECT telegram_message_id FROM campaign_messages WHERE campaign_id = ?",
                        (r["id"],),
                    )
                    msg_rows = await cur2.fetchall()
                    message_ids = [m["telegram_message_id"] for m in msg_rows]
                    campaigns.append(
                        Campaign(
                            id=r["id"],
                            name=r["name"],
                            user_id=r["user_id"],
                            message_ids=message_ids,
                            source_entity=r["source_entity"],
                            mode=r["mode"],
                            interval=r["interval"],
                            active=bool(r["active"]),
                            filters=json.loads(r["filters"]) if r["filters"] else {},
                        )
                    )
                except Exception as e:
                    logger.error("Failed to load campaign %s: %s", r["id"], e)
            return campaigns

    async def delete_campaign(self, campaign_id: str) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("DELETE FROM campaigns WHERE id = ?", (campaign_id,))
            await db.commit()

    # Statistics
    async def log_activity(
        self, account_name: str, campaign_id: str, target_id: Union[int, str], target_type: str, success: bool, error: Optional[str] = None
    ) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                INSERT INTO statistics (account_name, campaign_id, target_id, target_type, message_sent, timestamp, error)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    account_name,
                    campaign_id,
                    str(target_id),
                    target_type,
                    1 if success else 0,
                    datetime.utcnow().isoformat(),
                    error,
                ),
            )
            await db.commit()

    async def get_user_statistics(self, account_names: List[str]) -> Dict[str, Any]:
        if not account_names:
            return {
                "total_sent": 0,
                "total_failed": 0,
                "today_sent": 0,
                "by_account": {},
            }
        placeholder = ",".join("?" for _ in account_names)
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            # overall sent/failed
            cur = await db.execute(f"SELECT message_sent, COUNT(*) as cnt FROM statistics WHERE account_name IN ({placeholder}) GROUP BY message_sent", account_names)
            rows = await cur.fetchall()
            total_sent = next((r["cnt"] for r in rows if r["message_sent"] == 1), 0)
            total_failed = next((r["cnt"] for r in rows if r["message_sent"] == 0), 0)
            # today sent
            today = datetime.utcnow().strftime("%Y-%m-%d")
            cur2 = await db.execute(f"SELECT COUNT(*) as cnt FROM statistics WHERE account_name IN ({placeholder}) AND timestamp LIKE ? AND message_sent = 1", (*account_names, f"{today}%"))
            row2 = await cur2.fetchone()
            today_sent = row2["cnt"] if row2 else 0
            # by account
            cur3 = await db.execute(f"SELECT account_name, message_sent, COUNT(*) as cnt FROM statistics WHERE account_name IN ({placeholder}) GROUP BY account_name, message_sent", account_names)
            rows3 = await cur3.fetchall()
            by_account: Dict[str, Dict[str, int]] = {}
            for r in rows3:
                acc = r["account_name"]
                if acc not in by_account:
                    by_account[acc] = {"sent": 0, "failed": 0}
                if r["message_sent"] == 1:
                    by_account[acc]["sent"] = r["cnt"]
                else:
                    by_account[acc]["failed"] = r["cnt"]
            return {
                "total_sent": total_sent,
                "total_failed": total_failed,
                "today_sent": today_sent,
                "by_account": by_account,
            }


# ---------------------------
# Thread manager - per-user loops
# ---------------------------


class ThreadManager:
    """
    Creates isolated asyncio loops in background threads per user.
    Also stores per-thread clients and tasks.
    """

    def __init__(self):
        self.user_threads: Dict[int, threading.Thread] = {}
        self.user_loops: Dict[int, asyncio.AbstractEventLoop] = {}
        self.user_tasks: Dict[int, Dict[str, asyncio.Task]] = {}
        self.user_clients: Dict[int, Dict[str, TelegramClient]] = {}
        self._lock = threading.Lock()

    def create_user_thread(self, user_id: int) -> None:
        with self._lock:
            if user_id in self.user_threads:
                return

            def worker():
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                self.user_loops[user_id] = loop
                self.user_tasks[user_id] = {}
                self.user_clients[user_id] = {}
                try:
                    loop.run_forever()
                finally:
                    # cancel tasks
                    tasks = list(self.user_tasks.get(user_id, {}).values())
                    for t in tasks:
                        if not t.done():
                            t.cancel()
                    # disconnect clients
                    for client in list(self.user_clients.get(user_id, {}).values()):
                        try:
                            if client.is_connected():
                                loop.run_until_complete(client.disconnect())
                        except Exception:
                            pass
                    loop.close()
                    self.user_loops.pop(user_id, None)
                    self.user_tasks.pop(user_id, None)
                    self.user_clients.pop(user_id, None)

            t = threading.Thread(target=worker, daemon=True)
            t.start()
            self.user_threads[user_id] = t
            # give it a moment to initialize
            time.sleep(0.1)

    def stop_user_thread(self, user_id: int, timeout: float = 5.0) -> None:
        with self._lock:
            loop = self.user_loops.get(user_id)
            if loop:
                loop.call_soon_threadsafe(loop.stop)
            thr = self.user_threads.get(user_id)
            if thr:
                thr.join(timeout=timeout)
                self.user_threads.pop(user_id, None)

    def run_coroutine_in_user_loop(self, user_id: int, coro):
        self.create_user_thread(user_id)
        loop = self.user_loops[user_id]
        future = asyncio.run_coroutine_threadsafe(coro, loop)
        return future

    def create_user_task(self, user_id: int, task_name: str, coro):
        """
        Schedules a named coroutine as a task inside the user's loop.
        """
        self.create_user_thread(user_id)

        async def wrapper():
            try:
                await coro
            except asyncio.CancelledError:
                logger.info("Task %s for user %s was cancelled.", task_name, user_id)
            except Exception:
                logger.exception("Task %s for user %s raised an exception.", task_name, user_id)
            finally:
                # cleanup from user_tasks dict
                if user_id in self.user_tasks and task_name in self.user_tasks[user_id]:
                    self.user_tasks[user_id].pop(task_name, None)

        def schedule():
            loop = self.user_loops[user_id]
            task = loop.create_task(wrapper())
            self.user_tasks[user_id][task_name] = task
            return task

        future = asyncio.run_coroutine_threadsafe(asyncio.to_thread(schedule), self.user_loops[user_id])
        # wait a tiny bit for schedule to execute in the user loop
        try:
            return future.result(timeout=3)
        except Exception:
            return None

    def cancel_user_task(self, user_id: int, task_name: str) -> None:
        if user_id not in self.user_tasks:
            return
        task = self.user_tasks[user_id].get(task_name)
        if task and not task.done():
            task.cancel()
            self.user_tasks[user_id].pop(task_name, None)


# ---------------------------
# Main bot
# ---------------------------


class TelegramAdBot:
    def __init__(self):
        # env checks
        if not BOT_TOKEN or not API_ID or not API_HASH:
            logger.critical("BOT_TOKEN, TELEGRAM_API_ID, TELEGRAM_API_HASH must be set in .env")
            raise SystemExit("Missing env variables")

        # Core
        self.db = DatabaseManager()
        self.bot: Optional[TelegramClient] = None
        self.thread_manager = ThreadManager()

        # In-memory caches
        self.accounts: Dict[str, Account] = {}
        self.clients: Dict[str, TelegramClient] = {}
        self.campaigns: Dict[str, Campaign] = {}
        self.user_state: Dict[int, Dict[str, Any]] = {}
        self.stats = {"uptime_start": datetime.utcnow()}

        # Per-account semaphores to limit concurrency
        self._account_semaphores: Dict[str, asyncio.Semaphore] = {}

    # ---------- lifecycle ----------
    async def start(self):
        await self.db.initialize()
        # load persisted data
        await self._load_from_db()

        # create the bot client
        self.bot = TelegramClient("bot_session", int(API_ID), API_HASH)
        self._register_handlers()

        logger.info("Starting bot...")
        await self.bot.start(bot_token=BOT_TOKEN)
        logger.info("Bot started.")
        await self.bot.run_until_disconnected()

    async def shutdown(self):
        logger.info("Shutting down Bot...")
        # stop per-user threads
        for uid in list(self.thread_manager.user_threads.keys()):
            try:
                self.thread_manager.stop_user_thread(uid)
            except Exception:
                pass
        # disconnect global bot
        if self.bot and self.bot.is_connected():
            await self.bot.disconnect()
        # disconnect any clients in main process
        for client in list(self.clients.values()):
            try:
                if client.is_connected():
                    await client.disconnect()
            except Exception:
                pass
        logger.info("Shutdown complete.")

    async def _load_from_db(self):
        accounts = await self.db.get_accounts()
        self.accounts = {a.name: a for a in accounts}
        campaigns = await self.db.get_campaigns()
        self.campaigns = {c.id: c for c in campaigns}
        logger.info("Loaded %d accounts and %d campaigns from DB", len(self.accounts), len(self.campaigns))
        # recover flood_wait statuses
        for a in self.accounts.values():
            if a.status == "flood_wait" and a.flood_wait_until and datetime.utcnow() > a.flood_wait_until:
                a.status = "active"
                a.flood_wait_until = None
                await self.db.save_account(a)

    # ---------- handlers & UI ----------
    def _register_handlers(self):
        assert self.bot is not None
        self.bot.add_event_handler(self.cmd_start, events.NewMessage(pattern="/start"))
        self.bot.add_event_handler(self.cmd_help, events.NewMessage(pattern="/help"))
        self.bot.add_event_handler(self.cmd_stats, events.NewMessage(pattern="/stats"))
        self.bot.add_event_handler(self.cmd_stop, events.NewMessage(pattern="/stop"))
        self.bot.add_event_handler(self.on_callback, events.CallbackQuery)
        # document handler for .session uploads
        self.bot.add_event_handler(self.on_document, events.NewMessage(func=lambda e: e.document))
        # general message handler: forwarded messages + text flows
        self.bot.add_event_handler(self.on_message, events.NewMessage(func=lambda e: (e.text or e.fwd_from) and not (e.text and e.text.startswith("/"))))

    # ---------- basic command implementations ----------
    async def cmd_start(self, event):
        sender = await event.get_sender()
        user_id = sender.id if sender else event.sender_id
        if not await self._check_channel_membership(user_id):
            btns = [
                [Button.url("Join Channel", f"https://t.me/{REQUIRED_CHANNEL.lstrip('@')}")],
                [Button.inline("Check Membership", b"check_membership")],
            ]
            await event.reply(
                f"To use this bot you must join the channel {REQUIRED_CHANNEL}. Please join and click Check Membership.",
                buttons=btns,
            )
            return

        text = "Welcome to the Telegram Marketing Bot.\n\nUse the buttons to manage accounts, campaigns and statistics."
        buttons = [
            [Button.inline("Dashboard", b"dashboard")],
            [Button.inline("Accounts", b"accounts")],
            [Button.inline("Messaging", b"campaigns")],
            [Button.inline("Statistics", b"statistics")],
            [Button.inline("Help", b"help")],
        ]
        await event.reply(text, buttons=buttons)

    async def cmd_help(self, event):
        help_text = (
            "This bot helps you run messaging campaigns across multiple Telegram accounts.\n\n"
            "Main flows:\n"
            "- Add account: upload a .session file (via Accounts -> Add Account)\n"
            "- Create campaign: forward messages to the bot so they are preserved, then configure mode/interval\n"
            "- Start campaign: runs across your accounts and forwards messages to groups/DMs\n\n"
            "Commands:\n"
            "/start - show main menu\n"
            "/help - this help\n"
            "/stats - quick statistics\n"
            "/stop - stop all running tasks for your user\n"
        )
        await event.reply(help_text)

    async def cmd_stats(self, event):
        sender = await event.get_sender()
        user_id = sender.id if sender else event.sender_id
        account_names = [a.name for a in self._get_user_accounts(user_id)]
        stats = await self.db.get_user_statistics(account_names)
        total_attempts = stats["total_sent"] + stats["total_failed"]
        success_rate = (stats["total_sent"] / total_attempts * 100) if total_attempts > 0 else 0.0
        lines = [
            "Statistics",
            f"Total Sent: {stats['total_sent']}",
            f"Total Failed: {stats['total_failed']}",
            f"Success Rate: {success_rate:.2f}%",
            f"Sent Today: {stats['today_sent']}",
            "",
            "Per Account:",
        ]
        if stats["by_account"]:
            for acc, vals in stats["by_account"].items():
                lines.append(f" - {acc}: {vals['sent']}/{vals['failed']}")
        else:
            lines.append(" No activity recorded.")
        await event.reply("\n".join(lines))

    async def cmd_stop(self, event):
        # Cancel all tasks for this user (thread_manager.user_tasks)
        sender = await event.get_sender()
        user_id = sender.id if sender else event.sender_id
        task_keys = list(self.thread_manager.user_tasks.get(user_id, {}).keys())
        stopped = 0
        for key in task_keys:
            self.thread_manager.cancel_user_task(user_id, key)
            stopped += 1
        await event.reply(f"Stopped {stopped} running task(s).")

    # ---------- callback router ----------
    async def on_callback(self, event):
        try:
            data = event.data.decode("utf-8") if isinstance(event.data, (bytes, bytearray)) else str(event.data)
        except Exception:
            data = str(event.data)

        user_id = event.sender_id
        # membership check unless checking membership itself
        if data != "check_membership" and not await self._check_channel_membership(user_id):
            btns = [
                [Button.url("Join Channel", f"https://t.me/{REQUIRED_CHANNEL.lstrip('@')}")],
                [Button.inline("Check Membership", b"check_membership")],
            ]
            try:
                await event.edit(f"You must join {REQUIRED_CHANNEL} to use this bot.", buttons=btns)
            except errors.MessageNotModifiedError:
                pass
            return

        # route
        try:
            if data == "dashboard":
                await self._show_dashboard(event)
            elif data == "accounts":
                await self._show_accounts_menu(event)
            elif data == "campaigns":
                await self._show_campaigns_menu(event)
            elif data == "statistics":
                await self._show_stats_menu(event)
            elif data == "help":
                await self.cmd_help(event)
            elif data == "check_membership":
                await event.answer("Checking membership...", alert=False)
                await self.cmd_start(event)
            # Account flow
            elif data == "add_account":
                await self._initiate_account_upload(event)
            elif data.startswith("account_details_"):
                name = data.split("_", 2)[2]
                await self._show_account_details(event, name)
            elif data.startswith("delete_account_"):
                name = data.split("_", 2)[2]
                await self._confirm_delete(event, "account", name)
            elif data.startswith("do_delete_account_"):
                name = data.split("_", 2)[2]
                await self._do_delete_account(event, name)
            elif data.startswith("test_account_"):
                name = data.split("_", 2)[2]
                await self._test_account(event, name)
            elif data == "join_groups":
                await self._initiate_group_join(event)
            # Campaigns flow
            elif data == "create_campaign":
                await self._initiate_campaign_creation(event)
            elif data.startswith("campaign_details_"):
                camp_id = data.split("_", 2)[2]
                await self._show_campaign_details(event, camp_id)
            elif data.startswith("delete_campaign_"):
                camp_id = data.split("_", 2)[2]
                await self._confirm_delete(event, "campaign", camp_id)
            elif data.startswith("do_delete_campaign_"):
                camp_id = data.split("_", 2)[2]
                await self._do_delete_campaign(event, camp_id)
            elif data.startswith("activate_campaign_"):
                camp_id = data.split("_", 2)[2]
                await self._toggle_campaign(event, camp_id, True)
            elif data.startswith("deactivate_campaign_"):
                camp_id = data.split("_", 2)[2]
                await self._toggle_campaign(event, camp_id, False)
            elif data.startswith("start_campaign_"):
                camp_id = data.split("_", 2)[2]
                await self._start_campaign_task(event, camp_id, None)
            elif data.startswith("stop_campaign_"):
                camp_id = data.split("_", 2)[2]
                await self._stop_campaign_task(event, camp_id, None)
            elif data.startswith("start_account_campaign_"):
                parts = data.split("_", 3)
                account_name = parts[2]
                camp_id = parts[3]
                await self._start_campaign_task(event, camp_id, [account_name])
            elif data.startswith("stop_account_campaign_"):
                parts = data.split("_", 3)
                account_name = parts[2]
                camp_id = parts[3]
                await self._stop_campaign_task(event, camp_id, [account_name])
            # campaign creation mode selection
            elif data.startswith("set_mode_"):
                mode = data.split("_", 2)[2]
                await self._handle_campaign_mode_selection(event, mode)
            else:
                await event.answer("Unknown action.", alert=True)
        except errors.MessageNotModifiedError:
            pass
        except Exception:
            logger.exception("Unhandled callback error")
            try:
                await event.answer("An unexpected error occurred.", alert=True)
            except Exception:
                pass

    # ---------- file/document handlers ----------
    async def on_document(self, event):
        # Called when user sends a document (we only treat if user_state indicates upload)
        sender = await event.get_sender()
        user_id = sender.id if sender else event.sender_id
        state = self.user_state.get(user_id, {})
        if state.get("action") == "upload_session":
            await self._handle_session_upload(event)

    async def on_message(self, event):
        # Multi-step flows: campaign creation, group joining, general text handling
        sender = await event.get_sender()
        user_id = sender.id if sender else event.sender_id
        state = self.user_state.get(user_id)
        # If forwarded message during campaign creation, collect it
        if event.fwd_from and state and state.get("action") == "campaign_messages":
            await self._handle_forwarded_message_for_campaign(event)
            return

        if not state:
            return

        action = state.get("action")
        if action == "campaign_name":
            await self._handle_campaign_name_input(event)
        elif action == "campaign_messages":
            # 'done' triggers finalize
            text = (event.raw_text or "").strip()
            if text.lower() == "done":
                await self._finalize_campaign_messages(event)
            else:
                await event.reply("Please forward messages or type 'done' when finished.")
        elif action == "campaign_interval":
            await self._handle_campaign_interval_input(event)
        elif action == "awaiting_group_links":
            await self._handle_group_join_input(event)
        elif action == "upload_session":
            # user accidentally sent text; tell them to upload file
            await event.reply("Please upload a .session file for the account (as a document).")
        else:
            # unknown state: clear
            self.user_state.pop(user_id, None)
            await event.reply("Session expired. Please start again.")

    # ---------- membership check ----------
    async def _check_channel_membership(self, user_id: int) -> bool:
        """Return True if user is participant of REQUIRED_CHANNEL"""
        if not self.bot:
            return True
        try:
            # Resolve channel entity first
            channel_entity = await self.bot.get_entity(REQUIRED_CHANNEL)
            # get_participant will raise if user not participant
            await self.bot(GetParticipantRequest(channel_entity, user_id))
            return True
        except errors.UserNotParticipantError:
            return False
        except ValueError:
            # invalid channel
            logger.error("REQUIRED_CHANNEL invalid: %s", REQUIRED_CHANNEL)
            return False
        except Exception:
            # other errors - do not block usage: log and allow (fail-safe)
            logger.exception("Error checking channel membership")
            # allow in case of transient errors
            return True

    # ---------- accounts UI & logic ----------
    def _get_user_accounts(self, user_id: int) -> List[Account]:
        return [a for a in self.accounts.values() if a.user_id == user_id]

    async def _show_dashboard(self, event):
        sender = await event.get_sender()
        user_id = sender.id if sender else event.sender_id
        accounts = self._get_user_accounts(user_id)
        campaigns = [c for c in self.campaigns.values() if c.user_id == user_id]
        active_accs = len([a for a in accounts if a.status == "active"])
        uptime = str(datetime.utcnow() - self.stats["uptime_start"]).split(".")[0]
        text = (
            f"Dashboard\n\nAccounts: {active_accs} active / {len(accounts)} total\n"
            f"Messaging: {len(campaigns)} configured\n"
            f"Running Tasks: {len(self.thread_manager.user_tasks.get(user_id, {}))}\n"
            f"Uptime: {uptime}"
        )
        buttons = [
            [Button.inline("Accounts", b"accounts")],
            [Button.inline("Messaging", b"campaigns")],
            [Button.inline("Statistics", b"statistics")],
            [Button.inline("Help", b"help")],
        ]
        try:
            await event.edit(text, buttons=buttons)
        except errors.MessageNotModifiedError:
            pass

    async def _show_accounts_menu(self, event):
        sender = await event.get_sender()
        user_id = sender.id if sender else event.sender_id
        accounts = self._get_user_accounts(user_id)
        text = "Accounts Management\n\n"
        buttons = []
        if not accounts:
            text += "You have not added any accounts yet."
        else:
            for acc in sorted(accounts, key=lambda x: x.name):
                buttons.append([Button.inline(f"{acc.name} ({acc.status})", f"account_details_{acc.name}")])
        buttons.append([Button.inline("Add Account", b"add_account")])
        buttons.append([Button.inline("Join Groups", b"join_groups")])
        buttons.append([Button.inline("Back", b"dashboard")])
        try:
            await event.edit(text, buttons=buttons)
        except errors.MessageNotModifiedError:
            pass

    async def _initiate_account_upload(self, event):
        sender = await event.get_sender()
        user_id = sender.id if sender else event.sender_id
        self.user_state[user_id] = {"action": "upload_session"}
        try:
            await event.edit("Please upload your Telethon .session file as a document. This file must be the exact .session file generated by Telethon.", buttons=[[Button.inline("Cancel", b"accounts")]])
        except errors.MessageNotModifiedError:
            pass

    async def _handle_session_upload(self, event):
        sender = await event.get_sender()
        user_id = sender.id if sender else event.sender_id
        msg = await event.reply("Processing session file...")
        try:
            doc = event.document
            file_name = None
            for attr in getattr(doc, "attributes", []):
                if hasattr(attr, "file_name"):
                    file_name = attr.file_name
                    break
            if not file_name or not file_name.endswith(".session"):
                await msg.edit("Invalid file. Please upload a .session file.")
                self.user_state.pop(user_id, None)
                return

            account_name = f"user{user_id}_{int(time.time())}"
            target_path = SESSIONS_DIR / f"{account_name}.session"
            await event.download_media(file=str(target_path))

            account = Account(name=account_name, session_file=str(target_path), user_id=user_id)
            # initialize client (in main loop) to check auth
            ok = await self._init_account_client(account)
            if not ok:
                # remove file
                try:
                    if target_path.exists():
                        target_path.unlink()
                except Exception:
                    pass
                await msg.edit("Failed to initialize the session. The session may be invalid or revoked.")
            else:
                self.accounts[account.name] = account
                await self.db.save_account(account)
                await msg.edit(f"Account {account.name} added successfully.")
            # show accounts menu
            await self._show_accounts_menu(event)
        except Exception:
            logger.exception("Error processing session upload")
            await msg.edit("An unexpected error occurred while processing the file.")
        finally:
            self.user_state.pop(user_id, None)

    async def _init_account_client(self, account: Account) -> bool:
        """
        Initialize a Telethon client for the account in the main loop (used for initial auth/test).
        Returns True if authorized / connected.
        """
        # if already have
        if account.name in self.clients and self.clients[account.name].is_connected():
            return True
        try:
            client = TelegramClient(account.session_file, int(API_ID), API_HASH)
            await client.connect()
            if not await client.is_user_authorized():
                account.status = "error"
                account.errors_count += 1
                await self.db.save_account(account)
                await client.disconnect()
                return False
            me = await client.get_me()
            account.phone_number = getattr(me, "phone", None) or "Unknown"
            account.status = "active" if account.status != "banned" else account.status
            await self.db.save_account(account)
            self.clients[account.name] = client
            self._account_semaphores[account.name] = asyncio.Semaphore(PER_ACCOUNT_SEMAPHORE)
            logger.info("Initialized account client %s (%s)", account.name, account.phone_number)
            return True
        except Exception:
            logger.exception("Failed to init account client %s", account.name)
            account.status = "error"
            account.errors_count += 1
            await self.db.save_account(account)
            return False

    # initialize client inside user's thread event loop (for campaign running)
    async def _init_account_client_in_thread(self, account: Account, user_id: int) -> bool:
        """
        This will be invoked inside the user's dedicated thread/loop.
        It constructs and connects a TelegramClient using the account.session_file, stores it under thread_manager.user_clients.
        """
        # Check if client exists in thread storage
        thread_clients = self.thread_manager.user_clients.get(user_id, {})
        if account.name in thread_clients:
            cli = thread_clients[account.name]
            if cli.is_connected():
                return True
        # Validate session file exists
        if not pathlib.Path(account.session_file).exists():
            logger.error("Session file missing for %s", account.name)
            return False
        try:
            client = TelegramClient(account.session_file, int(API_ID), API_HASH)
            await client.connect()
            if not await client.is_user_authorized():
                logger.error("Session file for %s is not authorized", account.name)
                account.status = "error"
                await self.db.save_account(account)
                await client.disconnect()
                return False
            me = await client.get_me()
            account.phone_number = getattr(me, "phone", None) or "Unknown"
            account.status = "active" if account.status != "banned" else account.status
            await self.db.save_account(account)
            # store
            if user_id not in self.thread_manager.user_clients:
                self.thread_manager.user_clients[user_id] = {}
            self.thread_manager.user_clients[user_id][account.name] = client
            # ensure a semaphore exists in the thread loop as well if needed
            # Note: per-account semaphores are created in main loop; for thread safety, rely on sequential usage inside thread.
            logger.info("Initialized account client %s in thread for user %s", account.name, user_id)
            return True
        except Exception:
            logger.exception("Failed to init account %s in thread for user %s", account.name, user_id)
            account.status = "error"
            account.errors_count += 1
            await self.db.save_account(account)
            return False

    async def _show_account_details(self, event, account_name: str):
        account = self.accounts.get(account_name)
        if not account:
            await event.edit("Account not found.", buttons=[[Button.inline("Back", b"accounts")]])
            return
        last = account.last_used.strftime("%Y-%m-%d %H:%M:%S") if account.last_used else "Never"
        text = (
            f"Account: {account.name}\n"
            f"Status: {account.status}\n"
            f"Phone: {account.phone_number}\n"
            f"Messages Sent: {account.messages_sent}\n"
            f"Errors: {account.errors_count}\n"
            f"Last Used: {last}\n"
        )
        if account.status == "flood_wait" and account.flood_wait_until:
            rem = account.flood_wait_until - datetime.utcnow()
            if rem.total_seconds() > 0:
                text += f"Wait Ends In: {str(rem).split('.')[0]}\n"
        buttons = [
            [Button.inline("Test Account", f"test_account_{account.name}")],
            [Button.inline("Delete Account", f"delete_account_{account.name}")],
            [Button.inline("Back", b"accounts")],
        ]
        try:
            await event.edit(text, buttons=buttons)
        except errors.MessageNotModifiedError:
            pass

    async def _confirm_delete(self, event, item_type: str, item_id: str):
        if item_type == "account":
            text = f"Are you sure you want to permanently delete the account '{item_id}'?"
            yes = f"do_delete_account_{item_id}"
            no = "accounts"
        else:
            camp = self.campaigns.get(item_id)
            name = camp.name if camp else item_id
            text = f"Are you sure you want to permanently delete campaign '{name}'?"
            yes = f"do_delete_campaign_{item_id}"
            no = "campaigns"
        buttons = [[Button.inline("Yes, delete", yes), Button.inline("No, cancel", no)]]
        try:
            await event.edit(text, buttons=buttons)
        except errors.MessageNotModifiedError:
            pass

    async def _do_delete_account(self, event, account_name: str):
        user = await event.get_sender()
        user_id = user.id if user else event.sender_id
        # cancel tasks for this account
        for key in list(self.thread_manager.user_tasks.get(user_id, {}).keys()):
            if key.startswith(f"{account_name}_") or key.endswith(f"_{account_name}"):
                self.thread_manager.cancel_user_task(user_id, key)
        # disconnect client if in main
        client = self.clients.pop(account_name, None)
        if client:
            try:
                if client.is_connected():
                    await client.disconnect()
            except Exception:
                pass
        # remove session file and record
        acc = self.accounts.pop(account_name, None)
        if acc:
            try:
                p = pathlib.Path(acc.session_file)
                if p.exists():
                    p.unlink()
            except Exception:
                pass
            await self.db.delete_account(account_name)
        await event.answer(f"Account {account_name} deleted.", alert=True)
        await self._show_accounts_menu(event)

    async def _test_account(self, event, account_name: str):
        acc = self.accounts.get(account_name)
        if not acc or acc.status != "active":
            return await event.answer("Account not found or not active.", alert=True)
        # ensure client is initialized
        ok = await self._init_account_client(acc)
        if not ok:
            return await event.answer("Failed to initialize client for test.", alert=True)
        client = self.clients.get(account_name)
        try:
            await client.send_message(event.sender_id, f"Test message from account {account_name}.")
            await event.answer("Test message sent. Check your messages.", alert=False)
        except Exception as e:
            logger.exception("Failed to send test message")
            await event.answer(f"Test failed: {e}", alert=True)

    # ---------- Campaign creation flow ----------
    async def _initiate_campaign_creation(self, event):
        sender = await event.get_sender()
        user_id = sender.id if sender else event.sender_id
        if not self._get_user_accounts(user_id):
            return await event.answer("You must add at least one account before creating a campaign.", alert=True)
        self.user_state[user_id] = {"action": "campaign_name", "data": {}}
        try:
            await event.edit("Step 1/4: Enter a name for your campaign (4-50 characters).", buttons=[[Button.inline("Cancel", b"campaigns")]])
        except errors.MessageNotModifiedError:
            pass

    async def _handle_campaign_name_input(self, event):
        sender = await event.get_sender()
        user_id = sender.id if sender else event.sender_id
        text = (event.raw_text or "").strip()
        if not (4 <= len(text) <= 50):
            return await event.reply("Campaign name must be between 4 and 50 characters.")
        state = self.user_state[user_id]
        state["data"]["name"] = text
        state["action"] = "campaign_messages"
        state["data"]["message_ids"] = []
        # tell user to forward messages
        await event.reply("Step 2/4: Forward the messages you want to include in the campaign to this chat. When finished, type 'done'.")

    async def _handle_forwarded_message_for_campaign(self, event):
        # store the forwarded message's id and the chat as source entity
        sender = await event.get_sender()
        user_id = sender.id if sender else event.sender_id
        state = self.user_state.get(user_id)
        if not state or state.get("action") != "campaign_messages":
            return
        if "message_ids" not in state["data"]:
            state["data"]["message_ids"] = []
        # Save the id and source chat id
        state["data"]["message_ids"].append(event.id)
        state["data"].setdefault("source_entity", event.chat_id)
        await event.reply(f"Message added ({len(state['data']['message_ids'])}). Forward more or type 'done'.")

    async def _finalize_campaign_messages(self, event):
        sender = await event.get_sender()
        user_id = sender.id if sender else event.sender_id
        state = self.user_state.get(user_id)
        if not state or not state["data"].get("message_ids"):
            return await event.reply("No messages were added. Please forward messages first.")
        # present mode choices
        buttons = [
            [Button.inline("Groups Only", b"set_mode_groups")],
            [Button.inline("DMs Only", b"set_mode_dms")],
            [Button.inline("Both Groups & DMs", b"set_mode_both")],
        ]
        await event.reply(f"Step 3/4: {len(state['data']['message_ids'])} messages saved. Choose targeting mode:", buttons=buttons)

    async def _handle_campaign_mode_selection(self, event, mode: str):
        sender = await event.get_sender()
        user_id = sender.id if sender else event.sender_id
        state = self.user_state.get(user_id)
        if not state:
            return await event.answer("Session expired, please start over.", alert=True)
        state["data"]["mode"] = mode
        state["action"] = "campaign_interval"
        try:
            await event.edit(f"Mode set to {mode}. Step 4/4: Enter the delay (in seconds) between messages. Recommended 5+.", buttons=[[Button.inline("Cancel", b"campaigns")]])
        except errors.MessageNotModifiedError:
            pass

    async def _handle_campaign_interval_input(self, event):
        sender = await event.get_sender()
        user_id = sender.id if sender else event.sender_id
        state = self.user_state.get(user_id)
        if not state:
            return
        try:
            interval = int((event.raw_text or "").strip())
            if interval < MIN_INTERVAL_SECONDS:
                return await event.reply(f"Interval must be at least {MIN_INTERVAL_SECONDS} seconds.")
            data = state["data"]
            # Create campaign
            camp = Campaign(
                id=f"camp_{user_id}_{int(time.time())}",
                name=data["name"],
                user_id=user_id,
                message_ids=data["message_ids"],
                source_entity=str(data["source_entity"]),
                mode=data["mode"],
                interval=interval,
                active=False,
                filters={},
            )
            self.campaigns[camp.id] = camp
            await self.db.save_campaign(camp)
            await event.reply(f"Campaign '{camp.name}' created successfully.")
            self.user_state.pop(user_id, None)
            await self._show_campaigns_menu(event)
        except ValueError:
            await event.reply("Please enter a valid integer for interval.")

    async def _show_campaigns_menu(self, event):
        sender = await event.get_sender()
        user_id = sender.id if sender else event.sender_id
        camps = [c for c in self.campaigns.values() if c.user_id == user_id]
        text = "Messaging Management\n\n"
        buttons = []
        if not camps:
            text += "You have not created any messaging campaigns."
        else:
            for c in sorted(camps, key=lambda x: x.name):
                is_running = any(key == c.id or key.endswith(f"_{c.id}") for key in self.thread_manager.user_tasks.get(user_id, {}))
                status = "Running" if is_running else ("Active" if c.active else "Inactive")
                buttons.append([Button.inline(f"{c.name} ({status})", f"campaign_details_{c.id}")])
        buttons.append([Button.inline("Create Campaign", b"create_campaign")])
        buttons.append([Button.inline("Back", b"dashboard")])
        try:
            await event.edit(text, buttons=buttons)
        except errors.MessageNotModifiedError:
            pass

    async def _show_campaign_details(self, event, campaign_id: str):
        camp = self.campaigns.get(campaign_id)
        if not camp:
            await event.edit("Campaign not found.", buttons=[[Button.inline("Back", b"campaigns")]])
            return
        sender = await event.get_sender()
        user_id = sender.id if sender else event.sender_id
        is_running = any(key == camp.id or key.endswith(f"_{campaign_id}") for key in self.thread_manager.user_tasks.get(user_id, {}))
        status_text = "Running" if is_running else ("Active" if camp.active else "Inactive")
        text = (
            f"Campaign: {camp.name}\n"
            f"Status: {status_text}\n"
            f"Mode: {camp.mode}\n"
            f"Interval: {camp.interval} seconds\n"
            f"Messages: {len(camp.message_ids)}\n"
            f"Source: {camp.source_entity}\n"
        )
        buttons = []
        if camp.active:
            if is_running:
                buttons.append([Button.inline("Stop Campaign", f"stop_campaign_{camp.id}")])
            else:
                buttons.append([Button.inline("Start Campaign", f"start_campaign_{camp.id}")])
            buttons.append([Button.inline("Deactivate", f"deactivate_campaign_{camp.id}")])
        else:
            buttons.append([Button.inline("Activate", f"activate_campaign_{camp.id}")])
        buttons.append([Button.inline("Delete", f"delete_campaign_{campaign_id}")])
        buttons.append([Button.inline("Back", b"campaigns")])
        try:
            await event.edit(text, buttons=buttons)
        except errors.MessageNotModifiedError:
            pass

    async def _confirm_delete_campaign_ui(self, event, campaign_id: str):
        # wrapper used earlier
        await self._confirm_delete(event, "campaign", campaign_id)

    async def _do_delete_campaign(self, event, campaign_id: str):
        user = await event.get_sender()
        user_id = user.id if user else event.sender_id
        # cancel related tasks
        for key in list(self.thread_manager.user_tasks.get(user_id, {}).keys()):
            if key == campaign_id or key.endswith(f"_{campaign_id}"):
                self.thread_manager.cancel_user_task(user_id, key)
        camp = self.campaigns.pop(campaign_id, None)
        if camp:
            await self.db.delete_campaign(campaign_id)
            await event.answer(f"Campaign '{camp.name}' deleted.", alert=True)
        await self._show_campaigns_menu(event)

    async def _toggle_campaign(self, event, campaign_id: str, active: bool):
        camp = self.campaigns.get(campaign_id)
        if not camp:
            return await event.answer("Campaign not found.", alert=True)
        camp.active = active
        if not active:
            # stop tasks
            user = await event.get_sender()
            user_id = user.id if user else event.sender_id
            for key in list(self.thread_manager.user_tasks.get(user_id, {}).keys()):
                if key == campaign_id or key.endswith(f"_{campaign_id}"):
                    self.thread_manager.cancel_user_task(user_id, key)
        await self.db.save_campaign(camp)
        await event.answer(f"Campaign {'activated' if active else 'deactivated'}.", alert=False)
        await self._show_campaign_details(event, campaign_id)

    # ---------- Campaign execution (advanced and safe) ----------
    def _get_targets_sync(self, clients: List[TelegramClient], mode: str):
        """
        Synchronous wrapper to call get_targets within an async loop; used only when running inside user's thread.
        But we provide an async get_targets below and prefer to call it directly.
        """
        raise NotImplementedError("Use async version get_targets")

    async def get_targets(self, clients: List[TelegramClient], mode: str) -> List[Dict[str, Any]]:
        """
        Collects unique target dialogs across a set of clients (group channels and DMs).
        Returns list of targets: {id, entity, title, type} where type is 'group' or 'dm'
        """
        unique: Dict[int, Dict[str, Any]] = {}
        for client in clients:
            try:
                async for dialog in client.iter_dialogs():
                    if dialog.id in unique:
                        continue
                    ent = dialog.entity
                    if not ent:
                        continue
                    ttype = None
                    if isinstance(ent, (Channel, Chat)):
                        ttype = "group"
                    elif isinstance(ent, User) and not getattr(ent, "bot", False) and not getattr(ent, "is_self", False):
                        ttype = "dm"
                    if not ttype:
                        continue
                    if mode == "groups" and ttype != "group":
                        continue
                    if mode == "dms" and ttype != "dm":
                        continue
                    unique[dialog.id] = {"id": dialog.id, "entity": ent, "title": getattr(dialog, "title", str(dialog.id)), "type": ttype}
            except Exception:
                logger.warning("Could not enumerate dialogs from a client", exc_info=True)
        logger.info("Found %d unique targets for mode %s", len(unique), mode)
        return list(unique.values())

    async def forward_message_with_handling(self, client: TelegramClient, account: Account, campaign_id: str, target: Dict[str, Any], message_id: int) -> bool:
        """
        Forwards a single message, returns True on success.
        Handles FloodWait, common errors, logs stats & account state.
        """
        success = False
        error_reason = None
        try:
            camp = self.campaigns[campaign_id]
            # resolve source entity: if stored as int string, cast; else get_entity lazily
            source_val = camp.source_entity
            try:
                # source may be int string of chat id or username. Attempt to resolve to an InputPeer.
                source_entity = await client.get_input_entity(int(source_val))
            except Exception:
                # fallback: try get_entity by identifier (username or channel)
                try:
                    source_entity = await client.get_input_entity(source_val)
                except Exception:
                    source_entity = None

            if source_entity is None:
                error_reason = "source_entity_unavailable"
                raise RuntimeError("Source entity cannot be resolved")

            to_peer = target["entity"]
            # Use ForwardMessagesRequest for explicit forwarding (preserves emojis, formatting, premium)
            await client(ForwardMessagesRequest(from_peer=source_entity, id=[message_id], to_peer=to_peer, drop_author=True, drop_media_captions=False))
            logger.info("[%s] forwarded to '%s' (%s)", account.name, target.get("title"), target.get("type"))
            account.messages_sent += 1
            account.last_used = datetime.utcnow()
            success = True
        except errors.FloodWaitError as e:
            wait = e.seconds + FLOOD_WAIT_BUFFER
            account.status = "flood_wait"
            account.flood_wait_until = datetime.utcnow() + timedelta(seconds=wait)
            error_reason = f"FloodWait:{e.seconds}"
            logger.warning("[%s] FloodWait %ss - pausing account", account.name, e.seconds)
        except (errors.PeerFloodError, errors.UserPrivacyRestrictedError, errors.UserIsBlockedError, errors.InputUserDeactivatedError) as e:
            # Non-fatal skip
            error_reason = type(e).__name__
            logger.warning("[%s] skipping %s due to %s", account.name, target.get("title"), error_reason)
        except Exception as e:
            account.errors_count += 1
            error_reason = str(e)
            # detect textual hints of ban/terminated
            if any(k in error_reason.lower() for k in ("banned", "terminated", "deactivated", "deleted")):
                account.status = "banned"
            logger.exception("[%s] failed to forward to %s", account.name, target.get("title"))
        finally:
            # persist stats and account quickly
            try:
                await self.db.log_activity(account.name, campaign_id, target["id"], target["type"], success, error_reason)
                await self.db.save_account(account)
            except Exception:
                logger.exception("Failed to log activity or save account state")
            return success

    async def run_campaign_logic(self, campaign_id: str, account_names: Optional[List[str]] = None):
        """
        Main campaign runner. This function is designed to be scheduled inside the user's dedicated thread loop.
        It initializes clients for the accounts in that thread and runs a forwarding loop until targets exhausted or stopped.
        """
        camp = self.campaigns.get(campaign_id)
        if not camp:
            logger.error("Campaign %s not found", campaign_id)
            return

        user_id = camp.user_id
        # Build account list for this user
        user_accounts = [a for a in self._get_user_accounts(user_id)]
        accounts_to_use = user_accounts
        if account_names:
            accounts_to_use = [a for a in user_accounts if a.name in account_names]

        # initialize clients inside the user's thread
        active_clients: Dict[str, TelegramClient] = {}
        for acc in accounts_to_use:
            if acc.status != "active":
                continue
            ok = await self._init_account_client_in_thread(acc, user_id)
            if ok:
                active_clients[acc.name] = self.thread_manager.user_clients[user_id][acc.name]

        if not active_clients:
            logger.warning("No active accounts available for campaign %s", camp.name)
            return

        targets = await self.get_targets(list(active_clients.values()), camp.mode)
        if not targets:
            logger.warning("No targets found for campaign %s", camp.name)
            return

        random.shuffle(targets)
        client_names = list(active_clients.keys())
        logger.info("Starting campaign %s with %d accounts and %d targets", camp.name, len(client_names), len(targets))

        i = 0
        while i < len(targets):
            target = targets[i]
            # choose next available account
            # skip accounts that are not active or in flood_wait
            available_accounts = [n for n in client_names if self.accounts[n].status == "active"]
            if not available_accounts:
                logger.warning("All accounts paused or banned during campaign %s", camp.name)
                break
            # round-robin
            account_name = available_accounts[i % len(available_accounts)]
            account = self.accounts[account_name]
            client = active_clients[account_name]

            # Re-check flood wait expiry
            if account.status == "flood_wait" and account.flood_wait_until and datetime.utcnow() >= account.flood_wait_until:
                account.status = "active"
                account.flood_wait_until = None
                await self.db.save_account(account)

            if account.status != "active":
                # skip this account
                client_names = [n for n in client_names if n != account_name]
                if not client_names:
                    logger.warning("No active accounts left for campaign %s", camp.name)
                    break
                continue

            # Acquire a per-account semaphore within the thread (sequential per account)
            # (we are already running inside the thread's loop; use normal asyncio primitives)
            try:
                # forward chosen message id
                message_id = random.choice(camp.message_ids)
                await self.forward_message_with_handling(client, account, campaign_id, target, message_id)
            except asyncio.CancelledError:
                logger.info("Campaign %s cancelled", camp.name)
                break
            except Exception:
                logger.exception("Unexpected error in campaign loop for %s", camp.name)

            # wait interval + jitter
            delay = camp.interval + random.uniform(*FORWARD_JITTER)
            await asyncio.sleep(delay)
            i += 1

        logger.info("Campaign %s finished.", camp.name)

    async def _start_campaign_task(self, event, campaign_id: str, account_names: Optional[List[str]] = None):
        sender = await event.get_sender()
        user_id = sender.id if sender else event.sender_id
        # check if campaign exists and active
        camp = self.campaigns.get(campaign_id)
        if not camp:
            return await event.answer("Campaign not found.", alert=True)
        if not camp.active:
            return await event.answer("Campaign must be activated before it can be started.", alert=True)

        # create a unique task key: if account_names provided, prefix with account
        if account_names:
            task_key = f"{account_names[0]}_{campaign_id}"
            if task_key in self.thread_manager.user_tasks.get(user_id, {}):
                return await event.answer("Campaign is already running on this account.", alert=True)
        else:
            task_key = campaign_id
            if task_key in self.thread_manager.user_tasks.get(user_id, {}):
                return await event.answer("Campaign is already running globally.", alert=True)

        # Create a user thread task
        # run_campaign_logic itself must run inside the user's thread loop. We call thread_manager.create_user_task
        self.thread_manager.create_user_task(user_id, task_key, self.run_campaign_logic(campaign_id, account_names))
        msg = f"Campaign '{camp.name}' started."
        if account_names:
            msg += f" on account {account_names[0]}."
        else:
            msg += " on all available accounts."
        await event.answer(msg, alert=False)
        await self._show_campaign_details(event, campaign_id)

    async def _stop_campaign_task(self, event, campaign_id: str, account_names: Optional[List[str]] = None):
        sender = await event.get_sender()
        user_id = sender.id if sender else event.sender_id
        if account_names:
            task_key = f"{account_names[0]}_{campaign_id}"
            if task_key in self.thread_manager.user_tasks.get(user_id, {}):
                self.thread_manager.cancel_user_task(user_id, task_key)
                await event.answer(f"Campaign stopped on {account_names[0]}.", alert=False)
            else:
                await event.answer("Campaign was not running on this account.", alert=True)
        else:
            stopped = 0
            for key in list(self.thread_manager.user_tasks.get(user_id, {}).keys()):
                if key == campaign_id or key.endswith(f"_{campaign_id}"):
                    self.thread_manager.cancel_user_task(user_id, key)
                    stopped += 1
            await event.answer(f"Stopped {stopped} running task(s).", alert=False)
        await self._show_campaign_details(event, campaign_id)

    # ---------- Group joining flow ----------
    async def _initiate_group_join(self, event):
        sender = await event.get_sender()
        user_id = sender.id if sender else event.sender_id
        if not any(acc.status == "active" for acc in self._get_user_accounts(user_id)):
            return await event.answer("You need at least one active account to join groups.", alert=True)
        self.user_state[user_id] = {"action": "awaiting_group_links", "groups": []}
        text = "Send group links or usernames (e.g., @groupname or t.me/groupname). Send each as separate message. Type 'done' when finished."
        try:
            await event.edit(text, buttons=[[Button.inline("Cancel", b"accounts")]])
        except errors.MessageNotModifiedError:
            pass

    async def _handle_group_join_input(self, event):
        sender = await event.get_sender()
        user_id = sender.id if sender else event.sender_id
        state = self.user_state.get(user_id)
        if not state:
            return
        text = (event.raw_text or "").strip()
        if text.lower() == "done":
            groups = state.get("groups", [])
            if not groups:
                return await event.reply("Please add at least one group.")
            task_key = f"join_groups_{user_id}"
            if task_key in self.thread_manager.user_tasks.get(user_id, {}):
                return await event.reply("A group join task is already in progress.")
            # schedule group join task inside user's thread
            self.thread_manager.create_user_task(user_id, task_key, self.run_group_join_logic(user_id, groups))
            await event.reply(f"Started joining {len(groups)} groups.")
            self.user_state.pop(user_id, None)
        else:
            if text:
                state.setdefault("groups", []).append(text)
                await event.reply(f"Added {text}. Send another or type 'done'.")
            else:
                await event.reply("Invalid input. Send a username or link.")

    async def run_group_join_logic(self, user_id: int, groups_to_join: List[str]):
        """
        Runs in the user's dedicated thread loop. Uses thread-local clients that were initialized.
        Each account will attempt to join groups in round-robin fashion.
        """
        # get user's accounts
        user_accounts = [a for a in self._get_user_accounts(user_id) if a.status == "active"]
        if not user_accounts:
            # send message to user via bot
            if self.bot:
                try:
                    await self.bot.send_message(user_id, "Group join failed: no active accounts.")
                except Exception:
                    pass
            return

        # initialize clients in thread
        active_clients = {}
        for acc in user_accounts:
            if await self._init_account_client_in_thread(acc, user_id):
                active_clients[acc.name] = self.thread_manager.user_clients[user_id][acc.name]

        if not active_clients:
            if self.bot:
                try:
                    await self.bot.send_message(user_id, "Group join failed: could not initialize any account clients.")
                except Exception:
                    pass
            return

        client_names = list(active_clients.keys())
        joined, failed = 0, 0
        # notify user
        status_msg = None
        if self.bot:
            try:
                status_msg = await self.bot.send_message(user_id, f"Starting to join {len(groups_to_join)} groups...")
            except Exception:
                status_msg = None

        for idx, group_identifier in enumerate(groups_to_join):
            try:
                client_name = client_names[idx % len(client_names)]
                client = active_clients[client_name]
                try:
                    # private invite link?
                    if "joinchat/" in group_identifier or "t.me/+" in group_identifier or "t.me/joinchat/" in group_identifier:
                        if "t.me/+" in group_identifier:
                            invite_hash = group_identifier.split("+")[-1]
                        else:
                            invite_hash = group_identifier.split("/")[-1]
                        await client(ImportChatInviteRequest(invite_hash))
                        logger.info("[%s] joined private invite %s", client_name, group_identifier)
                    else:
                        username = group_identifier.strip().lstrip("@")
                        await client(JoinChannelRequest(username))
                        logger.info("[%s] joined public group %s", client_name, username)
                    joined += 1
                except errors.UserAlreadyParticipantError:
                    joined += 1
                except Exception:
                    logger.exception("Failed to join %s with %s", group_identifier, client_name)
                    failed += 1

                if status_msg and ((idx + 1) % 5 == 0 or idx + 1 == len(groups_to_join)):
                    try:
                        await status_msg.edit(f"Progress: {idx+1}/{len(groups_to_join)}\nJoined: {joined}\nFailed: {failed}")
                    except errors.MessageNotModifiedError:
                        pass
                await asyncio.sleep(random.uniform(*JOIN_GROUP_BATCH_DELAY))
            except asyncio.CancelledError:
                logger.info("Group join task cancelled for user %s", user_id)
                break
            except Exception:
                logger.exception("Unexpected error in join group loop")
                failed += 1

        if status_msg:
            try:
                await status_msg.edit(f"Group joining complete.\nTotal Joined: {joined}\nTotal Failed: {failed}")
            except Exception:
                pass

    # ---------- statistics menu ----------
    async def _show_stats_menu(self, event):
        sender = await event.get_sender()
        user_id = sender.id if sender else event.sender_id
        account_names = [a.name for a in self._get_user_accounts(user_id)]
        stats = await self.db.get_user_statistics(account_names)
        total_attempts = stats["total_sent"] + stats["total_failed"]
        success_rate = (stats["total_sent"] / total_attempts * 100) if total_attempts > 0 else 0
        text = (
            "Your Statistics\n\n"
            f"Total Sent: {stats['total_sent']}\n"
            f"Total Failed: {stats['total_failed']}\n"
            f"Success Rate: {success_rate:.2f}%\n"
            f"Messages Sent Today: {stats['today_sent']}\n\n"
            "Per Account:\n"
        )
        if stats["by_account"]:
            for acc_name, acc_stats in sorted(stats["by_account"].items()):
                text += f" - {acc_name}: {acc_stats['sent']}/{acc_stats['failed']}\n"
        else:
            text += " No activity recorded yet.\n"
        buttons = [[Button.inline("Refresh", b"statistics")], [Button.inline("Back", b"dashboard")]]
        try:
            await event.edit(text, buttons=buttons)
        except errors.MessageNotModifiedError:
            pass


# ---------------------------
# Entrypoint
# ---------------------------


async def main():
    bot = TelegramAdBot()
    try:
        await bot.start()
    except KeyboardInterrupt:
        logger.info("Interrupted by user. Shutting down.")
        await bot.shutdown()
    except Exception:
        logger.exception("Fatal error in bot execution")
        try:
            await bot.shutdown()
        except Exception:
            pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received. Exiting.")
