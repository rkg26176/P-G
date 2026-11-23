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
from typing import Callable, Optional
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
BOT_TOKEN = "8880114377:AAEP0yHWvebq-1FdFFgH7XMzeBCSXIhDK48"

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

CLAIMS_DATA_FILE = "claimed_coupons.json"  # Direct Mobile और Firebase दोनों से क्लेम हुए कूपन का डेटा
MAX_DIRECT_LINKS = 20  # 20 से ज़्यादा links पर .txt या .zip file मांगेगा
MAX_CONCURRENT_REQUESTS = 10  # एक साथ 10 URLs check करने की speed
MAX_FIREBASE_UPI_USES = 8  # एक फ्रेश UPI ID के लिए फिक्स यूज़ लिमिट
MAX_SEND_RETRIES = 2  # एक नंबर पर अधिकतम 2 बार OTP भेजने का प्रयास
WAIT_SECONDS = 5  # हर प्रयास के बाद 5 सेकंड का इंतजार

file_lock = asyncio.Lock()

# Conversation States
WAITING_FOR_FIREBASE_INPUT = 1
WAITING_FOR_DIRECT_MOBILE_FLOW = 2
WAITING_FOR_FIREBASE_COUPONS = 3
WAITING_FOR_FIREBASE_UPI = 4

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


async def get_unjoined_channels(bot, user_id: int) -> list[dict]:
    """चेक करता है कि यूज़र ने कौन-कौन से चैनल्स/ग्रुप्स जॉइन नहीं किए हैं।"""
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
    """सिर्फ बाकी बचे हुए चैनल्स के बटन और Verify बटन बनाता है।"""
    keyboard = []
    for ch in unjoined_channels:
        keyboard.append([InlineKeyboardButton(text=ch["name"], url=ch["url"])])
    keyboard.append([InlineKeyboardButton(text="✅ Verify Membership", callback_data="verify_join")])
    return InlineKeyboardMarkup(keyboard)


async def ensure_user_subscribed(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """अगर यूज़र ने सभी चैनल्स जॉइन नहीं किए हैं तो Access Denied दिखाकर जॉइन बटन भेजता है।"""
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id

    unjoined = await get_unjoined_channels(context.bot, user_id)
    if not unjoined:
        return True

    await close_user_session(chat_id)
    reply_markup = build_join_keyboard(unjoined)
    msg_text = (
        "🚫 **Access Denied!**\n\n"
        "Bot ko use karne ke liye neeche diye gaye sabhi channels aur group join karna compulsory hai.\n"
        "Join karne ke baad **'✅ Verify Membership'** par click karein:"
    )
    if update.callback_query:
        try:
            await update.callback_query.edit_message_text(msg_text, reply_markup=reply_markup, parse_mode="Markdown")
        except Exception:
            pass
    elif update.message:
        await update.message.reply_text(msg_text, reply_markup=reply_markup, parse_mode="Markdown")
    return False


async def get_shared_browser(headless: bool = True) -> Browser:
    """एक ही Global Chromium Browser instance को initialize और reuse करता है।"""
    global global_playwright, global_browser
    async with browser_init_lock:
        if global_browser and global_browser.is_connected():
            return global_browser

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
    Global Chromium Browser के अंदर प्रत्येक यूज़र के लिए isolated BrowserContext
    का उपयोग करके multi-step form/API workflow और डायनामिक टोकन को हैंडल करता है।
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
        """Shared Browser में नया isolated Context और Page initialize करता है।"""
        browser = await get_shared_browser(headless=self.headless)

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

        self.context = await browser.new_context(**context_kwargs)

        await self.context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )

        self.page = await self.context.new_page()

    async def load_initial_page(self) -> dict:
        """Main page load करता है, cookies set करता है, और CSRF token व state options निकालता है।"""
        if not self.page:
            raise RuntimeError("Session start nahi hua hai. Pehle start() call karein.")

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
        DOM ([data-sitekey]), iframe (?k=), या script से site_key ऑटो-डिटेक्ट करके
        reCAPTCHA token निकालता है।
        """
        if not self.page:
            return ""

        token = await self.page.evaluate(
            """async ({ fallbackKey, actionName }) => {
                let detectedKey = "";

                // 1. DOM से data-sitekey ऑटो-डिटेक्ट करें
                const keyEl = document.querySelector('[data-sitekey]');
                if (keyEl && keyEl.getAttribute('data-sitekey')) {
                    detectedKey = keyEl.getAttribute('data-sitekey');
                }

                // 2. iframe URL से 'k' parameter निकालें
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

                // 3. Script tag के render parameter से की (key) चेक करें
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

                // 4. v3 grecaptcha available है तो token generate करें
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

                // 5. Fallback: DOM textarea/input चेक करें
                const el = document.querySelector('textarea[name="g-recaptcha-response"], input[name="g-recaptcha-response"]');
                return el && el.value ? el.value : "";
            }""",
            {"fallbackKey": fallback_site_key, "actionName": action},
        )
        return token

    async def post_step(self, endpoint: str, form_data: dict) -> dict:
        """
        Browser page के native fetch() का उपयोग करके रिक्वेस्ट भेजता है
        ताकि असली cookies, User-Agent, और sec-ch-ua headers ऑटोमैटिक सिंक रहें।
        """
        if not self.page:
            raise RuntimeError("Page available nahi hai.")

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
        """सिर्फ इस सेशन का Page और Context cleanly close करता है (Global Browser चलता रहेगा)।"""
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
    POST रिक्वेस्ट भेजता है, HTTP स्टेटस कोड (200 OK) और HTML/WAF रिस्पॉन्स की जांच करता है।
    """
    res = await client.post_step(url, payload)

    if res["status"] != 200:
        raise RuntimeError(f"HTTP {res['status']}: Server ne request reject kar di.")

    data = res["data"]
    if isinstance(data, dict) and "raw" in data and len(data) == 1:
        raw_content = data["raw"].strip()
        if raw_content.startswith("<") or "<html" in raw_content.lower():
            raise RuntimeError("Server ya security firewall (WAF) ne session block kar diya hai ya session expire ho gaya hai.")
        raise RuntimeError("Server se invalid (Non-JSON) response mila hai.")

    return data


async def close_user_session(chat_id: int):
    """मौजूदा सेशन को सुरक्षित रूप से बंद और डिलीट करता है"""
    session = user_sessions.pop(chat_id, None)
    if session and "client" in session:
        await session["client"].close()


async def cleanup_loop():
    """हर 60 सेकंड में निष्क्रिय (inactive) सेशन्स को बंद करके RAM लीक रोकता है।"""
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
    """बॉट स्टार्ट होने पर Left Side Blue Menu Button में शॉर्टकट कमांड्स सेट करता है, और Global Browser व क्लीनअप टास्क चालू करता है।"""
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
    """बॉट बंद होने पर सारे सेशन्स और Global Browser cleanly बंद करता है।"""
    global global_playwright, global_browser, cleanup_task

    if cleanup_task and not cleanup_task.done():
        cleanup_task.cancel()
        try:
            await cleanup_task
        except asyncio.CancelledError:
            pass

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
    """पुराने सेशन को बंद करके नया UnifiedSessionClient बनाता है (प्रॉक्सी फॉलबैक और हार्ड टाइमआउट सहित)"""
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
# 3. FIREBASE STATUS CHECKER
# ==========================================
def check_firebase_sms_status(firebase_url: str) -> str:
    """Firebase URL के node पर जाकर active/inactive status check करता है।"""
    base_url = firebase_url.rstrip('/')
    if not base_url.endswith('.json'):
        target_url = f"{base_url}/sms.json"
    else:
        target_url = base_url

    try:
        response = requests.get(target_url, timeout=15)
        response.raise_for_status()
        data = response.json()

        if not data:
            return "Inactive (Database खाली है)"

        timestamps = []
        items = data.values() if isinstance(data, dict) else data

        for item in items:
            if isinstance(item, dict):
                ts = item.get('timestamp') or item.get('date') or item.get('time')
                if ts is not None:
                    try:
                        timestamps.append(float(ts))
                    except (ValueError, TypeError):
                        continue

        if not timestamps:
            return "Inactive (कोई valid timestamp नहीं मिला)"

        latest_ts = max(timestamps)
        latest_ts_sec = latest_ts / 1000.0 if latest_ts > 1e11 else latest_ts

        current_time_sec = time.time()
        time_diff = current_time_sec - latest_ts_sec

        if 0 <= time_diff <= 600:
            return f"Active (आखिरी SMS {int(time_diff)} सेकंड पहले आया था)"
        else:
            return f"Inactive (आखिरी SMS {int(time_diff // 60)} मिनट पहले आया था)"

    except requests.exceptions.HTTPError as e:
        if e.response is not None and e.response.status_code == 401:
            return "Error: Permission Denied (Firebase rules private हैं)"
        return f"HTTP Error: {e}"
    except Exception as e:
        return f"Error: {e}"


# ==========================================
# 4. EXTRACTION, FILE & CLAIM LEADERBOARD HELPERS
# ==========================================
def extract_urls_from_text(text: str) -> list[str]:
    """Text में से सभी clean और unique URLs निकालता है।"""
    url_pattern = r'https?://[^\s<>"]+|www\.[^\s<>"]+'
    raw_urls = re.findall(url_pattern, text, re.IGNORECASE)
    cleaned_urls = [url.rstrip('.,;:!?)\']}') for url in raw_urls]
    return list(dict.fromkeys(cleaned_urls))


def extract_firebase_from_url(target_url: str) -> str | None:
    """Direct Firebase URL या Base64 parameter वाले Panel URL से Firebase link निकालता है।"""
    if not target_url.lower().startswith(("http://", "https://")):
        target_url = "https://" + target_url

    parsed = urlparse(target_url)
    params = parse_qs(parsed.query)
    firebase_pattern = r'https?://[a-zA-Z0-9-]+\.(?:firebaseio\.com|firebasedatabase\.app)(?:/[^\s?#]*)?'

    direct_match = re.search(firebase_pattern, target_url, re.IGNORECASE)
    if direct_match:
        return direct_match.group(0).rstrip('/')

    for _, values in params.items():
        for val in values:
            try:
                clean_val = unquote(val).strip()
                padded_val = clean_val + '=' * (-len(clean_val) % 4)
                decoded_bytes = base64.urlsafe_b64decode(padded_val)
                decoded_text = decoded_bytes.decode('utf-8', errors='ignore')

                match = re.search(firebase_pattern, decoded_text, re.IGNORECASE)
                if match:
                    return match.group(0).rstrip('/')
            except Exception:
                continue

    return None


def filter_valid_firebase_urls(urls: list[str]) -> tuple[list[tuple[str, str]], int]:
    """
    केवल valid Firebase/Panel URLs को ही फिल्टर करके रखता है।
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


def extract_numbers_and_otps(text: str) -> dict:
    """Text या SMS में से 10-digit Indian mobile numbers और 4-8 digit OTPs निकालता है।"""
    number_pattern = r'(?<!\d)(?:\+91[\-\s]?|91[\-\s]?|0)?([6-9]\d{9})(?!\d)'
    otp_pattern = r'(?:OTP|code|pin|passcode|password|verification|ओटीपी|कोड)[^\d\n]{0,25}(\d{4,8})\b'

    numbers = list(dict.fromkeys(re.findall(number_pattern, text)))
    otps = list(dict.fromkeys(re.findall(otp_pattern, text, re.IGNORECASE)))

    return {
        "numbers": numbers,
        "otps": otps,
    }


def extract_coupon_codes_from_text(text: str) -> list[str]:
    """Text या .txt file से unique coupon codes निकालता है (single, sequence या line-by-line)।"""
    raw_tokens = re.split(r'[\s,;|]+', text.strip())
    coupons = []
    for token in raw_tokens:
        clean_code = re.sub(r'^[^A-Za-z0-9]+|[^A-Za-z0-9]+$', '', token).upper()
        if len(clean_code) >= 4 and clean_code.isalnum():
            if clean_code not in coupons:
                coupons.append(clean_code)
    return coupons


def is_valid_upi_format(upi_id: str) -> bool:
    """UPI ID का strict format (username@bankhandle) चेक करता है।"""
    upi_pattern = r'^[a-zA-Z0-9][a-zA-Z0-9.\-_]{1,254}@[a-zA-Z0-9]{2,64}$'
    return bool(re.match(upi_pattern, upi_id))


def _is_firebase_node_offline(node: dict) -> bool:
    """चेक करता है कि Firebase का डिवाइस/सेशन नोड offline तो नहीं है (offline सेशन पर OTP नहीं मिलता)।"""
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
    """चेक करता है कि Firebase का डिवाइस/सेशन नोड स्पष्ट रूप से online है या नहीं।"""
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

    # अगर node में recent lastSeen / ping / timestamp (10 मिनट के अंदर) है तो online मानें
    for ts_key in ("lastSeen", "last_seen", "ping", "heartbeat", "updatedAt", "timestamp"):
        ts = node.get(ts_key)
        if ts is not None:
            try:
                ts_val = float(ts)
                ts_sec = ts_val / 1000.0 if ts_val > 1e11 else ts_val
                if 0 <= (time.time() - ts_sec) <= 600:
                    return True
            except (ValueError, TypeError):
                continue

    return False


def _extract_online_numbers_from_tree(obj, inherited_offline: bool = False, inherited_online: bool = False) -> list[str]:
    """
    Firebase JSON tree को स्कैन करता है:
    - 'offline' सेशन/ब्रांच को पूरी तरह स्किप करता है।
    - केवल 'online' सेशन/डिवाइस से ही 10-digit Indian mobile numbers निकालता है।
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

            branch_online = node_online or ("online" in key_lower)

            # अगर key खुद एक 10-digit मोबाइल नंबर है और यह सेशन online है
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
            elif branch_online and val is not None:
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


def get_online_numbers(firebase_url: str) -> list[dict]:
    """
    Firebase से केवल उन्हीं नंबरों की लिस्ट निकालता है जिनका status 'online' है।
    नोट: अपने Firebase के नोड (node) नाम के अनुसार '/numbers.json' को बदल लें।
    """
    base = firebase_url.rstrip('/')
    root_url = base[:-5] if base.endswith('.json') else base
    try:
        res = requests.get(f"{root_url}/numbers.json", timeout=10)
        if res.status_code == 200 and res.json():
            data = res.json()
            online_list = []

            # यदि डेटा dictionary (dict) फॉर्मेट में है
            if isinstance(data, dict):
                for key, info in data.items():
                    if isinstance(info, dict) and str(info.get("status", "")).lower() == "online":
                        # नंबर या उसकी key को सेव करें
                        num = info.get("number") or info.get("phone") or key
                        online_list.append({"id": key, "number": str(num)})
            return online_list
    except Exception as e:
        print(f"  [Error] Firebase से नंबर लाने में समस्या ({root_url}): {e}")
    return []


def trigger_otp_send(number: str, otp_sender_fn: Optional[Callable[[str], bool]] = None) -> bool:
    """
    दिए गए मोबाइल नंबर पर वास्तविक OTP भेजने वाले callback (otp_sender_fn) को कॉल करता है।
    (Woohoo पर वास्तविक OTP भेजने का पूरा लॉजिक auto_claim_triplet_on_woohoo में संभाला जाता है।)
    """
    print(f"  -> {number} पर OTP भेजा जा रहा है...")
    if callable(otp_sender_fn):
        try:
            return bool(otp_sender_fn(number))
        except Exception as e:
            print(f"  [Error] {number} पर OTP भेजने में समस्या: {e}")
            return False
    return False


def check_otp_in_firebase(firebase_url: str, number_id: str) -> Optional[str]:
    """
    5 सेकंड तक हर 1 सेकंड में Firebase चेक करता है कि उस नंबर पर नया OTP आया या नहीं।
    """
    base = firebase_url.rstrip('/')
    root_url = base[:-5] if base.endswith('.json') else base
    for _ in range(WAIT_SECONDS):
        try:
            # अपने Firebase के OTP पाथ के अनुसार इसे बदलें
            res = requests.get(f"{root_url}/numbers/{number_id}/otp.json", timeout=5)
            if res.status_code == 200:
                otp_val = res.json()
                # यदि OTP मौजूद है और खाली नहीं है
                if otp_val and str(otp_val).strip() != "":
                    return str(otp_val).strip()
        except Exception:
            pass
        time.sleep(1)  # 1 सेकंड रुककर दोबारा चेक करें (कुल 5 सेकंड तक)
    return None


def clear_old_otp(firebase_url: str, number_id: str) -> None:
    """नया OTP भेजने से पहले पुराने OTP को साफ करता है ताकि पुराना OTP रीड न हो।"""
    base = firebase_url.rstrip('/')
    root_url = base[:-5] if base.endswith('.json') else base
    try:
        requests.patch(f"{root_url}/numbers/{number_id}.json", json={"otp": ""}, timeout=5)
    except Exception:
        pass


def process_all_panels(
    firebase_urls: list[str],
    used_numbers: Optional[list[str]] = None,
    otp_sender_fn: Optional[Callable[[str], bool]] = None,
) -> Optional[dict]:
    """
    सभी Active Firebase URLs और उनके 'online' नंबरों को चेक करता है:
    - प्रत्येक online नंबर पर अधिकतम MAX_SEND_RETRIES (2) बार प्रयास करता है
    - प्रत्येक प्रयास पर पुराना OTP साफ करके नया OTP ट्रिगर करता है और WAIT_SECONDS (5s) तक चेक करता है
    - OTP न मिलने पर अगले online नंबर और फिर अगले Firebase URL पर स्विच करता है।
    """
    if used_numbers is None:
        used_numbers = []

    for fb_url in firebase_urls:
        print(f"\n==========================================")
        print(f"पैनल चेक किया जा रहा है: {fb_url}")

        online_numbers = get_online_numbers(fb_url)
        if not online_numbers:
            print("-> इस पैनल में कोई भी नंबर 'online' नहीं है। अगले URL पर जा रहे हैं...")
            continue

        print(f"-> कुल {len(online_numbers)} ऑनलाइन नंबर मिले।")

        # पहले उन online नंबरों को प्राथमिकता दें जो पहले यूज़ नहीं हुए हैं
        fresh_items = [it for it in online_numbers if it["number"] not in used_numbers]
        ordered_items = fresh_items if fresh_items else online_numbers

        for item in ordered_items:
            num_id = item["id"]
            phone = item["number"]
            print(f"\n[सक्रिय नंबर]: {phone} (ID: {num_id})")

            # एक नंबर पर अधिकतम 2 बार प्रयास का लूप
            for attempt in range(1, MAX_SEND_RETRIES + 1):
                print(f"  प्रयास {attempt}/{MAX_SEND_RETRIES}:")

                # पुराना OTP साफ करें और नया OTP ट्रिगर करें
                clear_old_otp(fb_url, num_id)
                sent_ok = trigger_otp_send(phone, otp_sender_fn=otp_sender_fn)
                if not sent_ok:
                    print(f"  [स्किप] {phone} पर OTP ट्रिगर नहीं हो पाया।")
                    break

                print(f"  {WAIT_SECONDS} सेकंड तक OTP का इंतजार किया जा रहा है...")
                otp = check_otp_in_firebase(fb_url, num_id)

                if otp:
                    print(f"\n[सफलता!] नंबर {phone} पर OTP मिल गया: {otp}")
                    return {"url": fb_url, "id": num_id, "number": phone, "otp": otp}

                print(f"  [फेल] {WAIT_SECONDS} सेकंड में OTP नहीं मिला।")

            print(f"-> नंबर {phone} पर {MAX_SEND_RETRIES} बार कोशिश के बाद भी OTP नहीं मिला। अगले ऑनलाइन नंबर पर स्विच कर रहे हैं...")

        print(f"\n-> पैनल ({fb_url}) के सभी ऑनलाइन नंबर खत्म हो गए। अगले Firebase URL पर जा रहे हैं...")

    print("\n[समाप्त] सभी Firebase URLs और ऑनलाइन नंबर चेक हो चुके हैं।")
    return None


def _fetch_numbers_from_single_firebase_sync(firebase_url: str) -> list[str]:
    """एक Active Firebase endpoint के केवल Online सेशन से 10-digit Indian mobile numbers निकालता है (Offline को स्किप करके)।"""
    base = firebase_url.rstrip('/')
    root_url = base[:-5] if base.endswith('.json') else base
    urls_to_check = [f"{root_url}/.json", f"{root_url}/sms.json"]

    found_numbers = []

    for url in urls_to_check:
        try:
            resp = requests.get(url, timeout=12)
            if resp.status_code != 200:
                continue
            data = resp.json()
            if not data:
                continue

            # 1. पहले केवल स्पष्ट रूप से 'online' सेशन वाले नंबर्स निकालें (offline को छोड़कर)
            online_nums = _extract_online_numbers_from_tree(data, inherited_offline=False, inherited_online=False)
            for num in online_nums:
                if num not in found_numbers:
                    found_numbers.append(num)

            # 2. अगर स्ट्रक्चर में explicit 'online' टैग नहीं है, तब भी 'offline' को हटाकर ही नंबर्स लें
            if not found_numbers and isinstance(data, dict):
                filtered_data = {
                    k: v
                    for k, v in data.items()
                    if "offline" not in str(k).lower()
                    and not (isinstance(v, dict) and _is_firebase_node_offline(v))
                }
                fallback_nums = _extract_online_numbers_from_tree(
                    filtered_data,
                    inherited_offline=False,
                    inherited_online=True,
                )
                for num in fallback_nums:
                    if num not in found_numbers:
                        found_numbers.append(num)
        except Exception:
            continue

    return found_numbers


async def fetch_random_active_number_from_panels(context: ContextTypes.DEFAULT_TYPE) -> Optional[dict]:
    """
    सेशन में सेव हुए Active Firebase Panels से पहले /numbers.json के online नंबर निकालता है;
    यदि /numbers.json नोड न मिले तो ट्री स्कैन से online एक्टिव नंबर फेच करता है ताकि
    auto_claim_triplet_on_woohoo में Woohoo पर असली OTP भेजा जा सके।
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


def _poll_otp_for_number_sync(firebase_url: str, number: str, number_id: Optional[str], baseline_otps: list[str]) -> Optional[str]:
    """
    5 सेकंड तक हर 1 सेकंड में Firebase से उस नंबर के लिए नया OTP चेक करता है
    (पहले /numbers/{number_id}/otp.json और फिर पूरे Firebase JSON में से)।
    """
    base = firebase_url.rstrip('/')
    root_url = base[:-5] if base.endswith('.json') else base

    for _ in range(WAIT_SECONDS):
        # 1. यदि number_id उपलब्ध है तो सीधे /numbers/{number_id}/otp.json चेक करें
        if number_id:
            try:
                res = requests.get(f"{root_url}/numbers/{number_id}/otp.json", timeout=5)
                if res.status_code == 200:
                    otp_val = res.json()
                    if otp_val and str(otp_val).strip() != "":
                        clean_otp = str(otp_val).strip()
                        if clean_otp not in baseline_otps:
                            return clean_otp
            except Exception:
                pass

        # 2. अन्य Firebase स्ट्रक्चर (/sms.json या /.json) से नया OTP निकालें
        for node_url in (f"{root_url}/sms.json", f"{root_url}/.json"):
            try:
                res = requests.get(node_url, timeout=6)
                if res.status_code == 200 and res.json():
                    raw_dump = json.dumps(res.json(), ensure_ascii=False)
                    extracted = extract_numbers_and_otps(raw_dump)
                    for candidate_otp in reversed(extracted.get("otps", [])):
                        if candidate_otp not in baseline_otps:
                            return candidate_otp
            except Exception:
                continue

        time.sleep(1)
    return None


def _get_baseline_otps_sync(firebase_url: str) -> list[str]:
    """OTP भेजने से पहले Firebase में पहले से मौजूद पुराने OTPs की लिस्ट लेता है।"""
    base = firebase_url.rstrip('/')
    root_url = base[:-5] if base.endswith('.json') else base
    existing = []
    for node_url in (f"{root_url}/sms.json", f"{root_url}/.json"):
        try:
            res = requests.get(node_url, timeout=6)
            if res.status_code == 200 and res.json():
                raw_dump = json.dumps(res.json(), ensure_ascii=False)
                for o in extract_numbers_and_otps(raw_dump).get("otps", []):
                    if o not in existing:
                        existing.append(o)
        except Exception:
            continue
    return existing


async def auto_claim_triplet_on_woohoo(
    context: ContextTypes.DEFAULT_TYPE,
    coupon_code: str,
    upi_id: str,
) -> dict:
    """
    जब Online Number + Coupon + UPI का पेयर बनता है:
    1. UPI ID को साइड में सुरक्षित रखता है और आखिरी स्टेप (Step 3) में ऑटोमेटिक सबमिट करता है।
    2. एक ही Coupon Code को तब तक पकड़कर रखता है जब तक वह सक्सेसफुली क्लेम न हो जाए।
    3. Active Firebase Panels के Online Numbers पर Woohoo Step 1 (Send OTP) ट्रिगर करता है,
       5 सेकंड वेट के साथ अधिकतम 2 बार रीट्राई करता है, और OTP न मिलने पर अगला Online Number उठाता है।
    4. Firebase से OTP मिलते ही Step 2 (Verify OTP) और Step 3 (UPI Submit) पूरा करके रिजल्ट लौटाता है।
    """
    session_panels: dict = context.user_data.get("session_panels", {})
    active_endpoints = [
        ep for ep, info in session_panels.items() if info.get("is_active")
    ]
    if not active_endpoints:
        return {"success": False, "reason": "NO_ACTIVE_PANELS", "message": "कोई Active Firebase Panel नहीं मिला।"}

    used_numbers: list[str] = context.user_data.setdefault("used_firebase_numbers", [])
    last_error_msg = "किसी भी Online Number पर OTP या Claim पूरा नहीं हो पाया।"

    for fb_url in active_endpoints:
        # 1. पहले /numbers.json से online नंबर निकालें, नहीं तो ट्री स्कैन से निकालें
        structured_online = await asyncio.to_thread(get_online_numbers, fb_url)
        candidates = []
        if structured_online:
            for item in structured_online:
                candidates.append({"id": item["id"], "number": item["number"]})
        else:
            raw_nums = await asyncio.to_thread(_fetch_numbers_from_single_firebase_sync, fb_url)
            for num in raw_nums:
                candidates.append({"id": None, "number": num})

        if not candidates:
            continue

        fresh_candidates = [c for c in candidates if c["number"] not in used_numbers]
        ordered_candidates = fresh_candidates if fresh_candidates else candidates

        for item in ordered_candidates:
            phone = item["number"]
            num_id = item["id"]

            if phone not in used_numbers:
                used_numbers.append(phone)

            # हर नंबर के लिए नया UnifiedSessionClient सेशन तैयार करें
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
                    last_error_msg = "Website se session token fetch nahi ho paya."
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

                # एक नंबर पर अधिकतम MAX_SEND_RETRIES (2) बार OTP भेजने व 5s चेक करने का लूप
                for attempt in range(1, MAX_SEND_RETRIES + 1):
                    if num_id:
                        await asyncio.to_thread(clear_old_otp, fb_url, num_id)
                    baseline_otps = await asyncio.to_thread(_get_baseline_otps_sync, fb_url)

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
                        if any(k in err_body.lower() for k in ("invalid coupon", "already used", "already claimed", "expired", "not valid")):
                            coupon_invalid = True
                            break
                        continue

                    # 5 सेकंड तक Firebase से इस नंबर का OTP चेक करें
                    received_otp = await asyncio.to_thread(
                        _poll_otp_for_number_sync,
                        fb_url,
                        phone,
                        num_id,
                        baseline_otps,
                    )
                    if received_otp:
                        break

                if coupon_invalid:
                    return {
                        "success": False,
                        "reason": "COUPON_INVALID",
                        "message": f"Coupon `{coupon_code}` Error: {last_error_msg}",
                    }

                # यदि 2 बार ट्राई करने पर भी OTP नहीं मिला तो इसी कूपन के साथ अगले Online नंबर पर स्विच करें
                if not received_otp:
                    last_error_msg = f"Number {phone} par {MAX_SEND_RETRIES} attempts mein OTP nahi mila."
                    continue

                # ------------------------------------------
                # STEP 2: OTP वेरिफिकेशन (ऑटोमेटिक)
                # ------------------------------------------
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
                    last_error_msg = f"OTP Verification Failed ({phone}): {res_2.get('messageBody') or res_2}"
                    continue

                # ------------------------------------------
                # STEP 3: वही 8-बार वाली UPI ID डालकर फाइनल सबमिट
                # ------------------------------------------
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
                    success_msg = res_3.get("messageBody") or "Coupon successfully claimed!"
                    return {
                        "success": True,
                        "number": phone,
                        "endpoint": fb_url,
                        "otp": received_otp,
                        "coupon": coupon_code,
                        "upi_id": upi_id,
                        "server_message": success_msg,
                    }
                else:
                    last_error_msg = f"Final UPI Submit Error: {res_3.get('messageBody') or res_3}"
                    continue

            except Exception as e:
                last_error_msg = str(e)
                continue
            finally:
                await client.close()

    return {
        "success": False,
        "reason": "ALL_NUMBERS_EXHAUSTED",
        "message": last_error_msg,
    }


def pick_unused_firebase_coupon(context: ContextTypes.DEFAULT_TYPE) -> Optional[dict]:
    """
    भेजे हुए कूपन्स में से कोई एक unused coupon उठाता है और उसे used मार्क कर देता है
    ताकि वही कूपन दोबारा यूज़ न हो।
    """
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
    """
    अगर कूपन सक्सेसफुली रिडीम नहीं होता है, तो उसे used लिस्ट से वापस निकालकर
    दोबारा उपलब्ध (available) कूपन्स में डाल देता है।
    """
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
    """
    वर्तमान UPI ID का 1 यूज़ काउंट करता है।
    8 बार सेम UPI ID देने के बाद 'needs_new_upi': True लौटाता है ताकि बॉट सीधे नई UPI ID मांग सके।
    """
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
    """ZIP file के अंदर मौजूद केवल .txt files का text निकालता है।"""
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
                    logger.warning(f"File {file_info.filename} read करने में दिक्कत: {e}")
    return "\n".join(combined_text)


def read_text_from_txt(txt_path: str) -> str:
    """Direct .txt file का पूरा text निकालता है।"""
    with open(txt_path, "r", encoding="utf-8", errors="ignore") as f:
        return f.read()


def _record_coupon_claim_sync(entry: dict):
    """Direct Mobile और Firebase दोनों से सफलतापूर्वक क्लेम हुए कूपन को JSON file में save करता है।"""
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
    """Thread-safe तरीके से सफल कूपन क्लेम (Direct Mobile या Firebase) को save करता है।"""
    async with file_lock:
        await asyncio.to_thread(_record_coupon_claim_sync, entry)


def _get_real_leaderboard_sync() -> list[dict]:
    """claimed_coupons.json से हर यूजर का रियल कूपन क्लेम काउंट निकालता है (Firebase + Direct Mobile दोनों)।"""
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
    """रियल कूपन क्लेम लीडरबोर्ड का फॉर्मेटेड टेक्स्ट तैयार करता है।"""
    async with file_lock:
        leaderboard = await asyncio.to_thread(_get_real_leaderboard_sync)

    if not leaderboard:
        return (
            "🏆 **Coupon Claim Leaderboard**\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "अभी तक किसी भी यूजर ने कोई कूपन क्लेम नहीं किया है।\n"
            "सबसे पहले कूपन क्लेम करके टॉप पर आएं!"
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
    """
    यूजर के current session में सारे checked panels (active/inactive) save करता है।
    ये तब तक saved रहते हैं जब तक यूजर दोबारा /start नहीं दबाता।
    """
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
) -> dict:
    async with semaphore:
        fb_status = await asyncio.to_thread(check_firebase_sms_status, extracted_firebase)

    return {
        "submitted_url": original_url,
        "firebase_endpoint": extracted_firebase,
        "status": fb_status,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


async def process_text_data_for_bot(
    text: str,
    context: ContextTypes.DEFAULT_TYPE,
    is_from_file: bool = False,
) -> tuple[list[str], bool]:
    urls_found = extract_urls_from_text(text)

    if is_from_file:
        if not urls_found:
            return [
                "🚫 **Invalid File Content (Rejected):**\n"
                "इस file में कोई भी link नहीं मिला!\n\n"
                "📌 कृपया केवल **Firebase / Panel links** वाली `.txt` या `.zip` file ही upload करें।"
            ], False

        valid_pairs, rejected_count = filter_valid_firebase_urls(urls_found)
        if not valid_pairs:
            return [
                "🚫 **Invalid Links File (Rejected):**\n"
                f"इस file में `{len(urls_found)}` links मिले, लेकिन **एक भी valid Firebase / Panel link नहीं है**!\n\n"
                "📌 फालतू text या किसी और वेबसाइट के links वाली file स्वीकार नहीं की जाएगी। "
                "केवल वही `.txt` या `.zip` file भेजें जिसमें आपके Firebase/Panel links हों।"
            ], False
    else:
        if len(urls_found) > MAX_DIRECT_LINKS:
            return [
                f"🚫 **Limit Exceeded:** आपने एक साथ `{len(urls_found)}` links भेजे हैं!\n"
                f"⚠️ Direct paste में maximum `{MAX_DIRECT_LINKS}` links ही allowed हैं।\n\n"
                "📄 कृपया इन सभी links को एक **`.txt`** या **`.zip`** file में डालकर यहाँ upload करें।"
            ], False
        valid_pairs, rejected_count = filter_valid_firebase_urls(urls_found)

    extracted_data = extract_numbers_and_otps(text) if not is_from_file else {"numbers": [], "otps": []}
    numbers = extracted_data["numbers"]
    otps = extracted_data["otps"]

    valid_results = []
    output_blocks = []
    has_active = False

    if valid_pairs:
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        tasks = [
            check_single_url_worker(semaphore, orig_url, fb_url)
            for orig_url, fb_url in valid_pairs
        ]
        valid_results = await asyncio.gather(*tasks)

        has_active = any(item["status"].startswith("Active") for item in valid_results)
        total_in_session = save_panels_to_user_session(context, valid_results)

        rejected_note = (
            f"\n⚠️ *({rejected_count} फालतू/non-Firebase links को ignore कर दिया गया)*"
            if rejected_count > 0
            else ""
        )

        output_blocks.append(
            f"✅ **Total {len(valid_results)} Valid Firebase Endpoints मिले और Session में Save हुए:**\n"
            f"📌 *(इस session में कुल saved panels: `{total_in_session}`)*"
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
            "❌ **Firebase Endpoint Not Found:** इन links में कोई valid Firebase endpoint नहीं मिला।"
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
            "⚠️ **No Data Found:** कोई valid mobile number, OTP या Firebase URL नहीं मिला।"
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
    """Bottom menu keyboard: 4 options ऊपर और उनके नीचे Leaderboard का option."""
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
    """Support button के लिए direct Telegram link button."""
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
    "Please select an option below to input your Firebase Panel:\n"
)

PASTE_URL_PROMPT_TEXT = (
    "━━━━━━━━━━━━━━━━━━━━━━\n"
    "📥 **Direct / Bulk / File Firebase Input**\n"
    "━━━━━━━━━━━━━━━━━━━━━━\n\n"
    "Please send your **Firebase URL** directly, paste **multiple URLs in bulk** "
    "(up to 20 links at once), or if you have more than 20 links, upload a **`.txt` or `.zip` file** containing **only those Firebase/Panel links**.\n\n"
    "👇 *Send your links or file below, or tap Cancel to go back:*"
)

COUPON_PROMPT_AFTER_ACTIVE_TEXT = (
    "🎟️ **Active Firebase Panel मिल गया है! अब अपने Coupon Codes भेजें:**\n\n"
    "• आप एक **Single Coupon**, एक साथ **Multiple Coupons** (जैसे 8 coupons sequence/line-by-line), या **`.txt` file** भेज सकते हैं।\n\n"
    "👇 *नीचे अपने कूपन भेजें या Cancel दबाएं:*"
)


def build_fresh_upi_prompt_text(coupon_count: int) -> str:
    return (
        f"✅ **Total `{coupon_count}` Coupon Codes सेव हो गए हैं!**\n\n"
        "💳 **अब एक एकदम Fully Fresh UPI ID भेजें (e.g., `name@upi`):**\n"
        f"⚠️ *यह UPI ID लगातार **{MAX_FIREBASE_UPI_USES} बार** सेम रहेगी। {MAX_FIREBASE_UPI_USES} बार पूरा होते ही आपसे डायरेक्ट नई UPI ID मांगी जाएगी (डुप्लीकेट UPI ID allowed नहीं है)।*\n\n"
        "👇 *नीचे अपनी Fresh UPI ID भेजें या Cancel दबाएं:*"
    )


NEXT_NEW_UPI_PROMPT_TEXT = (
    "🔄 **8 बार यूज़ पूरा हो गया है! अब डायरेक्ट नई Fresh UPI ID भेजें (e.g., `name@upi`):**\n"
    "⚠️ *पुरानी/डुप्लीकेट UPI ID दोबारा स्वीकार नहीं की जाएगी।*"
)

FINAL_PNL_BATCH_COMPLETE_TEXT = (
    "🎉 Process successfully complete ho gaya! Naya session start karne ke liye /pnlclaim bhejein"
)


def build_atoz_claim_log_text(
    triplet_entry: dict,
    upi_slot: dict,
    remaining_coupons: int,
    server_message: str,
) -> str:
    """हर कूपन क्लेम होने पर A-to-Z पूरी डिटेल का लॉग तैयार करता है।"""
    return (
        "✅ **Coupon Successfully Redeemed — Full A-to-Z Claim Log**\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🎟️ **Coupon Code   :** `{triplet_entry['coupon']}`\n"
        f"📱 **Mobile Number :** `{triplet_entry['number']}`\n"
        f"🔑 **Verified OTP  :** `{triplet_entry['otp']}`\n"
        f"💳 **UPI ID        :** `{triplet_entry['upi_id']}` *(Use `{upi_slot['used_count']}/{upi_slot['max_uses']}`)*\n"
        f"🌐 **Firebase Node :** `{triplet_entry['source_endpoint']}`\n"
        f"🕒 **Claimed Time  :** `{triplet_entry['created_at']}`\n"
        f"📊 **Status        :** `Successfully Redeemed`\n"
        f"📩 **Server Reply  :** {server_message}\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📦 **Remaining Unused Coupons:** `{remaining_coupons}` | **UPI Uses Left:** `{upi_slot['remaining_uses']}`"
    )


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
    """मुख्य वेलकम मैसेज और मेनू कीबोर्ड भेजता है।"""
    reset_note = (
        f"\n🔄 *पिछला session ({old_count} panels) साफ़ कर दिया गया है और नया session शुरू हो गया है।*\n"
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

    # दोबारा /start दबाने पर पिछला सारा session data और Playwright session reset हो जाएगा
    await close_user_session(chat_id)
    old_count = len(context.user_data.get("session_panels", {}))
    context.user_data.clear()
    context.user_data["session_panels"] = {}
    context.user_data["session_started_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # सबसे पहले Force Channel Join वेरिफिकेशन चेक करें
    if not await ensure_user_subscribed(update, context):
        return ConversationHandler.END

    await send_welcome_portal(update.message, old_count=old_count)
    return ConversationHandler.END


async def pnlclaim_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """/pnlclaim भेजते ही सीधे Claim Coupon का पहला वाला (Paste Firebase URL Direct) सेशन ट्रिगर करता है।"""
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
    """/claim भेजने पर सीधे Direct Send Your Mobile Number वाला सेशन ट्रिगर करता है।"""
    if not update.message or update.effective_chat.type != "private":
        return ConversationHandler.END

    if not await ensure_user_subscribed(update, context):
        return ConversationHandler.END

    chat_id = update.effective_chat.id

    async with get_user_lock(chat_id):
        status_msg = await update.message.reply_text("⏳ Naya session prepare ho raha hai...")
        token_ok = await init_user_session(chat_id)

        if not token_ok:
            await status_msg.edit_text(
                "⚠️ Website se initial session token fetch nahi ho paya. "
                "Please internet connection check karein ya dobara try karein.",
                reply_markup=get_cancel_to_claim_keyboard(),
            )
            return ConversationHandler.END

        await status_msg.edit_text(
            "🚀 Naya session start ho gaya hai.\nPlease apna 10-digit mobile number bhejein:",
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


async def handle_other_main_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_user_subscribed(update, context):
        return

    text = update.message.text
    if text == "Check Coupon":
        await update.message.reply_text(
            "🚫 **Check Coupon — Temporarily Closed / Not Available**\n\n"
            "यह फीचर फिलहाल अस्थायी रूप से बंद (Temporarily Closed) है।",
            parse_mode=ParseMode.MARKDOWN,
        )
    elif text == "Buy Coupon":
        await update.message.reply_text(
            "🛒 **Buy Coupon**\nThis feature will be configured next.",
            parse_mode=ParseMode.MARKDOWN,
        )
    elif text == "Support":
        await update.message.reply_text(
            "🎧 **Customer Support**\n\n"
            "किसी भी मदद या जानकारी के लिए नीचे दिए गए बटन पर क्लिक करके हमारी सपोर्ट टीम से संपर्क करें:",
            reply_markup=get_support_inline_keyboard(),
            parse_mode=ParseMode.MARKDOWN,
        )
    elif text == "🏆 Leaderboard":
        user_id = update.effective_user.id if update.effective_user else 0
        lb_text = await get_leaderboard_text(user_id)
        await update.message.reply_text(
            lb_text,
            parse_mode=ParseMode.MARKDOWN,
        )


async def verify_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Verify Membership बटन क्लिक होने पर बचे हुए चैनल्स चेक करता है और वेरिफाई होते ही मेनू खोलता है।"""
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
                "🚫 **Verification Failed!**\n\n"
                "Abhi bhi neeche diye gaye channels/group join karna baaki hai.\n"
                "Inhe join karke dobara **'✅ Verify Membership'** dabayein:",
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
            await query.edit_message_text("⏳ Naya session prepare ho raha hai...")
            token_ok = await init_user_session(chat_id)

            if not token_ok:
                await query.edit_message_text(
                    "⚠️ Website se initial session token fetch nahi ho paya. "
                    "Please internet connection check karein ya dobara try karein.",
                    reply_markup=get_cancel_to_claim_keyboard(),
                )
                return ConversationHandler.END

            await query.edit_message_text(
                "🚀 Naya session start ho gaya hai.\nPlease apna 10-digit mobile number bhejein:",
                reply_markup=get_cancel_to_claim_keyboard(),
            )
            return WAITING_FOR_DIRECT_MOBILE_FLOW

    elif data == "fb_next_triplet":
        # पिछला कूपन सक्सेसफुली रिडीम हो गया है — उसे कन्फर्म मार्क करें और अगला पेयर दें
        triplets = context.user_data.get("firebase_assigned_triplets", [])
        if triplets:
            triplets[-1]["status"] = "Redeemed"
        return await send_firebase_assigned_triplet(query.message, context)

    elif data == "fb_retry_coupon":
        # अगर कूपन सक्सेसफुली रिडीम नहीं हुआ तो उसे वापस पूल में डालकर नए Online नंबर के साथ दोबारा भेजें
        triplets = context.user_data.get("firebase_assigned_triplets", [])
        failed_coupon = triplets[-1].get("coupon") if triplets else None
        if triplets:
            triplets[-1]["status"] = "Failed_Returned"

        returned = release_unredeemed_firebase_coupon(context, failed_coupon)
        if returned:
            await query.message.reply_text(
                f"♻️ **Coupon `{returned}` रिडीम नहीं हुआ था — इसे दोबारा वापस निकाल लिया गया है!**\n"
                "🔄 अब नए Online नंबर के साथ दोबारा भेजा जा रहा है...",
                parse_mode=ParseMode.MARKDOWN,
            )
        return await send_firebase_assigned_triplet(query.message, context)

    elif data == "back_to_claim_menu":
        await close_user_session(chat_id)
        await query.edit_message_text(
            text=CLAIM_PANEL_TEXT,
            reply_markup=get_claim_coupon_inline_keyboard(),
            parse_mode=ParseMode.MARKDOWN,
        )
        return ConversationHandler.END

    elif data == "claim_cancel_main":
        await close_user_session(chat_id)
        await query.edit_message_text(
            text="❌ **Action Cancelled.** Select an option from the main menu below.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return ConversationHandler.END

    return ConversationHandler.END


async def receive_direct_mobile_flow_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Direct Send Your Mobile Number पर क्लिक करने के बाद Woohoo का पूरा 4-step flow चलाता है।"""
    if not update.message or not update.message.text or update.effective_chat.type != "private":
        return WAITING_FOR_DIRECT_MOBILE_FLOW

    text = update.message.text.strip()

    # अगर यूज़र ने नीचे के मेनू बटन दबा दिए हैं, तो सेशन बंद करके उस मेनू पर जाएं
    if text in {"Claim Coupon", "Check Coupon", "Buy Coupon", "Support", "🏆 Leaderboard"}:
        await close_user_session(update.effective_chat.id)
        if text == "Claim Coupon":
            await handle_claim_coupon_menu(update, context)
        else:
            await handle_other_main_buttons(update, context)
        return ConversationHandler.END

    if not await ensure_user_subscribed(update, context):
        return ConversationHandler.END

    chat_id = update.effective_chat.id

    async with get_user_lock(chat_id):
        session_data = user_sessions.get(chat_id)
        if not session_data:
            await update.message.reply_text("⏳ Session initialize ho raha hai...")
            token_ok = await init_user_session(chat_id)
            if not token_ok:
                await update.message.reply_text(
                    "⚠️ Website se connect nahi ho pa raha hai. Please Cancel dabakar dobara try karein.",
                    reply_markup=get_cancel_to_claim_keyboard(),
                )
                return ConversationHandler.END
            await update.message.reply_text(
                "Please apna 10-digit mobile number bhejein:",
                reply_markup=get_cancel_to_claim_keyboard(),
            )
            return WAITING_FOR_DIRECT_MOBILE_FLOW

        session_data["last_activity"] = time.time()
        current_step = session_data["step"]
        client: UnifiedSessionClient = session_data["client"]

        # ------------------------------------------
        # STEP 1: मोबाइल नंबर इनपुट और वैलिडेशन
        # ------------------------------------------
        if current_step == "AWAITING_MOBILE":
            if not (text.isdigit() and len(text) == 10):
                await update.message.reply_text(
                    "❌ Invalid number! Please sahi 10-digit mobile number enter karein:",
                    reply_markup=get_cancel_to_claim_keyboard(),
                )
                return WAITING_FOR_DIRECT_MOBILE_FLOW

            session_data["mobile"] = text
            session_data["step"] = "AWAITING_CODE"
            await update.message.reply_text(
                "✅ Mobile number save ho gaya.\nAb apna unique coupon code bhejein:",
                reply_markup=get_cancel_to_claim_keyboard(),
            )
            return WAITING_FOR_DIRECT_MOBILE_FLOW

        # ------------------------------------------
        # STEP 2: यूनिक कोड इनपुट और API Step 1 कॉल
        # ------------------------------------------
        elif current_step == "AWAITING_CODE":
            session_data["code"] = text.upper()
            await update.message.reply_text("⏳ Code verify ho raha hai, please wait...")

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
                    server_msg = res_json.get("messageBody") or "OTP bhej diya gaya hai."
                    await update.message.reply_text(
                        f"📩 {server_msg}\nPlease received OTP enter karein:",
                        reply_markup=get_cancel_to_claim_keyboard(),
                    )
                else:
                    error_msg = res_json.get("messageBody") or str(res_json)
                    await update.message.reply_text(
                        f"❌ Website Error: {error_msg}\nPlease sahi unique code dobara bhejein (ya Cancel dabayein):",
                        reply_markup=get_cancel_to_claim_keyboard(),
                    )
                return WAITING_FOR_DIRECT_MOBILE_FLOW

            except Exception as e:
                await close_user_session(chat_id)
                await update.message.reply_text(
                    f"⚠️ Network ya API Error: {str(e)}\nDobara start karne ke liye /claim bhejein."
                )
                return ConversationHandler.END

        # ------------------------------------------
        # STEP 3: OTP वेरिफिकेशन
        # ------------------------------------------
        elif current_step == "AWAITING_OTP":
            if not text.isdigit():
                await update.message.reply_text(
                    "❌ Invalid OTP! Please sirf digits mein sahi OTP enter karein:",
                    reply_markup=get_cancel_to_claim_keyboard(),
                )
                return WAITING_FOR_DIRECT_MOBILE_FLOW

            session_data["otp"] = text
            await update.message.reply_text("⏳ OTP verify ho raha hai, please wait...")

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
                        f"❌ Website Error: {error_msg}\nPlease sahi OTP dobara enter karein (ya Cancel dabayein):",
                        reply_markup=get_cancel_to_claim_keyboard(),
                    )
                    return WAITING_FOR_DIRECT_MOBILE_FLOW

                session_data["step"] = "AWAITING_UPI"
                await update.message.reply_text(
                    "✅ OTP verify ho gaya!\nAb apni UPI ID bhejein (e.g., name@upi):",
                    reply_markup=get_cancel_to_claim_keyboard(),
                )
                return WAITING_FOR_DIRECT_MOBILE_FLOW

            except Exception as e:
                await close_user_session(chat_id)
                await update.message.reply_text(
                    f"⚠️ Server Error: {str(e)}\nDobara start karne ke liye /claim bhejein."
                )
                return ConversationHandler.END

        # ------------------------------------------
        # STEP 4: UPI ID इनपुट और फाइनल सबमिशन
        # ------------------------------------------
        elif current_step == "AWAITING_UPI":
            if "@" not in text or len(text) < 5:
                await update.message.reply_text(
                    "❌ Invalid UPI ID! Please sahi UPI ID enter karein (jisme '@' shamil ho):",
                    reply_markup=get_cancel_to_claim_keyboard(),
                )
                return WAITING_FOR_DIRECT_MOBILE_FLOW

            session_data["upi_id"] = text
            await update.message.reply_text("⏳ Final submit ho raha hai, please wait...")

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
                    success_msg = res_3_json.get("messageBody") or "Process successfully complete ho gaya!"

                    # Direct Mobile से सफलतापूर्वक क्लेम हुए कूपन को Leaderboard में जोड़ें
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
                        "🔄 Naya session start karne ke liye /claim bhejein:"
                    )
                    await close_user_session(chat_id)
                    return ConversationHandler.END
                else:
                    error_msg = res_3_json.get("messageBody") or str(res_3_json)
                    await update.message.reply_text(
                        f"❌ Website Error: {error_msg}\nPlease sahi UPI ID dobara enter karein (ya Cancel dabayein):",
                        reply_markup=get_cancel_to_claim_keyboard(),
                    )
                    return WAITING_FOR_DIRECT_MOBILE_FLOW

            except Exception as e:
                await close_user_session(chat_id)
                await update.message.reply_text(
                    f"⚠️ Server Error: {str(e)}\nDobara start karne ke liye /claim bhejein."
                )
                return ConversationHandler.END

    return WAITING_FOR_DIRECT_MOBILE_FLOW


async def receive_firebase_text_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_text = update.message.text.strip()

    if user_text in {"Claim Coupon", "Check Coupon", "Buy Coupon", "Support", "🏆 Leaderboard"}:
        if user_text == "Claim Coupon":
            await handle_claim_coupon_menu(update, context)
        else:
            await handle_other_main_buttons(update, context)
        return ConversationHandler.END

    if not await ensure_user_subscribed(update, context):
        return ConversationHandler.END

    context.user_data["telegram_user_info"] = _extract_telegram_user_info(update)

    status_msg = await update.message.reply_text(
        "⏳ **Scanning links and checking Firebase status... Please wait.**",
        parse_mode=ParseMode.MARKDOWN,
    )

    reply_chunks, has_active = await process_text_data_for_bot(
        user_text,
        context=context,
        is_from_file=False,
    )

    await status_msg.delete()
    for idx, chunk in enumerate(reply_chunks):
        markup = get_cancel_to_claim_keyboard() if (idx == len(reply_chunks) - 1 and not has_active) else None
        await update.message.reply_text(
            chunk,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=markup,
            disable_web_page_preview=True,
        )

    if has_active:
        await update.message.reply_text(
            COUPON_PROMPT_AFTER_ACTIVE_TEXT,
            reply_markup=get_cancel_to_claim_keyboard(),
            parse_mode=ParseMode.MARKDOWN,
        )
        return WAITING_FOR_FIREBASE_COUPONS

    await update.message.reply_text(
        "⚠️ **कोई Active Firebase Panel नहीं मिला (Inactive)।**\n"
        "🔄 कृपया दोबारा से अपना **Firebase URL** या **`.txt` / `.zip` file** भेजें:",
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
            "⚠️ **Invalid File Format:** केवल Firebase/Panel links वाली **`.txt`** या **`.zip`** file ही upload करें, या 20 तक links सीधे paste करें।",
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
                "🚫 **Rejected ZIP:** इस ZIP file के अंदर कोई `.txt` link file नहीं मिली।"
                if is_zip
                else "🚫 **Rejected TXT:** यह `.txt` file खाली है।"
            )
            await status_msg.edit_text(
                empty_msg,
                reply_markup=get_cancel_to_claim_keyboard(),
                parse_mode=ParseMode.MARKDOWN,
            )
            return WAITING_FOR_FIREBASE_INPUT

        reply_chunks, has_active = await process_text_data_for_bot(
            extracted_text,
            context=context,
            is_from_file=True,
        )

        await status_msg.delete()
        for idx, chunk in enumerate(reply_chunks):
            markup = get_cancel_to_claim_keyboard() if (idx == len(reply_chunks) - 1 and not has_active) else None
            await update.message.reply_text(
                chunk,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=markup,
                disable_web_page_preview=True,
            )

        if has_active:
            await update.message.reply_text(
                COUPON_PROMPT_AFTER_ACTIVE_TEXT,
                reply_markup=get_cancel_to_claim_keyboard(),
                parse_mode=ParseMode.MARKDOWN,
            )
            return WAITING_FOR_FIREBASE_COUPONS

        await update.message.reply_text(
            "⚠️ **कोई Active Firebase Panel नहीं मिला (Inactive)।**\n"
            "🔄 कृपया दोबारा से अपना **Firebase URL** या **`.txt` / `.zip` file** भेजें:",
            reply_markup=get_cancel_to_claim_keyboard(),
            parse_mode=ParseMode.MARKDOWN,
        )
        return WAITING_FOR_FIREBASE_INPUT

    except zipfile.BadZipFile:
        await status_msg.edit_text(
            "❌ **Error:** यह एक valid ZIP file नहीं है (corrupted file)।",
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
    """Active Firebase Panel मिलने के बाद यूज़र द्वारा भेजे गए Single या Sequence Coupon Codes को सेव करता है और Fresh UPI ID मांगता है।"""
    user_text = update.message.text.strip()

    if user_text in {"Claim Coupon", "Check Coupon", "Buy Coupon", "Support", "🏆 Leaderboard"}:
        if user_text == "Claim Coupon":
            await handle_claim_coupon_menu(update, context)
        else:
            await handle_other_main_buttons(update, context)
        return ConversationHandler.END

    if not await ensure_user_subscribed(update, context):
        return ConversationHandler.END

    context.user_data["telegram_user_info"] = _extract_telegram_user_info(update)

    coupons = extract_coupon_codes_from_text(user_text)
    if not coupons:
        await update.message.reply_text(
            "❌ **कोई Valid Coupon Code नहीं मिला!**\n"
            "कृपया अपना coupon code (single या sequence में) या `.txt` file भेजें:",
            reply_markup=get_cancel_to_claim_keyboard(),
            parse_mode=ParseMode.MARKDOWN,
        )
        return WAITING_FOR_FIREBASE_COUPONS

    context.user_data["firebase_coupons"] = coupons
    context.user_data.setdefault("used_firebase_coupons", [])

    await update.message.reply_text(
        build_fresh_upi_prompt_text(len(coupons)),
        reply_markup=get_cancel_to_claim_keyboard(),
        parse_mode=ParseMode.MARKDOWN,
    )
    return WAITING_FOR_FIREBASE_UPI


async def receive_firebase_coupons_file(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Active Firebase Panel मिलने के बाद यूज़र द्वारा भेजी गई .txt फाइल से Coupon Codes निकालता है और Fresh UPI ID मांगता है।"""
    if not await ensure_user_subscribed(update, context):
        return ConversationHandler.END

    context.user_data["telegram_user_info"] = _extract_telegram_user_info(update)

    document = update.message.document
    file_name = (document.file_name or "").lower() if document else ""

    if not document or not file_name.endswith(".txt"):
        await update.message.reply_text(
            "⚠️ **Invalid File Format:** कृपया केवल Coupon Codes वाली **`.txt`** file ही भेजें, या सीधे टेक्स्ट में कूपन पेस्ट करें।",
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
                "🚫 **Rejected TXT:** इस `.txt` file में कोई भी valid coupon code नहीं मिला। कृपया सही `.txt` file भेजें:",
                reply_markup=get_cancel_to_claim_keyboard(),
                parse_mode=ParseMode.MARKDOWN,
            )
            return WAITING_FOR_FIREBASE_COUPONS

        context.user_data["firebase_coupons"] = coupons
        context.user_data.setdefault("used_firebase_coupons", [])

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


async def send_firebase_assigned_triplet(message_target, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Active Firebase के केवल Online सेशन से Online Number + भेजे हुए कूपन्स में से 1 unused कूपन +
    8 बार यूज़ होने वाली UPI ID का पेयर बनाकर सीधे Woohoo पर ऑटो-क्लेम चलाता है:
    - हर कूपन क्लेम होने पर A-to-Z पूरी डिटेल का लॉग यूज़र को भेजता है और Leaderboard में काउंट जोड़ता है
    - पूरा 8 बार या सभी कूपन्स कंप्लीट होने पर फाइनल Reward + /pnlclaim रीस्टार्ट मैसेज भेजता है।
    """
    upi_cfg = context.user_data.get("firebase_upi_config")
    if not upi_cfg or upi_cfg.get("remaining_uses", 0) <= 0:
        await message_target.reply_text(
            NEXT_NEW_UPI_PROMPT_TEXT,
            reply_markup=get_cancel_to_claim_keyboard(),
            parse_mode=ParseMode.MARKDOWN,
        )
        return WAITING_FOR_FIREBASE_UPI

    # 1. पहले चेक करें कि कोई unused coupon बचा है या नहीं
    all_coupons: list[str] = context.user_data.get("firebase_coupons", [])
    used_coupons: list[str] = context.user_data.setdefault("used_firebase_coupons", [])
    remaining_coupons = [c for c in all_coupons if c not in used_coupons]

    if not remaining_coupons:
        await message_target.reply_text(FINAL_PNL_BATCH_COMPLETE_TEXT)
        return ConversationHandler.END

    # 2. 1 unused coupon पिक करें (इसे तब तक लॉक रखेंगे जब तक क्लेम न हो जाए)
    coupon_info = pick_unused_firebase_coupon(context)
    locked_coupon = coupon_info["coupon"]
    saved_upi_id = upi_cfg["upi_id"]

    wait_msg = await message_target.reply_text(
        f"⏳ **Auto-Claim Process Started...**\n"
        f"🎟️ **Coupon Code :** `{locked_coupon}` *(Locked until claimed)*\n"
        f"💳 **Reserved UPI:** `{saved_upi_id}` *(Step 3 पर ऑटो-सबमिट होगी)*\n\n"
        "🔄 *Active Firebase के Online Numbers पर OTP भेजकर वेरिफाई किया जा रहा है...*",
        parse_mode=ParseMode.MARKDOWN,
    )

    # 3. Online Number + Coupon का पेयर बनाकर Woohoo पर OTP -> Verify -> UPI Auto-Claim चलाएं
    claim_result = await auto_claim_triplet_on_woohoo(
        context=context,
        coupon_code=locked_coupon,
        upi_id=saved_upi_id,
    )

    await wait_msg.delete()

    if not claim_result.get("success"):
        # यदि कूपन इनवैलिड नहीं है (सिर्फ नंबरों पर OTP नहीं मिला), तो उसी कूपन को वापस उपलब्ध रखें
        if claim_result.get("reason") != "COUPON_INVALID":
            release_unredeemed_firebase_coupon(context, locked_coupon)
            await message_target.reply_text(
                f"⚠️ **Auto-Claim Complete नहीं हो पाया (Coupon `{locked_coupon}` सुरक्षित रखा गया है):**\n"
                f"📌 कारण: `{claim_result.get('message')}`\n\n"
                "🔄 कृपया नए **Active Firebase URL** या **`.txt` / `.zip` file** भेजें ताकि इसी कूपन को दोबारा चलाया जा सके:",
                reply_markup=get_cancel_to_claim_keyboard(),
                parse_mode=ParseMode.MARKDOWN,
            )
            return WAITING_FOR_FIREBASE_INPUT
        else:
            await message_target.reply_text(
                f"❌ **Coupon Invalid / Already Used:** `{locked_coupon}`\n"
                f"📌 {claim_result.get('message')}\n\n"
                "👇 *अगला कूपन चलाने के लिए नीचे बटन दबाएं:*",
                reply_markup=get_firebase_triplet_keyboard(),
                parse_mode=ParseMode.MARKDOWN,
            )
            return WAITING_FOR_FIREBASE_UPI

    # 4. कूपन सक्सेसफुली क्लेम हो गया है — अब 8-बार वाली UPI ID का 1 स्लॉट कंज्यूम करें
    upi_slot = consume_firebase_upi_use(context)
    server_msg = claim_result.get("server_message") or "Thank you for submitting the details. Reward will be transferred to provided UPI account within 48hrs."
    claimed_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    triplet_entry = {
        "number": claim_result["number"],
        "source_endpoint": claim_result["endpoint"],
        "otp": claim_result["otp"],
        "coupon": locked_coupon,
        "upi_id": upi_slot["upi_id"],
        "upi_use_number": upi_slot["used_count"],
        "upi_max_uses": upi_slot["max_uses"],
        "status": "Redeemed",
        "created_at": claimed_timestamp,
    }
    context.user_data.setdefault("firebase_assigned_triplets", []).append(triplet_entry)

    # Firebase से सफलतापूर्वक क्लेम हुए कूपन को भी Leaderboard में जोड़ें
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

    # A-to-Z पूरी डिटेल का लॉग यूज़र को भेजें
    atoz_log_msg = build_atoz_claim_log_text(
        triplet_entry=triplet_entry,
        upi_slot=upi_slot,
        remaining_coupons=coupon_info["remaining_total"],
        server_message=server_msg,
    )

    # 5. यदि पूरे 8 बार कंप्लीट हो गए हैं या भेजे गए सारे कूपन्स क्लेम हो चुके हैं
    if upi_slot["needs_new_upi"] or coupon_info["remaining_total"] == 0:
        await message_target.reply_text(
            atoz_log_msg,
            parse_mode=ParseMode.MARKDOWN,
            disable_web_page_preview=True,
        )
        await message_target.reply_text(FINAL_PNL_BATCH_COMPLETE_TEXT)
        return ConversationHandler.END

    # अभी और कूपन्स व UPI स्लॉट बाकी हैं
    await message_target.reply_text(
        atoz_log_msg,
        reply_markup=get_firebase_triplet_keyboard(),
        parse_mode=ParseMode.MARKDOWN,
        disable_web_page_preview=True,
    )
    return WAITING_FOR_FIREBASE_UPI


async def receive_firebase_fresh_upi(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    यूज़र द्वारा भेजी गई एकदम Fresh UPI ID का फॉर्मेट व डुप्लीकेट चेक करता है,
    ठीक 8 बार लगातार यूज़ होने के लिए सेव करता है, और तुरंत पहला पेयर (Online Number + Coupon + UPI) भेजता है।
    """
    if not update.message or not update.message.text:
        return WAITING_FOR_FIREBASE_UPI

    upi_text = update.message.text.strip()

    if upi_text in {"Claim Coupon", "Check Coupon", "Buy Coupon", "Support", "🏆 Leaderboard"}:
        if upi_text == "Claim Coupon":
            await handle_claim_coupon_menu(update, context)
        else:
            await handle_other_main_buttons(update, context)
        return ConversationHandler.END

    if not await ensure_user_subscribed(update, context):
        return ConversationHandler.END

    context.user_data["telegram_user_info"] = _extract_telegram_user_info(update)

    # 1. Strict UPI Format Check
    if not is_valid_upi_format(upi_text):
        await update.message.reply_text(
            "❌ **Invalid UPI Format!**\n"
            "कृपया सही फॉर्मेट में एकदम **Fresh UPI ID** भेजें (e.g., `rahul123@ybl`, `name@okaxis`):",
            reply_markup=get_cancel_to_claim_keyboard(),
            parse_mode=ParseMode.MARKDOWN,
        )
        return WAITING_FOR_FIREBASE_UPI

    # 2. Duplicate UPI ID Check (पहले से यूज़ हो चुकी या सेव्ड UPI दोबारा नहीं लेगा)
    normalized_upi = upi_text.lower()
    used_upis: list[str] = context.user_data.setdefault("used_firebase_upis", [])
    current_cfg = context.user_data.get("firebase_upi_config")

    if normalized_upi in used_upis or (
        current_cfg and current_cfg.get("upi_id", "").lower() == normalized_upi
    ):
        await update.message.reply_text(
            "🚫 **Duplicate UPI ID Rejected!**\n"
            f"`{upi_text}` पहले ही यूज़/एंटर की जा चुकी है।\n"
            "🔄 कृपया एक **एकदम नई Fresh UPI ID** भेजें:",
            reply_markup=get_cancel_to_claim_keyboard(),
            parse_mode=ParseMode.MARKDOWN,
        )
        return WAITING_FOR_FIREBASE_UPI

    # 3. UPI ID को डुप्लीकेट लिस्ट में लॉक करें और ठीक 8 बार सेम रहने के लिए सेट करें
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

    # 4. तुरंत 1 Online Number + 1 Unused Coupon + 1 UPI Use का सेट बनाकर ऑटो-क्लेम चलाएं
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
                    pattern="^(fb_next_triplet|fb_retry_coupon|back_to_claim_menu)$",
                ),
            ],
            WAITING_FOR_DIRECT_MOBILE_FLOW: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_direct_mobile_flow_input),
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
    app.add_handler(
        MessageHandler(
            filters.Regex("^(Check Coupon|Buy Coupon|Support|🏆 Leaderboard)$"),
            handle_other_main_buttons,
        )
    )

    print("🤖 Bot is running...")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
