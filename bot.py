import asyncio
import json
import random
import time
from typing import Optional
from playwright.async_api import async_playwright, Playwright, Browser, BrowserContext, Page
from telegram import Update
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# ==========================================
# 1. कॉन्फ़िगरेशन
# ==========================================

BOT_TOKEN = "8880114377:AAEP0yHWvebq-1FdFFgH7XMzeBCSXIhDK48"

BASE_URL = "https://pgretailpromo.woohoo.in"
API_ENDPOINT = "https://pgretailpromo.woohoo.in/ClaimReward/SaveData"
RECAPTCHA_SITE_KEY = "6Lfxu4UpAAAAAHGbsaANLP86Im_jvVTGp1bjZbN0"

STATES_LIST = ["JHARKHAND", "BIHAR", "DELHI", "MAHARASHTRA", "KARNATAKA", "KERALA"]

PROXIES_CONFIG = [
    {
        "server": "http://103.163.55.57:50101",
        "username": "ZAhaiMvE",
        "password": "Y39OCfsKR0",
    },
    {
        "server": "http://103.163.55.125:50101",
        "username": "Me01WF2K",
        "password": "OHVb4nh60D",
    },
    {
        "server": "http://103.171.77.18:50101",
        "username": "aRSVpM7F",
        "password": "VDheTt93j0",
    },
]

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

SESSION_TIMEOUT_SECONDS = 600  # 10 मिनट निष्क्रिय रहने पर सेशन ऑटो-क्लोज होगा

# ==========================================
# 2. Global Browser और UnifiedSessionClient
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


async def get_shared_browser(headless: bool = True) -> Browser:
    """एक ही Global Chromium Browser instance को initialize और reuse करता है।"""
    global global_playwright, global_browser
    async with browser_init_lock:
        if global_browser and global_browser.is_connected():
            return global_browser

        if not global_playwright:
            global_playwright = await async_playwright().start()

        global_browser = await global_playwright.chromium.launch(
            headless=headless,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
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
            raise RuntimeError("Session start नहीं हुआ है। पहले start() call करें।")

        await self.page.goto(self.base_url, wait_until="domcontentloaded", timeout=10000)

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
            raise RuntimeError("Page available नहीं है।")

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
        raise RuntimeError(f"HTTP {res['status']}: सर्वर ने रिक्वेस्ट अस्वीकार कर दी।")

    data = res["data"]
    if isinstance(data, dict) and "raw" in data and len(data) == 1:
        raw_content = data["raw"].strip()
        if raw_content.startswith("<") or "<html" in raw_content.lower():
            raise RuntimeError("सर्वर या सुरक्षा फ़ायरवॉल (WAF) ने सेशन ब्लॉक कर दिया है या सेशन समाप्त हो गया है।")
        raise RuntimeError("सर्वर से अमान्य (Non-JSON) रिस्पॉन्स प्राप्त हुआ है।")

    return data


async def close_user_session(chat_id: int):
    """मौजूदा सेशन को सुरक्षित रूप से बंद और डिलीट करता है"""
    session = user_sessions.pop(chat_id, None)
    if session and "client" in session:
        await session["client"].close()


async def cleanup_loop():
    """हर 60 सेकंड में निष्क्रिय (inactive) सेशन्स को बंद करके RAM लीक रोकता है (बिना JobQueue dependency के)।"""
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
    """बॉट स्टार्ट होने पर Global Browser और बैकग्राउंड क्लीनअप टास्क चालू करता है।"""
    global cleanup_task
    await get_shared_browser(headless=True)
    cleanup_task = asyncio.create_task(cleanup_loop())


async def on_shutdown(application: Application):
    """बॉट बंद (Ctrl+C / Stop) होने पर सारे सेशन्स और Global Browser cleanly बंद करता है।"""
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

            page_info = await asyncio.wait_for(_connect_and_load(), timeout=15.0)
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
            print(f"[Session Init Timeout] Proxy {proxy_cfg.get('server') if proxy_cfg else 'None'} ने 15 सेकंड से अधिक समय लिया।")
        except Exception as e:
            print(f"[Session Init Error with proxy {proxy_cfg.get('server') if proxy_cfg else 'None'}]: {e}")

        await client.close()

    return False


# ==========================================
# 3. बॉट कमांड्स और स्टेप-बाय-स्टेप हैंडलर
# ==========================================


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    async with get_user_lock(chat_id):
        await update.message.reply_text("⏳ नया सेशन तैयार हो रहा है...")
        token_ok = await init_user_session(chat_id)

        if not token_ok:
            await update.message.reply_text(
                "⚠️ चेतावनी: वेबसाइट से शुरुआती सेशन टोकन प्राप्त नहीं हो सका। "
                "कृपया इंटरनेट/प्रॉक्सी कनेक्शन जांचें या /reset दबाकर दोबारा प्रयास करें।"
            )
            return

        await update.message.reply_text("नया सेशन शुरू हो गया है।\nकृपया अपना 10-अंकों का मोबाइल नंबर भेजें:")


async def reset_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    async with get_user_lock(chat_id):
        await update.message.reply_text("⏳ सेशन रीसेट हो रहा है...")
        token_ok = await init_user_session(chat_id)

        if not token_ok:
            await update.message.reply_text(
                "⚠️ वेबसाइट से सेशन टोकन प्राप्त नहीं हो सका। कृपया थोड़ी देर बाद /reset दबाएं।"
            )
            return

        await update.message.reply_text("प्रोसेस रीसेट कर दिया गया है।\nकृपया नया मोबाइल नंबर भेजें:")


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return

    chat_id = update.effective_chat.id
    text = update.message.text.strip()

    async with get_user_lock(chat_id):
        session_data = user_sessions.get(chat_id)
        if not session_data:
            await update.message.reply_text("⏳ सेशन प्रारंभ हो रहा है...")
            token_ok = await init_user_session(chat_id)
            if not token_ok:
                await update.message.reply_text("⚠️ वेबसाइट से कनेक्ट नहीं हो पा रहा है। कृपया /reset दबाएं।")
                return
            await update.message.reply_text("कृपया अपना 10-अंकों का मोबाइल नंबर भेजें:")
            return

        session_data["last_activity"] = time.time()
        current_step = session_data["step"]
        client: UnifiedSessionClient = session_data["client"]

        # ------------------------------------------
        # STEP 1: मोबाइल नंबर इनपुट और वैलिडेशन
        # ------------------------------------------
        if current_step == "AWAITING_MOBILE":
            if not (text.isdigit() and len(text) == 10):
                await update.message.reply_text("❌ कृपया सही 10-अंकों का मोबाइल नंबर दर्ज करें:")
                return

            session_data["mobile"] = text
            session_data["step"] = "AWAITING_CODE"
            await update.message.reply_text("✅ नंबर सेव हो गया।\nअब अपना यूनिक कोड भेजें:")

        # ------------------------------------------
        # STEP 2: यूनिक कोड इनपुट और API Step 1 कॉल
        # ------------------------------------------
        elif current_step == "AWAITING_CODE":
            session_data["code"] = text.upper()
            await update.message.reply_text("⏳ प्रोसेस हो रहा है, कृपया प्रतीक्षा करें...")

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
                    server_msg = res_json.get("messageBody") or "OTP भेज दिया गया है।"
                    await update.message.reply_text(f"📩 {server_msg}\nकृपया प्राप्त हुआ OTP दर्ज करें:")
                else:
                    error_msg = res_json.get("messageBody") or str(res_json)
                    await update.message.reply_text(
                        f"❌ वेबसाइट एरर: {error_msg}\nकृपया सही यूनिक कोड दोबारा दर्ज करें (या नया नंबर डालने के लिए /reset दबाएं):"
                    )

            except Exception as e:
                await close_user_session(chat_id)
                await update.message.reply_text(f"⚠️ नेटवर्क या API एरर: {str(e)}\nरीसेट करने के लिए /reset दबाएं।")

        # ------------------------------------------
        # STEP 3: OTP वेरिफिकेशन
        # ------------------------------------------
        elif current_step == "AWAITING_OTP":
            if not text.isdigit():
                await update.message.reply_text("❌ कृपया केवल अंकों में सही OTP दर्ज करें:")
                return

            session_data["otp"] = text
            await update.message.reply_text("⏳ OTP वेरिफाई हो रहा है...")

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
                        f"❌ वेबसाइट एरर: {error_msg}\nकृपया सही OTP दोबारा दर्ज करें (या /reset दबाएं):"
                    )
                    return

                session_data["step"] = "AWAITING_UPI"
                await update.message.reply_text("✅ OTP वेरिफाई हो गया!\nअब अपनी UPI ID भेजें (जैसे: name@upi):")

            except Exception as e:
                await close_user_session(chat_id)
                await update.message.reply_text(f"⚠️ सर्वर एरर: {str(e)}\nरीसेट करने के लिए /reset दबाएं।")

        # ------------------------------------------
        # STEP 4: UPI ID इनपुट और फाइनल सबमिशन
        # ------------------------------------------
        elif current_step == "AWAITING_UPI":
            if "@" not in text or len(text) < 5:
                await update.message.reply_text("❌ कृपया सही UPI ID दर्ज करें (जिसमें '@' शामिल हो):")
                return

            session_data["upi_id"] = text
            await update.message.reply_text("⏳ फाइनल सबमिट हो रहा है, कृपया प्रतीक्षा करें...")

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
                    success_msg = res_3_json.get("messageBody") or "प्रोसेस सफलतापूर्वक पूरा हो गया!"
                    await update.message.reply_text(
                        f"🎉 {success_msg}\n\n"
                        "🔄 नया सेशन शुरू करने के लिए अगला मोबाइल नंबर या /start भेजें:"
                    )
                    await close_user_session(chat_id)
                else:
                    error_msg = res_3_json.get("messageBody") or str(res_3_json)
                    await update.message.reply_text(
                        f"❌ वेबसाइट एरर: {error_msg}\nकृपया सही UPI ID दोबारा दर्ज करें (या /reset दबाएं):"
                    )

            except Exception as e:
                await close_user_session(chat_id)
                await update.message.reply_text(f"⚠️ सर्वर एरर: {str(e)}\nरीसेट करने के लिए /reset दबाएं।")


# ==========================================
# 4. बॉट स्टार्टअप
# ==========================================

if __name__ == "__main__":
    app = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .concurrent_updates(True)
        .post_init(on_startup)
        .post_shutdown(on_shutdown)
        .build()
    )

    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("reset", reset_command))
    app.add_handler(CommandHandler("cancel", reset_command))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    print("Bot is running...")
    app.run_polling()
