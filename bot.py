import os
import json
import asyncio
from datetime import datetime
import logging
from typing import Dict
import httpx
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes
)

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
PUBLIC_CHANNEL_USERNAME = os.getenv("PUBLIC_CHANNEL_USERNAME", "@shohrukhposts")
PRIVATE_CHANNEL_ID = os.getenv("PRIVATE_CHANNEL_ID")
DATA_DIR = "data"
USER_DATA_FILE = os.path.join(DATA_DIR, "user_data.json")

os.makedirs(DATA_DIR, exist_ok=True)

_data_lock = asyncio.Lock()


def load_user_data() -> Dict:
    try:
        if os.path.exists(USER_DATA_FILE):
            with open(USER_DATA_FILE, 'r') as f:
                return json.load(f)
        return {}
    except Exception as e:
        logger.error(f"Error loading user data: {e}")
        return {}


async def save_user_data(user_data: Dict):
    async with _data_lock:
        try:
            with open(USER_DATA_FILE, 'w') as f:
                json.dump(user_data, f, indent=4)
        except Exception as e:
            logger.error(f"Error saving user data: {e}")


async def check_subscription(client: httpx.AsyncClient, user_id: int) -> bool:
    try:
        response = await client.get(
            f"https://api.telegram.org/bot{BOT_TOKEN}/getChatMember",
            params={"chat_id": PUBLIC_CHANNEL_USERNAME, "user_id": user_id},
            timeout=10.0
        )
        data = response.json()
        if not data.get("ok"):
            logger.error(f"API Error checking subscription: {data}")
            return False
        status = data["result"].get("status", "")
        is_member = status in ["member", "administrator", "creator"]
        logger.info(f"User {user_id} subscription status: {status}")
        return is_member
    except Exception as e:
        logger.error(f"Error checking subscription for user {user_id}: {e}")
        return False


async def generate_private_invite(client: httpx.AsyncClient) -> str:
    try:
        response = await client.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/createChatInviteLink",
            json={
                "chat_id": PRIVATE_CHANNEL_ID,
                "member_limit": 1,
                "creates_join_request": False,
                "expire_date": int(datetime.now().timestamp() + 3600)
            },
            timeout=10.0
        )
        data = response.json()
        if data.get("ok"):
            logger.info("Successfully generated invite link")
            return data["result"]["invite_link"]
        logger.error(f"Error generating invite link: {data}")
        return ""
    except Exception as e:
        logger.error(f"Error generating invite link: {e}")
        return ""


class SubscriptionBot:
    def __init__(self):
        self.user_data: Dict = load_user_data()
        self._http_client: httpx.AsyncClient = None

    async def get_client(self) -> httpx.AsyncClient:
        if self._http_client is None or self._http_client.is_closed:
            self._http_client = httpx.AsyncClient(
                limits=httpx.Limits(max_connections=200, max_keepalive_connections=50),
                timeout=httpx.Timeout(10.0)
            )
        return self._http_client

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            user_id = str(update.effective_user.id)
            user_name = update.effective_user.first_name or "there"

            client = await self.get_client()

            is_subscribed, invite_link = await asyncio.gather(
                check_subscription(client, int(user_id)),
                generate_private_invite(client) if True else asyncio.sleep(0)
            )

            if not is_subscribed:
                keyboard = [
                    [InlineKeyboardButton("🌟 Subscribe to Channel", url=f"https://t.me/{PUBLIC_CHANNEL_USERNAME.lstrip('@')}")],
                    [InlineKeyboardButton("✅ Verify Subscription", callback_data="check_sub")]
                ]
                await update.message.reply_text(
                    f"*Welcome\\!* 🎉\n\n"
                    "To access our exclusive private channel, please:\n\n"
                    "1️⃣ *Subscribe* to our main channel\n"
                    "2️⃣ Click *Verify Subscription* button\n\n"
                    "_This will give you access to premium content\\!_",
                    parse_mode=ParseMode.MARKDOWN_V2,
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
                return

            if invite_link:
                self.user_data[user_id] = {
                    "has_access": True,
                    "joined_at": datetime.now().isoformat()
                }
                asyncio.create_task(save_user_data(self.user_data))

                safe_name = _escape_md(user_name)
                keyboard = [[InlineKeyboardButton("🔐 Join Private Channel", url=invite_link)]]
                await update.message.reply_text(
                    f"🎉 *Congratulations {safe_name}\\!*\n\n"
                    "You now have access to our exclusive private channel\\!\n\n"
                    "⚠️ _This invite is valid for 1 hour and can only be used once\\._\n\n"
                    "🎯 _Click the button below to join\\!_",
                    parse_mode=ParseMode.MARKDOWN_V2,
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
            else:
                await update.message.reply_text(
                    "❌ *Error Notice*\n\n"
                    "Sorry, there was an error generating your invite link\\.\n"
                    "_Please try again in a few minutes\\._",
                    parse_mode=ParseMode.MARKDOWN_V2
                )
        except Exception as e:
            logger.error(f"Error in start command: {e}")
            try:
                await update.message.reply_text(
                    "⚠️ *System Notice*\n\n"
                    "An unexpected error occurred\\.\n"
                    "_Please try again later\\._",
                    parse_mode=ParseMode.MARKDOWN_V2
                )
            except Exception:
                pass

    async def button_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            query = update.callback_query
            await query.answer()
            user_name = query.from_user.first_name or "there"
            user_id = str(query.from_user.id)

            if query.data == "check_sub":
                client = await self.get_client()
                is_subscribed = await check_subscription(client, int(user_id))

                if is_subscribed:
                    invite_link = await generate_private_invite(client)
                    if invite_link:
                        self.user_data[user_id] = {
                            "has_access": True,
                            "joined_at": datetime.now().isoformat()
                        }
                        asyncio.create_task(save_user_data(self.user_data))

                        safe_name = _escape_md(user_name)
                        keyboard = [[InlineKeyboardButton("🔐 Join Private Channel", url=invite_link)]]
                        await query.message.edit_text(
                            f"✨ *Verification Successful\\!*\n\n"
                            f"Welcome aboard, {safe_name}\\! 🎉\n\n"
                            "⏳ _Your invite is ready_\n"
                            "👤 _Limited to one use_\n\n"
                            "🎯 _Click the button below to join\\!_",
                            parse_mode=ParseMode.MARKDOWN_V2,
                            reply_markup=InlineKeyboardMarkup(keyboard)
                        )
                    else:
                        await query.message.edit_text(
                            "❌ *System Notice*\n\n"
                            "Unable to generate invite link\\.\n"
                            "_Please try again in a few minutes\\._",
                            parse_mode=ParseMode.MARKDOWN_V2
                        )
                else:
                    keyboard = [
                        [InlineKeyboardButton("🌟 Subscribe Now", url=f"https://t.me/{PUBLIC_CHANNEL_USERNAME.lstrip('@')}")],
                        [InlineKeyboardButton("🔄 Check Again", callback_data="check_sub")]
                    ]
                    await query.message.edit_text(
                        "❗ *Subscription Required*\n\n"
                        f"Please subscribe to {PUBLIC_CHANNEL_USERNAME} first\\!\n\n"
                        "_Click 'Subscribe Now' and then 'Check Again'_",
                        parse_mode=ParseMode.MARKDOWN_V2,
                        reply_markup=InlineKeyboardMarkup(keyboard)
                    )
        except Exception as e:
            logger.error(f"Error in button callback: {e}")
            try:
                await query.message.edit_text(
                    "⚠️ *System Error*\n\n"
                    "An unexpected error occurred\\.\n"
                    "_Please try again later\\._",
                    parse_mode=ParseMode.MARKDOWN_V2
                )
            except Exception:
                pass


def _escape_md(text: str) -> str:
    chars = r'\_*[]()~`>#+-=|{}.!'
    for ch in chars:
        text = text.replace(ch, f'\\{ch}')
    return text


async def kick_unsubscribed_users(context: ContextTypes.DEFAULT_TYPE):
    try:
        user_data = load_user_data()
        async with httpx.AsyncClient(timeout=10.0) as client:
            tasks = []
            for user_id in list(user_data.keys()):
                tasks.append(_check_and_kick(context.bot, client, user_id))
            await asyncio.gather(*tasks, return_exceptions=True)
        await save_user_data(user_data)
    except Exception as e:
        logger.error(f"Error in kick_unsubscribed_users: {e}")


async def _check_and_kick(bot, client: httpx.AsyncClient, user_id: str):
    try:
        is_subscribed = await check_subscription(client, int(user_id))
        if not is_subscribed:
            await bot.ban_chat_member(chat_id=PRIVATE_CHANNEL_ID, user_id=int(user_id), revoke_messages=False)
            await bot.unban_chat_member(chat_id=PRIVATE_CHANNEL_ID, user_id=int(user_id), only_if_banned=True)
            logger.info(f"Kicked user {user_id} from private channel")
    except Exception as e:
        logger.error(f"Failed to check/kick user {user_id}: {e}")


async def status_check(context: ContextTypes.DEFAULT_TYPE):
    try:
        me = await context.bot.get_me()
        logger.info(f"Bot {me.username} is running")
    except Exception as e:
        logger.error(f"Status check failed: {e}")


async def send_broadcast(context: ContextTypes.DEFAULT_TYPE):
    bot = context.bot
    success_count = 0
    fail_count = 0

    try:
        user_data = load_user_data()
        total_users = len(user_data)
        logger.info(f"Starting broadcast to {total_users} users")

        semaphore = asyncio.Semaphore(30)

        async def send_one(user_id: str):
            nonlocal success_count, fail_count
            async with semaphore:
                try:
                    chat_id = int(user_id)
                    try:
                        user = await bot.get_chat(chat_id)
                        name = user.first_name or "there"
                    except Exception:
                        name = "there"

                    message = (
                        f"Hey {name}, my name is Shokhrukh, the channel you've just subscribed to. "
                        "I love to see you in my community, if you have any questions just contact my manager, ok? "
                        "or if you need counseling about your upcoming exam or IELTS in general, I will be happy to help. "
                        "Don't worry it's free. Stay tuned, thank you again!"
                    )
                    await bot.send_message(chat_id=chat_id, text=message)
                    success_count += 1
                except Exception as e:
                    logger.error(f"Failed to send to {user_id}: {e}")
                    fail_count += 1

        await asyncio.gather(*[send_one(uid) for uid in user_data.keys()])
        logger.info(f"Broadcast complete. Success: {success_count}, Failed: {fail_count}, Total: {total_users}")
    except Exception as e:
        logger.error(f"Broadcast failed: {e}")
        raise


def main():
    if not all([BOT_TOKEN, PUBLIC_CHANNEL_USERNAME, PRIVATE_CHANNEL_ID]):
        logger.error("Missing required environment variables!")
        return

    try:
        bot = SubscriptionBot()
        application = (
            Application.builder()
            .token(BOT_TOKEN)
            .concurrent_updates(True)
            .build()
        )

        application.add_handler(CommandHandler("start", bot.start))
        application.add_handler(CallbackQueryHandler(bot.button_callback))

        application.job_queue.run_repeating(status_check, interval=21600)
        application.job_queue.run_repeating(kick_unsubscribed_users, interval=600)

        logger.info("Bot started successfully!")
        application.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)
    except Exception as e:
        logger.error(f"Error starting bot: {e}")


if __name__ == "__main__":
    main()
