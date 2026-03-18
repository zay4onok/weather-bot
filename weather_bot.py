"""
Telegram Weather Bot with AI (Groq LLM + OpenWeatherMap)

Single-file bot ready for cloud deployment.
Commands:
  /start        - Welcome message
  /setcity      - Set your city (e.g., /setcity Kyiv)
  /weather      - Get weather now (for saved city or specify: /weather London)
  /subscribe    - Subscribe to daily morning forecast
  /unsubscribe  - Unsubscribe from daily forecast
"""

import os
import logging
import asyncio
import httpx
import asyncpg
from datetime import datetime, time
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
from telegram import Update, BotCommand
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ConversationHandler,
    ContextTypes,
    filters,
)

WAITING_CITY = 1
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

load_dotenv()

TELEGRAM_TOKEN = os.environ["TELEGRAM_TOKEN"]
OWM_API_KEY = os.environ["OWM_API_KEY"]
GROQ_API_KEY = os.environ["GROQ_API_KEY"]

MORNING_HOUR = int(os.getenv("MORNING_HOUR", "8"))
MORNING_MINUTE = int(os.getenv("MORNING_MINUTE", "0"))
DEFAULT_TIMEZONE = os.getenv("DEFAULT_TIMEZONE", "Europe/Kyiv")
DEFAULT_CITY = os.getenv("DEFAULT_CITY", "Kyiv")
DATABASE_URL = os.environ["DATABASE_URL"]

_pool: asyncpg.Pool | None = None

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Database (PostgreSQL via asyncpg)
# ---------------------------------------------------------------------------

async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            DATABASE_URL, min_size=1, max_size=5, statement_cache_size=0
        )
    return _pool


async def init_db():
    pool = await get_pool()
    await pool.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            chat_id    BIGINT PRIMARY KEY,
            city       TEXT NOT NULL DEFAULT 'Kyiv',
            subscribed INTEGER NOT NULL DEFAULT 0,
            tz_offset  INTEGER NOT NULL DEFAULT 7200
        )
        """
    )


async def upsert_user(chat_id: int, **fields):
    pool = await get_pool()
    if not fields:
        return
    row = await pool.fetchrow("SELECT chat_id FROM users WHERE chat_id = $1", chat_id)
    if row:
        sets = ", ".join(f"{k} = ${i+2}" for i, k in enumerate(fields))
        vals = [chat_id] + list(fields.values())
        await pool.execute(f"UPDATE users SET {sets} WHERE chat_id = $1", *vals)
    else:
        keys = ["chat_id"] + list(fields.keys())
        placeholders = ", ".join(f"${i+1}" for i in range(len(keys)))
        cols = ", ".join(keys)
        vals = [chat_id] + list(fields.values())
        await pool.execute(f"INSERT INTO users ({cols}) VALUES ({placeholders})", *vals)


async def get_user(chat_id: int) -> dict | None:
    pool = await get_pool()
    row = await pool.fetchrow("SELECT * FROM users WHERE chat_id = $1", chat_id)
    return dict(row) if row else None


async def get_subscribers() -> list[dict]:
    pool = await get_pool()
    rows = await pool.fetch("SELECT * FROM users WHERE subscribed = 1")
    return [dict(r) for r in rows]

# ---------------------------------------------------------------------------
# Weather (OpenWeatherMap)
# ---------------------------------------------------------------------------

async def fetch_weather(city: str) -> dict | None:
    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {
        "q": city,
        "appid": OWM_API_KEY,
        "units": "metric",
        "lang": "ua",
    }
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(url, params=params)
        if resp.status_code != 200:
            log.warning("OWM error %s for city=%s: %s", resp.status_code, city, resp.text)
            return None
        return resp.json()


async def fetch_forecast(city: str) -> dict | None:
    url = "https://api.openweathermap.org/data/2.5/forecast"
    params = {
        "q": city,
        "appid": OWM_API_KEY,
        "units": "metric",
        "lang": "ua",
        "cnt": 8,
    }
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(url, params=params)
        if resp.status_code != 200:
            return None
        return resp.json()


CONDITION_IMAGES = {
    "01": "https://images.unsplash.com/photo-1601297183305-6df142704ea2?w=800",
    "02": "https://images.unsplash.com/photo-1534088568595-a066f410bcda?w=800",
    "03": "https://images.unsplash.com/photo-1501630834273-4b5604d2ee31?w=800",
    "04": "https://images.unsplash.com/photo-1501630834273-4b5604d2ee31?w=800",
    "09": "https://images.unsplash.com/photo-1534274988757-a28bf1a57c17?w=800",
    "10": "https://images.unsplash.com/photo-1519692933481-e162a57d6721?w=800",
    "11": "https://images.unsplash.com/photo-1605727216801-e27ce1d0cc28?w=800",
    "13": "https://images.unsplash.com/photo-1478265409131-1f65c88f965c?w=800",
    "50": "https://images.unsplash.com/photo-1487621167305-5d248087c724?w=800",
}


def get_condition_photo(current: dict) -> str:
    icon_code = current["weather"][0]["icon"][:2]
    return CONDITION_IMAGES.get(icon_code, CONDITION_IMAGES["03"])


async def fetch_daily_minmax(lat: float, lon: float) -> tuple[float, float] | None:
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "daily": "temperature_2m_max,temperature_2m_min",
        "timezone": "auto",
        "forecast_days": 1,
    }
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(url, params=params)
            log.info("Open-Meteo minmax response: status=%s lat=%s lon=%s", resp.status_code, lat, lon)
            if resp.status_code != 200:
                log.warning("Open-Meteo minmax error %s: %s", resp.status_code, resp.text)
                return None
            data = resp.json()
            daily = data.get("daily", {})
            t_min = daily.get("temperature_2m_min", [None])[0]
            t_max = daily.get("temperature_2m_max", [None])[0]
            if t_min is not None and t_max is not None:
                return t_min, t_max
            log.warning("Open-Meteo minmax missing data: %s", daily)
    except Exception as e:
        log.error("Open-Meteo minmax exception: %s", e)
    return None


def _wind_direction(deg: int) -> str:
    dirs = ["Пн", "ПнСх", "Сх", "ПдСх", "Пд", "ПдЗх", "Зх", "ПнЗх"]
    return dirs[round(deg / 45) % 8]


def format_weather_data(current: dict, forecast: dict | None, minmax: tuple[float, float] | None = None) -> str:
    main = current["main"]
    wind = current["wind"]
    desc = current["weather"][0]["description"]
    if not minmax:
        temps = [main["temp"]]
        if forecast and "list" in forecast:
            temps.extend(item["main"]["temp"] for item in forecast["list"][:8])
        minmax = (min(temps), max(temps))
    lines = [
        f"Місто: {current['name']}",
        f"Температура: {main['temp']}°C (відчувається як {main['feels_like']}°C)",
        f"Мін/Макс: {minmax[0]:.0f}°C / {minmax[1]:.0f}°C",
        f"Вологість: {main['humidity']}%",
        f"Вітер: {wind['speed']} м/с",
        f"Опис: {desc}",
    ]
    if forecast and "list" in forecast:
        lines.append("\nПрогноз на найближчі 24 години:")
        for item in forecast["list"][:8]:
            dt = datetime.fromtimestamp(item["dt"])
            t = item["main"]["temp"]
            d = item["weather"][0]["description"]
            lines.append(f"  {dt.strftime('%H:%M')} — {t}°C, {d}")
    return "\n".join(lines)


def format_details_card(current: dict, forecast: dict | None, minmax: tuple[float, float] | None = None) -> str:
    main = current["main"]
    wind = current["wind"]
    sys = current.get("sys", {})
    clouds = current.get("clouds", {}).get("all", 0)
    visibility = current.get("visibility", 0)
    tz_offset = current.get("timezone", 0)

    wind_speed = wind.get("speed", 0)
    wind_gust = wind.get("gust", wind_speed)
    wind_deg = wind.get("deg", 0)

    sunrise_ts = sys.get("sunrise")
    sunset_ts = sys.get("sunset")
    sunrise = datetime.fromtimestamp(sunrise_ts + tz_offset, ZoneInfo("UTC")).strftime("%H:%M") if sunrise_ts else "—"
    sunset = datetime.fromtimestamp(sunset_ts + tz_offset, ZoneInfo("UTC")).strftime("%H:%M") if sunset_ts else "—"

    vis_km = round(visibility / 1000, 1) if visibility is not None else "—"

    pressure_mmhg = round(main["pressure"] * 0.750062, 1)

    rain_1h = current.get("rain", {}).get("1h", 0)
    snow_1h = current.get("snow", {}).get("1h", 0)
    precip = rain_1h + snow_1h

    lines = [
        f"🌡 Відчувається — {main['feels_like']:.1f}°C",
    ]
    if not minmax:
        temps = [main["temp"]]
        if forecast and "list" in forecast:
            temps.extend(item["main"]["temp"] for item in forecast["list"][:8])
        minmax = (min(temps), max(temps))
    lines.append(f"🔻 Мін / Макс — {minmax[0]:.0f}° / {minmax[1]:.0f}°")
    lines += [
        f"💧 Вологість — {main['humidity']}%",
        f"🌀 Тиск — {pressure_mmhg} мм рт.ст.",
        f"💨 Вітер — {wind_speed:.1f} м/с, {_wind_direction(wind_deg)}",
        f"🌬 Пориви — {wind_gust:.1f} м/с",
        f"☁️ Хмарність — {clouds}%",
        f"👁 Видимість — {vis_km} км",
        f"🌧 Опади (1 год) — {precip:.1f} мм",
        f"🌅 Схід сонця — {sunrise}",
        f"🌇 Захід сонця — {sunset}",
    ]

    if forecast and "list" in forecast:
        lines.append("")
        lines.append("⏱ Прогноз на 24 години:")
        for item in forecast["list"][:8]:
            dt = datetime.fromtimestamp(item["dt"])
            t = item["main"]["temp"]
            d = item["weather"][0]["description"]
            lines.append(f"  {dt.strftime('%H:%M')} — {t:.0f}°, {d}")

    return "\n".join(lines)

# ---------------------------------------------------------------------------
# AI (Groq)
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """Ти — дружній погодний асистент у Telegram.
Отримавши дані про погоду, склади коротке (3-5 речень), приємне повідомлення.
Включи:
- поточну температуру та відчуття
- головне про погоду (опади, хмарність, вітер)
- практичну пораду (парасолька, одяг, сонцезахисний крем тощо)
Використовуй емодзі для наочності.
Відповідай українською мовою."""


async def generate_weather_text(weather_data: str) -> str:
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {GROQ_API_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": "llama-3.3-70b-versatile",
                    "messages": [
                        {"role": "system", "content": SYSTEM_PROMPT},
                        {"role": "user", "content": f"Ось дані про погоду:\n\n{weather_data}\n\nСклади гарне повідомлення про погоду."},
                    ],
                    "temperature": 0.7,
                    "max_tokens": 500,
                },
            )
            resp.raise_for_status()
            return resp.json()["choices"][0]["message"]["content"]
    except Exception as e:
        log.error("Groq error: %s", e)
        return f"Не вдалося згенерувати опис. Сирі дані:\n\n{weather_data}"

# ---------------------------------------------------------------------------
# Bot handlers
# ---------------------------------------------------------------------------

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "Привіт! 👋\n"
        "Я твій особистий бот-метеоролог зі штучним інтелектом.\n"
        "\n"
        "━━━━━━━━━━━━━━━\n"
        "\n"
        "🏙  /setcity — встановити своє місто\n"
        "🌤  /weather — дізнатися погоду зараз\n"
        "📬  /subscribe — ранкова розсилка\n"
        "🔕  /unsubscribe — відписатися\n"
        "\n"
        "━━━━━━━━━━━━━━━\n"
        "\n"
        "Для початку обери своє місто — натисни /setcity"
    )
    await update.message.reply_text(text)


def _utc_offset_label(offset_sec: int) -> str:
    h = offset_sec // 3600
    m = abs(offset_sec % 3600) // 60
    sign = "+" if offset_sec >= 0 else ""
    return f"UTC{sign}{h}" + (f":{m:02d}" if m else "")


async def cmd_setcity(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.args:
        return await _process_city(update, " ".join(context.args))
    await update.message.reply_text("🏙 Напиши назву міста:")
    return WAITING_CITY


async def _receive_city(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await _process_city(update, update.message.text.strip())


async def _process_city(update: Update, city: str):
    test = await fetch_weather(city)
    if test is None:
        await update.message.reply_text(f"Не вдалося знайти місто «{city}». Спробуй ще раз:")
        return WAITING_CITY
    real_name = test["name"]
    tz_offset = test.get("timezone", 7200)
    await upsert_user(update.effective_chat.id, city=real_name, tz_offset=tz_offset)
    await update.message.reply_text(
        f"✅ Місто встановлено: {real_name}\n"
        f"🕐 Часовий пояс: {_utc_offset_label(tz_offset)}"
    )
    return ConversationHandler.END


async def _cancel_setcity(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("❌ Скасовано.")
    return ConversationHandler.END


async def cmd_weather(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = await get_user(update.effective_chat.id)
    if not user:
        await update.message.reply_text(
            "📍 Спочатку обери своє місто — натисни /setcity"
        )
        return
    city = user["city"]

    msg = await update.message.reply_text(f"🔍 Шукаю погоду для {city}...")

    current = await fetch_weather(city)
    if current is None:
        await msg.edit_text(f"❌ Не вдалося отримати погоду для «{city}».")
        return

    forecast = await fetch_forecast(city)
    coord = current.get("coord", {})
    minmax = await fetch_daily_minmax(coord.get("lat", 0), coord.get("lon", 0))
    raw = format_weather_data(current, forecast, minmax)
    ai_text = await generate_weather_text(raw)
    details = format_details_card(current, forecast, minmax)
    photo_url = get_condition_photo(current)

    full_text = f"{ai_text}\n\n━━━━━━━━━━━━━━━\n\n{details}"

    try:
        caption = full_text[:1024] if len(full_text) > 1024 else full_text
        await update.message.reply_photo(photo=photo_url, caption=caption)
        if len(full_text) > 1024:
            await update.message.reply_text(full_text[1024:])
        await msg.delete()
    except Exception as e:
        log.error("Send error: %s", e)
        await msg.edit_text(full_text)


async def cmd_subscribe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user = await get_user(chat_id)
    if not user:
        await update.message.reply_text(
            "📍 Спочатку обери своє місто — натисни /setcity"
        )
        return
    if user["subscribed"]:
        tz_label = _utc_offset_label(user["tz_offset"])
        await update.message.reply_text(
            f"📬 Ти вже підписаний на ранкову розсилку!\n"
            f"📍 Місто: {user['city']}\n"
            f"⏰ Час: {MORNING_HOUR:02d}:{MORNING_MINUTE:02d} ({tz_label})\n\n"
            "Змінити місто: /setcity <місто>\n"
            "Відписатися: /unsubscribe"
        )
        return
    await upsert_user(chat_id, subscribed=1)
    city = user["city"]
    tz_label = _utc_offset_label(user["tz_offset"])
    await update.message.reply_text(
        f"✅ Підписку оформлено! Щоранку о {MORNING_HOUR:02d}:{MORNING_MINUTE:02d} "
        f"({tz_label}) ти отримуватимеш прогноз погоди.\n"
        f"📍 Поточне місто: {city}\n"
        "Змінити місто: /setcity <місто>"
    )


async def cmd_unsubscribe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user = await get_user(chat_id)
    if not user or not user["subscribed"]:
        await update.message.reply_text(
            "📭 Ти не підписаний на розсилку.\n"
            "Підписатися: /subscribe"
        )
        return
    await upsert_user(chat_id, subscribed=0)
    await update.message.reply_text("✅ Ти відписався від ранкової розсилки.")

# ---------------------------------------------------------------------------
# Scheduler
# ---------------------------------------------------------------------------

async def morning_broadcast(app: Application):
    subscribers = await get_subscribers()
    if not subscribers:
        return

    now_utc = datetime.now(ZoneInfo("UTC"))

    for sub in subscribers:
        tz_offset = sub["tz_offset"]
        local_hour = (now_utc.hour + tz_offset // 3600) % 24
        local_minute = now_utc.minute + (tz_offset % 3600) // 60
        if local_hour != MORNING_HOUR or local_minute != MORNING_MINUTE:
            continue

        chat_id = sub["chat_id"]
        city = sub["city"]
        try:
            current = await fetch_weather(city)
            if current is None:
                await app.bot.send_message(chat_id, f"❌ Не вдалося отримати погоду для {city}.")
                continue
            forecast = await fetch_forecast(city)
            coord = current.get("coord", {})
            minmax = await fetch_daily_minmax(coord.get("lat", 0), coord.get("lon", 0))
            raw = format_weather_data(current, forecast, minmax)
            ai_text = await generate_weather_text(raw)
            details = format_details_card(current, forecast, minmax)
            photo_url = get_condition_photo(current)
            full_text = f"{ai_text}\n\n━━━━━━━━━━━━━━━\n\n{details}"
            try:
                caption = full_text[:1024] if len(full_text) > 1024 else full_text
                await app.bot.send_photo(chat_id, photo=photo_url, caption=caption)
                if len(full_text) > 1024:
                    await app.bot.send_message(chat_id, full_text[1024:])
            except Exception:
                await app.bot.send_message(chat_id, full_text)
            log.info("Sent morning forecast to %s (city=%s)", chat_id, city)
        except Exception as e:
            log.error("Failed to send to %s: %s", chat_id, e)

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def post_init(app: Application):
    await init_db()

    await app.bot.set_my_commands([
        BotCommand("start", "Почати"),
        BotCommand("setcity", "Встановити місто"),
        BotCommand("weather", "Поточна погода"),
        BotCommand("subscribe", "Підписатися на ранкову розсилку"),
        BotCommand("unsubscribe", "Відписатися від розсилки"),
    ])

    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        morning_broadcast,
        trigger=IntervalTrigger(minutes=1),
        args=[app],
        id="morning_weather",
        replace_existing=True,
    )
    scheduler.start()
    log.info("Scheduler started: checking every minute for %02d:%02d local time", MORNING_HOUR, MORNING_MINUTE)


def main():
    app = (
        Application.builder()
        .token(TELEGRAM_TOKEN)
        .post_init(post_init)
        .build()
    )
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(ConversationHandler(
        entry_points=[CommandHandler("setcity", cmd_setcity)],
        states={
            WAITING_CITY: [MessageHandler(filters.TEXT & ~filters.COMMAND, _receive_city)],
        },
        fallbacks=[
            CommandHandler("cancel", _cancel_setcity),
            CommandHandler("setcity", cmd_setcity),
            CommandHandler("weather", cmd_weather),
            CommandHandler("start", cmd_start),
            CommandHandler("subscribe", cmd_subscribe),
            CommandHandler("unsubscribe", cmd_unsubscribe),
        ],
        allow_reentry=True,
    ))
    app.add_handler(CommandHandler("weather", cmd_weather))
    app.add_handler(CommandHandler("subscribe", cmd_subscribe))
    app.add_handler(CommandHandler("unsubscribe", cmd_unsubscribe))

    log.info("Bot starting...")

    render_url = os.getenv("RENDER_EXTERNAL_URL")
    if render_url:
        port = int(os.getenv("PORT", "10000"))
        app.run_webhook(
            listen="0.0.0.0",
            port=port,
            url_path="/webhook",
            webhook_url=f"{render_url}/webhook",
            drop_pending_updates=True,
        )
    else:
        app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
