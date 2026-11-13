from __future__ import annotations

import asyncio
import logging
import logging.handlers
import sys

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramUnauthorizedError
from aiogram.fsm.storage.memory import MemoryStorage
from telethon import TelegramClient
from telethon.errors import AuthKeyDuplicatedError, AuthKeyUnregisteredError, UnauthorizedError
from telethon.sessions import StringSession

from .bot import COMMANDS, App, build_router, render_status
from .config import ConfigError, Settings, load_settings
from .db import Database
from .forwarder import Forwarder

log = logging.getLogger("main")
EX_CONFIG = 78  # systemd: RestartPreventExitStatus=78 — не перезапускать при ошибке конфигурации


def setup_logging(s: Settings) -> None:
    fmt = logging.Formatter("%(asctime)s %(levelname)-7s %(name)s: %(message)s")
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    fh = logging.handlers.RotatingFileHandler(
        s.log_dir / "bot.log", maxBytes=5_000_000, backupCount=5, encoding="utf-8")
    sh = logging.StreamHandler(sys.stdout)
    for h in (fh, sh):
        h.setFormatter(fmt)
        root.addHandler(h)
    # Telethon/aiohttp болтливы; токены в логи не попадают, но уровень снижаем
    logging.getLogger("telethon").setLevel(logging.WARNING)
    logging.getLogger("aiogram.event").setLevel(logging.WARNING)


async def supervise(name: str, factory) -> None:
    """Перезапускает фоновую задачу при любой ошибке."""
    delay = 5
    while True:
        try:
            await factory()
            return
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            log.exception("Задача %s упала, перезапуск через %d сек", name, delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 120)


async def amain() -> int:
    try:
        s = load_settings()
    except ConfigError as e:
        print(f"Ошибка конфигурации: {e}", file=sys.stderr)
        return EX_CONFIG
    setup_logging(s)

    db = Database(s.db_path)
    await db.init()

    client = TelegramClient(
        StringSession(s.session_string), s.api_id, s.api_hash,
        connection_retries=-1, retry_delay=3, auto_reconnect=True,
        flood_sleep_threshold=10, catch_up=True,
        device_model="TG Forwarder", system_version="Linux",
    )
    bot = Bot(s.bot_token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    tasks: list[asyncio.Task] = []
    code = 0
    try:
        await client.connect()
        if not await client.is_user_authorized():
            log.error("SESSION_STRING недействительна. Сгенерируйте заново: python -m scripts.gen_session")
            return EX_CONFIG

        fw = Forwarder(s, db, client)

        async def notify(text: str) -> None:
            await bot.send_message(s.owner_id, text)

        fw.notify = notify
        app = App(settings=s, db=db, fw=fw, bot=bot, import_lock=asyncio.Lock())
        await fw.start()

        dp = Dispatcher(storage=MemoryStorage())
        dp.include_router(build_router(app))
        await bot.set_my_commands(COMMANDS)

        tasks = [
            asyncio.create_task(supervise("worker", fw.run_worker)),
            asyncio.create_task(supervise("poller", fw.run_poller)),
            asyncio.create_task(supervise("watchdog", fw.run_watchdog)),
        ]

        try:  # панель статуса при запуске
            text, kb = await render_status(app)
            await bot.send_message(s.owner_id, "🚀 <b>Бот запущен</b>\n\n" + text, reply_markup=kb)
        except Exception as e:  # noqa: BLE001
            log.warning("Не удалось отправить стартовое сообщение (нажмите /start у бота): %s", e)

        log.info("Запущено. Владелец: %s", s.owner_id)
        await dp.start_polling(bot, handle_signals=True)
    except TelegramUnauthorizedError:
        log.error("BOT_TOKEN недействителен.")
        code = EX_CONFIG
    except (AuthKeyDuplicatedError, AuthKeyUnregisteredError, UnauthorizedError):
        log.error("Сессия Telethon отозвана или используется с другого IP. "
                  "Сгенерируйте новую SESSION_STRING и не запускайте две копии бота.")
        code = EX_CONFIG
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await bot.session.close()
        if client.is_connected():
            await client.disconnect()
        await db.close()
        log.info("Остановлено")
    return code


def run() -> None:
    sys.exit(asyncio.run(amain()))


if __name__ == "__main__":
    run()
