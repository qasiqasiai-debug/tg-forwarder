"""Одноразовая генерация SESSION_STRING (запускать на своём компьютере/сервере вручную).

    python -m scripts.gen_session

Спросит API_ID, API_HASH, номер телефона и код из Telegram (и пароль 2FA, если включён).
Строка сессии даёт полный доступ к аккаунту — храните её только в .env, никому не показывайте.
"""
import asyncio
import os

from telethon import TelegramClient
from telethon.sessions import StringSession


async def main() -> None:
    api_id = int(os.environ.get("API_ID") or input("API_ID: ").strip())
    api_hash = os.environ.get("API_HASH") or input("API_HASH: ").strip()
    async with TelegramClient(StringSession(), api_id, api_hash) as client:
        # start() интерактивно спросит телефон/код/пароль
        me = await client.get_me()
        print(f"\nАвторизован как: {me.first_name} (id {me.id})")
        print("\nВаш SESSION_STRING (вставьте в .env):\n")
        print(client.session.save())
        print()


if __name__ == "__main__":
    asyncio.run(main())
