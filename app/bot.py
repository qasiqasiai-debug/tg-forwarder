"""Управляющий бот (aiogram 3). Доступен только владельцу."""
from __future__ import annotations

import asyncio
import html
import logging
import time
from dataclasses import dataclass

from aiogram import Bot, F, Router
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command, CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    BotCommand,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)
from telethon.errors import FloodWaitError

from .config import Settings
from .db import Database
from .forwarder import Forwarder
from .utils import chunk_lines, fmt_duration, fmt_ts, now, parse_channel_ref, split_refs

log = logging.getLogger("bot")

COMMANDS = [
    BotCommand(command="start", description="Запуск и панель управления"),
    BotCommand(command="status", description="Статус работы"),
    BotCommand(command="channels", description="Список каналов"),
    BotCommand(command="addchannel", description="Добавить канал(ы)"),
    BotCommand(command="removechannel", description="Удалить канал"),
    BotCommand(command="setchat", description="Установить целевой чат"),
    BotCommand(command="stats", description="Статистика пересылок"),
    BotCommand(command="pause", description="Приостановить пересылку"),
    BotCommand(command="resume", description="Возобновить пересылку"),
    BotCommand(command="mode", description="Режим: forward / copy"),
    BotCommand(command="import", description="Импорт каналов из channels.txt"),
]


@dataclass
class App:
    settings: Settings
    db: Database
    fw: Forwarder
    bot: Bot
    import_lock: asyncio.Lock


class AddChannelState(StatesGroup):
    waiting = State()


class SetChatState(StatesGroup):
    waiting = State()


# ---------------------------------------------------------------- rendering
def esc(s) -> str:
    return html.escape(str(s))


def keyboard(paused: bool) -> InlineKeyboardMarkup:
    toggle = (InlineKeyboardButton(text="▶️ Продолжить", callback_data="resume")
              if paused else InlineKeyboardButton(text="⏸ Пауза", callback_data="pause"))
    return InlineKeyboardMarkup(inline_keyboard=[
        [toggle, InlineKeyboardButton(text="🔄 Обновить", callback_data="refresh")],
        [InlineKeyboardButton(text="📋 Каналы", callback_data="channels"),
         InlineKeyboardButton(text="📊 Статистика", callback_data="stats")],
        [InlineKeyboardButton(text="➕ Добавить канал", callback_data="addchannel"),
         InlineKeyboardButton(text="🎯 Целевой чат", callback_data="setchat")],
    ])


async def render_status(app: App) -> tuple[str, InlineKeyboardMarkup]:
    db, fw, s = app.db, app.fw, app.settings
    paused = await fw.is_paused()
    ok, total = await db.count_channels()
    queue = await db.queue_size()
    total_fw = await db.count_forwarded()
    today = await db.count_forwarded(now() - 86400)
    last = await db.get_setting("last_success_ts")
    mode = await db.get_setting("mode", s.default_mode)

    if not fw.connected:
        state = "🔴 Нет связи с Telegram (MTProto)"
    elif paused:
        state = "⏸ Пауза"
    elif fw.target_id is None:
        state = "⚠️ Не задан целевой чат"
    else:
        state = "✅ Работает"

    target = (f"{esc(fw.target_title)} <code>{fw.target_id}</code>"
              if fw.target_id is not None else "не задан — используйте /setchat")
    ch_line = f"{ok}/{total}"
    if total != s.expected_channels:
        ch_line += f" (ожидалось {s.expected_channels})"
    else:
        ch_line = f"{ok}/{s.expected_channels}"

    text = (
        "🤖 <b>Статус системы</b>\n"
        f"Состояние: {state}\n"
        f"Каналов подключено: <b>{ch_line}</b>\n"
        f"Целевой чат: {target}\n"
        f"Режим: <code>{mode}</code>\n"
        f"Переслано публикаций: <b>{total_fw}</b> (за 24ч: {today})\n"
        f"В очереди: {queue}\n"
        f"Последняя успешная пересылка: {fmt_ts(int(last) if last else None, s.timezone)}\n"
        f"Аптайм: {fmt_duration(time.time() - fw.started_at)}"
    )
    if ok < total:
        text += "\n\n⚠️ Есть каналы с ошибками — смотрите /channels"
    return text, keyboard(paused)


async def render_channels(app: App) -> list[str]:
    rows = await app.db.list_channels()
    if not rows:
        return ["Каналов пока нет. Добавьте через /addchannel или /import."]
    lines = [f"📋 <b>Каналы ({len(rows)})</b>"]
    for i, r in enumerate(rows, 1):
        icon = "✅" if r["status"] == "ok" else "⚠️"
        uname = f" @{esc(r['username'])}" if r["username"] else ""
        line = f"{i}. {icon} {esc(r['title'] or r['id'])}{uname}"
        if r["status"] != "ok" and r["error"]:
            line += f"\n    ↳ {esc(r['error'])}"
        lines.append(line)
    return chunk_lines(lines)


async def render_stats(app: App) -> str:
    db, fw, s = app.db, app.fw, app.settings
    t = now()
    day, week = t - 86400, t - 7 * 86400
    top = await db.top_channels(week, 10)
    top_lines = "\n".join(f"  {i}. {esc(r['title'])} — {r['n']}" for i, r in enumerate(top, 1)) or "  —"
    last_err = await db.get_setting("last_success_ts")
    return (
        "📊 <b>Статистика пересылок</b>\n"
        f"Всего: <b>{await db.count_forwarded()}</b>\n"
        f"За 24 часа: {await db.count_forwarded(day)}\n"
        f"За 7 дней: {await db.count_forwarded(week)}\n"
        f"В очереди: {await db.queue_size()}\n"
        f"Ошибок с запуска: {fw.errors} · FloodWait: {fw.flood_waits}\n"
        f"Последняя ошибка: {esc(fw.last_error) or '—'}\n"
        f"Последняя успешная пересылка: "
        f"{fmt_ts(int(last_err) if last_err else None, s.timezone)}\n\n"
        f"🏆 <b>Топ каналов за 7 дней</b>\n{top_lines}"
    )


# -------------------------------------------------------------- operations
async def add_refs(app: App, message: Message, refs: list[str]) -> None:
    if not refs:
        await message.answer("Не нашёл ссылок. Формат: @username, https://t.me/username, "
                             "https://t.me/+инвайт или -100…")
        return
    if app.import_lock.locked():
        await message.answer("⏳ Уже идёт добавление каналов, дождитесь завершения.")
        return
    async with app.import_lock:
        await message.answer(f"⏳ Обрабатываю {len(refs)} шт. Вступление в каналы идёт с паузами — "
                             "это может занять несколько минут.")
        report: list[str] = []
        added = exists = failed = 0
        for idx, ref in enumerate(refs):
            try:
                status, text = await app.fw.add_channel(ref)
            except FloodWaitError as e:
                rest = refs[idx:]
                report.append(f"⛔ Telegram ограничил вступление: подождите {e.seconds} сек. "
                              f"Не обработано: {len(rest)} (повторите /import позже).")
                failed += len(rest)
                break
            except Exception as e:  # noqa: BLE001
                log.exception("add_channel %s", ref)
                status, text = "error", f"{type(e).__name__}"
            if status == "added":
                added += 1
                report.append(f"✅ {esc(text)}")
            elif status == "exists":
                exists += 1
                report.append(f"➕ уже есть: {esc(text)}")
            else:
                failed += 1
                report.append(f"❌ {esc(ref)} — {esc(text)}")
        ok, total = await app.db.count_channels()
        report.append(f"\n<b>Итого:</b> добавлено {added}, уже было {exists}, ошибок {failed}. "
                      f"Каналов в базе: {total}/{app.settings.expected_channels}.")
    for chunk in chunk_lines(report):
        await message.answer(chunk)


async def do_setchat(app: App, message: Message, ref: str | int) -> None:
    ok, info = await app.fw.set_target(ref)
    if ok:
        await message.answer(f"🎯 Целевой чат установлен: <b>{esc(info)}</b>")
    else:
        await message.answer(f"❌ {esc(info)}")


# ------------------------------------------------------------------ router
def build_router(app: App) -> Router:
    r = Router()
    owner = F.from_user.id == app.settings.owner_id
    r.message.filter(owner)
    r.callback_query.filter(owner)

    async def send_status(message: Message) -> None:
        text, kb = await render_status(app)
        await message.answer(text, reply_markup=kb)

    @r.message(Command("start"))
    async def cmd_start(message: Message, state: FSMContext) -> None:
        await state.clear()
        await send_status(message)

    @r.message(Command("status"))
    async def cmd_status(message: Message) -> None:
        await send_status(message)

    @r.message(Command("channels"))
    async def cmd_channels(message: Message) -> None:
        for chunk in await render_channels(app):
            await message.answer(chunk)

    @r.message(Command("stats"))
    async def cmd_stats(message: Message) -> None:
        await message.answer(await render_stats(app))

    @r.message(Command("pause"))
    async def cmd_pause(message: Message) -> None:
        await app.db.set_setting("paused", "1")
        await message.answer("⏸ Пересылка приостановлена. Новые публикации копятся в очереди "
                             "и уйдут после /resume (или /resume clear — очистить очередь).")

    @r.message(Command("resume"))
    async def cmd_resume(message: Message, command: CommandObject) -> None:
        if command.args and command.args.strip().lower() == "clear":
            n = await app.db.clear_queue()
            await message.answer(f"🗑 Очередь очищена ({n} шт.).")
        await app.db.set_setting("paused", "0")
        app.fw.wake()
        await message.answer("▶️ Пересылка возобновлена.")

    @r.message(Command("mode"))
    async def cmd_mode(message: Message, command: CommandObject) -> None:
        arg = (command.args or "").strip().lower()
        if arg in ("forward", "copy"):
            await app.db.set_setting("mode", arg)
            await message.answer(f"Режим: <code>{arg}</code>")
        else:
            cur = await app.db.get_setting("mode", app.settings.default_mode)
            await message.answer(
                f"Текущий режим: <code>{cur}</code>\n\n"
                "<code>/mode forward</code> — обычная пересылка (видно источник, сохраняется всё)\n"
                "<code>/mode copy</code> — копия без «Переслано из»")

    # ---- add channels
    @r.message(Command("addchannel"))
    async def cmd_add(message: Message, command: CommandObject, state: FSMContext) -> None:
        if command.args:
            await state.clear()
            await add_refs(app, message, split_refs(command.args))
        else:
            await state.set_state(AddChannelState.waiting)
            await message.answer(
                "Пришлите каналы: @username, ссылки t.me/… или инвайты t.me/+… — "
                "по одному в строке. Можно отправить .txt файл. /start — отмена.")

    @r.message(AddChannelState.waiting, F.text, ~F.text.startswith("/"))
    async def got_channels(message: Message, state: FSMContext) -> None:
        await state.clear()
        await add_refs(app, message, split_refs(message.text))

    @r.message(F.document)
    async def got_file(message: Message, state: FSMContext) -> None:
        doc = message.document
        if not (doc.file_name or "").lower().endswith((".txt", ".csv")) or doc.file_size > 1_000_000:
            await message.answer("Пришлите текстовый файл .txt со списком каналов (до 1 МБ).")
            return
        buf = await app.bot.download(doc)
        text = buf.read().decode("utf-8", errors="ignore")
        await state.clear()
        await add_refs(app, message, split_refs(text))

    @r.message(Command("import"))
    async def cmd_import(message: Message) -> None:
        path = app.settings.channels_file
        if not path.exists():
            await message.answer(f"Файл <code>{esc(path)}</code> не найден на сервере. "
                                 "Либо отправьте .txt прямо в этот чат.")
            return
        await add_refs(app, message, split_refs(path.read_text(encoding="utf-8")))

    # ---- remove channel
    @r.message(Command("removechannel"))
    async def cmd_remove(message: Message, command: CommandObject) -> None:
        if not command.args:
            await message.answer(
                "Укажите номер из /channels, @username, ссылку или id.\n"
                "Пример: <code>/removechannel 12</code> или <code>/removechannel @name</code>")
            return
        rows = await app.db.list_channels()
        done, missing = [], []
        for tok in split_refs(command.args):
            target = None
            if tok.isdigit() and 1 <= int(tok) <= len(rows):
                target = rows[int(tok) - 1]
            else:
                parsed = parse_channel_ref(tok)
                for row in rows:
                    if parsed and parsed[0] == "id" and row["id"] == parsed[1]:
                        target = row
                    elif parsed and parsed[0] == "username" and \
                            (row["username"] or "").lower() == str(parsed[1]).lower():
                        target = row
                    if target:
                        break
            if target:
                await app.fw.remove_channel(target["id"])
                done.append(f"🗑 {esc(target['title'])}")
            else:
                missing.append(f"❌ не найден: {esc(tok)}")
        await message.answer("\n".join(done + missing) or "Ничего не удалено.")

    # ---- set chat
    @r.message(Command("setchat"))
    async def cmd_setchat(message: Message, command: CommandObject, state: FSMContext) -> None:
        if command.args:
            await state.clear()
            await do_setchat(app, message, command.args.strip())
        elif message.chat.type in ("group", "supergroup", "channel"):
            await do_setchat(app, message, message.chat.id)
        else:
            await state.set_state(SetChatState.waiting)
            await message.answer(
                "Пришлите @username / ссылку / id целевого чата. Либо выполните /setchat "
                "прямо в нужной группе (бот должен быть там). Аккаунт-«читатель» тоже должен "
                "быть участником чата и иметь право писать.\n"
                "Для теста можно указать <code>me</code> (Избранное).")

    @r.message(SetChatState.waiting, F.text, ~F.text.startswith("/"))
    async def got_chat(message: Message, state: FSMContext) -> None:
        await state.clear()
        await do_setchat(app, message, message.text.strip())

    # ---- callbacks
    @r.callback_query(F.data.in_({"pause", "resume", "refresh"}))
    async def cb_main(cb: CallbackQuery) -> None:
        if cb.data == "pause":
            await app.db.set_setting("paused", "1")
        elif cb.data == "resume":
            await app.db.set_setting("paused", "0")
            app.fw.wake()
        text, kb = await render_status(app)
        try:
            await cb.message.edit_text(text, reply_markup=kb)
        except TelegramBadRequest:
            pass  # «message is not modified»
        await cb.answer("Обновлено")

    @r.callback_query(F.data == "channels")
    async def cb_channels(cb: CallbackQuery) -> None:
        for chunk in await render_channels(app):
            await cb.message.answer(chunk)
        await cb.answer()

    @r.callback_query(F.data == "stats")
    async def cb_stats(cb: CallbackQuery) -> None:
        await cb.message.answer(await render_stats(app))
        await cb.answer()

    @r.callback_query(F.data == "addchannel")
    async def cb_add(cb: CallbackQuery, state: FSMContext) -> None:
        await state.set_state(AddChannelState.waiting)
        await cb.message.answer("Пришлите каналы (по одному в строке) или .txt файл.")
        await cb.answer()

    @r.callback_query(F.data == "setchat")
    async def cb_setchat(cb: CallbackQuery, state: FSMContext) -> None:
        await state.set_state(SetChatState.waiting)
        await cb.message.answer("Пришлите @username / ссылку / id целевого чата.")
        await cb.answer()

    return r
