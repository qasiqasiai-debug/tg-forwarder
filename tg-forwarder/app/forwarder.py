"""Ядро: мониторинг каналов через Telethon (MTProto) и доставка в целевой чат."""
from __future__ import annotations

import asyncio
import logging
import tempfile
import time
from pathlib import Path
from typing import Awaitable, Callable

from telethon import TelegramClient, events, utils as tl_utils
from telethon.errors import (
    ChannelPrivateError,
    ChatAdminRequiredError,
    ChatForwardsRestrictedError,
    ChatWriteForbiddenError,
    FloodWaitError,
    InviteHashExpiredError,
    InviteHashInvalidError,
    InviteRequestSentError,
    MediaCaptionTooLongError,
    MessageIdInvalidError,
    PeerIdInvalidError,
    RPCError,
    SlowModeWaitError,
    UserAlreadyParticipantError,
    UserBannedInChannelError,
    UsernameInvalidError,
    UsernameNotOccupiedError,
)
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import CheckChatInviteRequest, ImportChatInviteRequest
from telethon.tl.types import Channel, MessageMediaWebPage, PeerChannel

from .config import Settings
from .db import Database, QueueItem
from .utils import now, parse_channel_ref

log = logging.getLogger("forwarder")

# Ошибки «целевой чат недоступен» — ждём и повторяем без ограничения попыток.
TARGET_ERRORS = (
    ChatWriteForbiddenError,
    ChatAdminRequiredError,
    UserBannedInChannelError,
    PeerIdInvalidError,
    ConnectionError,
    OSError,
    asyncio.TimeoutError,
)
# Ошибки «пост недоступен» — пропускаем публикацию.
SOURCE_GONE_ERRORS = (MessageIdInvalidError, ChannelPrivateError)

MAX_ATTEMPTS = 8
Notifier = Callable[[str], Awaitable[None]]


class Forwarder:
    def __init__(self, settings: Settings, db: Database, client: TelegramClient):
        self.s = settings
        self.db = db
        self.client = client
        self.notify: Notifier | None = None

        self.channel_ids: set[int] = set()
        self.target_id: int | None = None
        self.target_title: str = ""
        self._target_peer = None

        self._wake = asyncio.Event()
        self.started_at = time.time()
        self.flood_waits = 0
        self.errors = 0
        self.last_error: str = ""
        self._notified: dict[str, float] = {}
        self._last_dialog_refresh = 0.0
        self._handlers_registered = False
        self.mtproto_ok = False

    # ------------------------------------------------------------------ utils
    async def _notify(self, key: str, text: str, every: int = 1800) -> None:
        """Уведомление владельцу, не чаще раза в `every` секунд на один ключ."""
        t = time.time()
        if t - self._notified.get(key, 0) < every:
            return
        self._notified[key] = t
        if self.notify:
            try:
                await self.notify(text)
            except Exception:  # noqa: BLE001
                log.exception("Не удалось отправить уведомление владельцу")

    def _record_error(self, text: str) -> None:
        self.errors += 1
        self.last_error = text[:300]

    @property
    def connected(self) -> bool:
        return self.client.is_connected() and self.mtproto_ok

    @property
    def paused_key(self) -> str:
        return "paused"

    async def is_paused(self) -> bool:
        return (await self.db.get_setting("paused", "0")) == "1"

    # ------------------------------------------------------------------ start
    async def start(self) -> None:
        await self.reload_channels()
        await self._refresh_dialogs(force=True)
        await self._load_target()
        await self.verify_channels()
        if not self._handlers_registered:
            self.client.add_event_handler(self._on_message, events.NewMessage())
            self.client.add_event_handler(self._on_album, events.Album())
            self._handlers_registered = True
        self.mtproto_ok = True
        log.info("Forwarder запущен: каналов=%d, целевой чат=%s",
                 len(self.channel_ids), self.target_id)

    async def reload_channels(self) -> None:
        self.channel_ids = {r["id"] for r in await self.db.list_channels()}

    async def _refresh_dialogs(self, force: bool = False) -> dict[int, object]:
        """Загружает диалоги (кэш сущностей Telethon) и возвращает {marked_id: entity}."""
        if not force and time.time() - self._last_dialog_refresh < 60:
            return {}
        self._last_dialog_refresh = time.time()
        found: dict[int, object] = {}
        try:
            async for d in self.client.iter_dialogs():
                found[d.id] = d.entity
        except Exception as e:  # noqa: BLE001
            log.warning("Не удалось загрузить диалоги: %s", e)
        return found

    async def verify_channels(self) -> None:
        dialogs = await self._refresh_dialogs(force=True)
        for ch in await self.db.list_channels():
            cid = ch["id"]
            ent = dialogs.get(cid)
            if ent is None:
                await self.db.set_channel_status(
                    cid, "error", "аккаунт не подписан на канал (не найден в диалогах)")
                continue
            title = getattr(ent, "title", None)
            await self.db.set_channel_status(cid, "ok", None, title)

    async def _load_target(self) -> None:
        raw = await self.db.get_setting("target_chat")
        self.target_id = int(raw) if raw else None
        self.target_title = await self.db.get_setting("target_title", "") or ""
        self._target_peer = None
        if self.target_id is not None:
            try:
                self._target_peer = await self.client.get_input_entity(self.target_id)
            except Exception as e:  # noqa: BLE001
                log.warning("Целевой чат %s пока не резолвится: %s", self.target_id, e)

    # ----------------------------------------------------------- target chat
    async def set_target(self, ref: str | int) -> tuple[bool, str]:
        try:
            if isinstance(ref, int):
                entity = await self.client.get_entity(ref)
            elif str(ref).lower() in ("me", "self"):
                entity = await self.client.get_me()
            else:
                parsed = parse_channel_ref(str(ref))
                if parsed and parsed[0] == "invite":
                    entity = await self._import_invite(str(parsed[1]))
                elif parsed and parsed[0] == "id":
                    entity = await self.client.get_entity(int(parsed[1]))
                elif parsed:
                    entity = await self.client.get_entity(str(parsed[1]))
                else:
                    try:
                        entity = await self.client.get_entity(int(str(ref)))
                    except ValueError:
                        return False, "Не удалось распознать чат. Пришлите @username, ссылку или id."
        except FloodWaitError as e:
            return False, f"FloodWait: повторите через {e.seconds} сек."
        except Exception as e:  # noqa: BLE001
            return False, ("Не удалось найти чат. Аккаунт-«читатель» должен состоять в этом чате "
                           f"и иметь право писать. ({type(e).__name__})")

        tid = tl_utils.get_peer_id(entity)
        title = tl_utils.get_display_name(entity) or str(tid)
        if tid in self.channel_ids:
            return False, "Этот чат входит в список мониторинга — это создаст петлю."
        try:
            await self.client.send_message(
                entity, "✅ Целевой чат подключён. Сюда будут приходить публикации из каналов.")
        except Exception as e:  # noqa: BLE001
            return False, f"Не могу писать в этот чат: {type(e).__name__}. Проверьте права аккаунта."
        await self.db.set_setting("target_chat", str(tid))
        await self.db.set_setting("target_title", title)
        self.target_id, self.target_title = tid, title
        self._target_peer = await self.client.get_input_entity(entity)
        self._wake.set()
        return True, title

    # -------------------------------------------------------------- channels
    async def _import_invite(self, invite_hash: str):
        try:
            upd = await self.client(ImportChatInviteRequest(invite_hash))
            await asyncio.sleep(self.s.join_delay)
            return upd.chats[0]
        except UserAlreadyParticipantError:
            info = await self.client(CheckChatInviteRequest(invite_hash))
            chat = getattr(info, "chat", None)
            if chat is None:
                raise
            return chat

    async def add_channel(self, ref: str) -> tuple[str, str]:
        """Возвращает (status, text); status: added | exists | error. FloodWait пробрасывается."""
        parsed = parse_channel_ref(ref)
        if not parsed:
            return "error", "не удалось распознать ссылку/имя"
        kind, value = parsed
        try:
            if kind == "invite":
                entity = await self._import_invite(str(value))
            elif kind == "id":
                entity = await self.client.get_entity(int(value))
            else:
                entity = await self.client.get_entity(str(value))
        except (UsernameNotOccupiedError, UsernameInvalidError):
            return "error", "канал с таким именем не найден"
        except (InviteHashExpiredError, InviteHashInvalidError):
            return "error", "инвайт-ссылка недействительна или просрочена"
        except InviteRequestSentError:
            return "error", "заявка на вступление отправлена — дождитесь одобрения и добавьте снова"
        except ValueError:
            return "error", "канал не найден (для -100… id аккаунт должен уже быть подписан)"

        if not isinstance(entity, Channel) or not entity.broadcast:
            return "error", "это не канал (нужен broadcast-канал)"

        cid = tl_utils.get_peer_id(entity)
        title = entity.title or str(cid)
        if self.target_id == cid:
            return "error", "это целевой чат"
        if await self.db.get_channel(cid):
            return "exists", title

        if getattr(entity, "left", False):
            await self.client(JoinChannelRequest(entity))
            await asyncio.sleep(self.s.join_delay)

        latest = await self._latest_id(entity)
        await self.db.add_channel(cid, ref, entity.username, title, latest)
        self.channel_ids.add(cid)
        log.info("Канал добавлен: %s (%s), курсор=%s", title, cid, latest)
        return "added", title

    async def _latest_id(self, entity) -> int:
        msgs = await self.client.get_messages(entity, limit=1)
        return msgs[0].id if msgs else 0

    async def remove_channel(self, cid: int) -> None:
        await self.db.remove_channel(cid)
        self.channel_ids.discard(cid)

    # ---------------------------------------------------------------- events
    async def _on_message(self, event: events.NewMessage.Event) -> None:
        try:
            msg = event.message
            if event.chat_id not in self.channel_ids or msg.action:
                return
            if msg.grouped_id:  # альбомы обрабатывает _on_album
                return
            await self._enqueue(event.chat_id, [msg])
        except Exception:  # noqa: BLE001
            log.exception("Ошибка в обработчике сообщения")

    async def _on_album(self, event: events.Album.Event) -> None:
        try:
            if event.chat_id not in self.channel_ids:
                return
            await self._enqueue(event.chat_id, list(event.messages))
        except Exception:  # noqa: BLE001
            log.exception("Ошибка в обработчике альбома")

    async def _enqueue(self, cid: int, msgs: list) -> bool:
        ids = [m.id for m in msgs]
        date = int(min(m.date.timestamp() for m in msgs))
        added = await self.db.enqueue(cid, ids, date)
        if added:
            log.info("В очередь: канал=%s ids=%s", cid, ids)
            self._wake.set()
        return added

    # -------------------------------------------------------------- backfill
    async def backfill(self, ch) -> int:
        """Догоняет пропущенные посты канала (после простоя / как страховка опроса)."""
        cid = ch["id"]
        try:
            peer = await self.client.get_input_entity(cid)
        except ValueError:
            await self._refresh_dialogs()
            try:
                peer = await self.client.get_input_entity(cid)
            except ValueError:
                await self.db.set_channel_status(cid, "error", "канал недоступен аккаунту")
                return 0

        cursor = ch["cursor"]
        try:
            collected = []
            async for m in self.client.iter_messages(
                    peer, min_id=cursor, limit=self.s.backfill_limit, reverse=True):
                collected.append(m)
        except FloodWaitError as e:
            self.flood_waits += 1
            log.warning("FloodWait при опросе %s: %s сек", cid, e.seconds)
            await asyncio.sleep(min(e.seconds, 300))
            return 0
        except ChannelPrivateError:
            await self.db.set_channel_status(cid, "error", "нет доступа (отписка/бан)")
            return 0

        if not collected:
            if ch["status"] != "ok":
                await self.db.set_channel_status(cid, "ok")
            return 0

        horizon = time.time() - self.s.catchup_max_age_hours * 3600
        settle = time.time() - 15  # не трогаем совсем свежие: их доставит событие целиком
        groups: dict[int, list] = {}
        order: list[list] = []
        new_cursor = cursor
        for m in collected:
            if m.date.timestamp() > settle:
                break
            new_cursor = max(new_cursor, m.id)
            if m.action or m.date.timestamp() < horizon:
                continue
            if m.grouped_id:
                if m.grouped_id not in groups:
                    groups[m.grouped_id] = []
                    order.append(groups[m.grouped_id])
                groups[m.grouped_id].append(m)
            else:
                order.append([m])

        added = 0
        for item in order:
            if await self._enqueue(cid, item):
                added += 1
        await self.db.set_cursor(cid, new_cursor)
        if ch["status"] != "ok":
            await self.db.set_channel_status(cid, "ok")
        return added

    async def backfill_all(self) -> int:
        total = 0
        for ch in await self.db.list_channels():
            total += await self.backfill(ch)
            await asyncio.sleep(0.4)
        if total:
            log.info("Догнано пропущенных публикаций: %d", total)
        return total

    # ---------------------------------------------------------------- loops
    async def run_poller(self) -> None:
        await asyncio.sleep(3)
        await self.backfill_all()  # догоняем всё, что было во время простоя
        while True:
            await asyncio.sleep(self.s.poll_interval)
            await self.backfill_all()

    async def run_watchdog(self) -> None:
        """Следит за соединением MTProto и переподключает при необходимости."""
        was_down = False
        while True:
            await asyncio.sleep(20)
            try:
                if not self.client.is_connected():
                    was_down = True
                    self.mtproto_ok = False
                    log.warning("MTProto отключён — переподключаюсь")
                    await self.client.connect()
                if self.client.is_connected():
                    if was_down:
                        was_down = False
                        log.info("MTProto снова подключён, догоняю пропущенное")
                        await self._notify("reconnect", "🟢 Соединение с Telegram восстановлено.", 300)
                        await self._load_target()
                        await self.backfill_all()
                    self.mtproto_ok = True
            except Exception as e:  # noqa: BLE001
                self._record_error(f"watchdog: {e}")
                log.warning("Ошибка переподключения: %s", e)
                await self._notify("down", "🔴 Нет связи с Telegram (MTProto). Пробую переподключиться.")

    async def run_worker(self) -> None:
        """Последовательная доставка очереди (порядок сохраняется)."""
        backoff = 5
        while True:
            self._wake.clear()
            item = await self.db.next_item()
            if item is None:
                try:
                    await asyncio.wait_for(self._wake.wait(), timeout=30)
                except asyncio.TimeoutError:
                    pass
                continue

            if await self.is_paused() or self.target_id is None:
                if self.target_id is None:
                    await self._notify("no_target", "⚠️ Целевой чат не задан: публикации копятся в очереди. "
                                                    "Установите его командой /setchat", 6 * 3600)
                await asyncio.sleep(3)
                continue

            done = await self._deliver(item)
            if done:
                backoff = 5
                await asyncio.sleep(self.s.send_interval)
            else:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 300)

    # --------------------------------------------------------------- deliver
    async def _deliver(self, item: QueueItem) -> bool:
        """True — элемент обработан (отправлен/пропущен), False — повторить позже."""
        try:
            if self._target_peer is None:
                self._target_peer = await self.client.get_input_entity(self.target_id)

            try:
                msgs = await self.client.get_messages(item.channel_id, ids=item.msg_ids)
            except ValueError:
                await self._refresh_dialogs()
                msgs = await self.client.get_messages(item.channel_id, ids=item.msg_ids)
            msgs = [m for m in msgs if m]
            if not msgs:
                log.info("Пост удалён в источнике, пропускаю: %s", item)
                await self.db.finish_item(item, forwarded=False)
                return True

            await self._send(self._target_peer, msgs)
            await self.db.finish_item(item)
            log.info("Переслано: канал=%s ids=%s", item.channel_id, item.msg_ids)
            return True

        except (FloodWaitError, SlowModeWaitError) as e:
            self.flood_waits += 1
            wait = int(getattr(e, "seconds", 30)) + 1
            log.warning("FloodWait %s сек — жду", wait)
            await asyncio.sleep(wait)
            return False
        except SOURCE_GONE_ERRORS as e:
            log.warning("Источник недоступен, пропускаю %s: %s", item, type(e).__name__)
            self._record_error(f"источник: {type(e).__name__}")
            await self.db.finish_item(item, forwarded=False)
            return True
        except TARGET_ERRORS as e:
            self._record_error(f"целевой чат/сеть: {type(e).__name__}")
            self._target_peer = None
            await self.db.bump_attempt(item.id, f"{type(e).__name__}: {e}", count=False)
            log.warning("Целевой чат/сеть недоступны (%s) — оставляю в очереди", type(e).__name__)
            await self._notify(
                "target_down",
                f"⚠️ Не удаётся отправить в целевой чат ({type(e).__name__}). "
                "Публикации сохраняются в очереди и уйдут автоматически после восстановления.")
            return False
        except Exception as e:  # noqa: BLE001
            self._record_error(f"{type(e).__name__}: {e}")
            log.exception("Ошибка пересылки %s", item)
            attempts = item.attempts + 1
            await self.db.bump_attempt(item.id, f"{type(e).__name__}: {e}")
            if attempts >= MAX_ATTEMPTS:
                log.error("Публикация отброшена после %d попыток: %s", attempts, item)
                await self.db.finish_item(item, forwarded=False)
                await self._notify(
                    f"drop{item.id}",
                    f"❌ Публикация не переслана после {attempts} попыток и пропущена.\n"
                    f"Канал: {item.channel_id}, сообщения: {item.msg_ids}\n"
                    f"Причина: {type(e).__name__}: {e}", 0)
                return True
            return False

    async def _send(self, target, msgs: list) -> None:
        mode = await self.db.get_setting("mode", self.s.default_mode)
        if mode == "forward":
            try:
                await self.client.forward_messages(target, msgs)
                return
            except ChatForwardsRestrictedError:
                log.info("Канал запрещает пересылку — копирую через скачивание")
        await self._copy(target, msgs)

    async def _copy(self, target, msgs: list) -> None:
        """Копия без «Переслано из». Сначала пробуем по file_id, при запрете — скачиваем."""
        try:
            await self._copy_by_reference(target, msgs)
        except (ChatForwardsRestrictedError, MediaCaptionTooLongError, RPCError) as e:
            if isinstance(e, (FloodWaitError, SlowModeWaitError)) or isinstance(e, TARGET_ERRORS):
                raise
            log.info("Копия по ссылке не удалась (%s) — скачиваю и загружаю", type(e).__name__)
            await self._copy_with_download(target, msgs)

    async def _copy_by_reference(self, target, msgs: list) -> None:
        if len(msgs) == 1:
            await self.client.send_message(target, msgs[0])
            return
        media = [m.media for m in msgs if m.media]
        caps = [m.text or "" for m in msgs if m.media]
        await self.client.send_file(target, media, caption=caps)

    async def _copy_with_download(self, target, msgs: list) -> None:
        with tempfile.TemporaryDirectory(prefix="fwd_") as tmp:
            files, caps, texts = [], [], []
            for m in msgs:
                has_file = m.media and not isinstance(m.media, MessageMediaWebPage)
                if has_file:
                    path = await self.client.download_media(m, file=Path(tmp) / f"{m.id}")
                    files.append(path)
                    caps.append(m.text or "")
                elif m.text:
                    texts.append(m)
            if not files:
                for m in texts:
                    await self.client.send_message(target, m.text, link_preview=False)
                return
            try:
                await self.client.send_file(
                    target, files if len(files) > 1 else files[0],
                    caption=caps if len(files) > 1 else caps[0])
            except MediaCaptionTooLongError:
                await self.client.send_file(target, files if len(files) > 1 else files[0])
                for c in caps:
                    if c:
                        await self.client.send_message(target, c, link_preview=False)
            for m in texts:
                await self.client.send_message(target, m.text, link_preview=False)

    # ----------------------------------------------------------------- misc
    def wake(self) -> None:
        self._wake.set()
