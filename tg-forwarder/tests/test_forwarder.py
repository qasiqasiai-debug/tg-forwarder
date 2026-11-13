"""Проверка доставки на заглушке клиента (без обращения к Telegram)."""
import asyncio
import tempfile
from pathlib import Path
from types import SimpleNamespace

from telethon.errors import ChatWriteForbiddenError, FloodWaitError

from app.config import Settings
from app.db import Database
from app.forwarder import Forwarder


class FakeClient:
    def __init__(self):
        self.fail_with = None
        self.sent = []

    def is_connected(self):
        return True

    async def get_input_entity(self, _):
        return "peer"

    async def get_messages(self, cid, ids=None, **_):
        return [SimpleNamespace(id=i) for i in ids]

    async def forward_messages(self, target, msgs):
        if self.fail_with:
            raise self.fail_with
        self.sent.append([m.id for m in msgs])


def make(tmp):
    s = Settings(1, "h", "t", "s", 1, Path(tmp), "Europe/Moscow", 54, "forward",
                 0.0, 120, 100, 48, 0.0, Path(tmp) / "c.txt")
    return s


def test_target_down_keeps_queue_then_recovers():
    async def scenario():
        with tempfile.TemporaryDirectory() as d:
            db = Database(Path(d) / "t.db")
            await db.init()
            fake = FakeClient()
            fw = Forwarder(make(d), db, fake)
            fw.target_id = -100999
            await db.enqueue(-1001, [5], 100)

            # целевой чат недоступен -> публикация остаётся в очереди, не теряется
            fake.fail_with = ChatWriteForbiddenError(request=None)
            item = await db.next_item()
            assert await fw._deliver(item) is False
            assert await db.queue_size() == 1
            assert (await db.next_item()).attempts == 0  # не «сгорает» по попыткам

            # чат восстановился -> уходит и фиксируется
            fake.fail_with = None
            assert await fw._deliver(await db.next_item()) is True
            assert fake.sent == [[5]]
            assert await db.queue_size() == 0 and await db.count_forwarded() == 1

            # повторное поступление того же поста не дублирует отправку
            assert await db.enqueue(-1001, [5], 100) is False

            # FloodWait -> ждём и повторяем (элемент остаётся)
            await db.enqueue(-1001, [6], 101)
            fake.fail_with = FloodWaitError(request=None, capture=0)
            assert await fw._deliver(await db.next_item()) is False
            assert await db.queue_size() == 1
            await db.close()

    asyncio.run(scenario())
