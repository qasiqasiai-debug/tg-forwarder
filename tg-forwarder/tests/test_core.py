import asyncio
import tempfile
from pathlib import Path

from app.db import Database
from app.utils import chunk_lines, parse_channel_ref, split_refs


def run(coro):
    return asyncio.run(coro)


def test_parse_refs():
    assert parse_channel_ref("@durov") == ("username", "durov")
    assert parse_channel_ref("https://t.me/telegram") == ("username", "telegram")
    assert parse_channel_ref("t.me/telegram/123") == ("username", "telegram")
    assert parse_channel_ref("https://t.me/+AbCdEf123") == ("invite", "AbCdEf123")
    assert parse_channel_ref("https://t.me/joinchat/AbCdEf123") == ("invite", "AbCdEf123")
    assert parse_channel_ref("-1001234567890") == ("id", -1001234567890)
    assert parse_channel_ref("https://t.me/c/1234567890/5") == ("id", -1001234567890)
    assert parse_channel_ref("###") is None
    assert parse_channel_ref("ab") is None


def test_split_refs():
    text = "# заголовок\n@one, @two\nhttps://t.me/three # коммент\n@one\n"
    assert split_refs(text) == ["@one", "@two", "https://t.me/three"]


def test_chunk():
    lines = ["x" * 1000] * 10
    chunks = chunk_lines(lines, 3800)
    assert all(len(c) <= 3800 for c in chunks) and len(chunks) == 4  # по 3 строки в чанке


def test_queue_dedup_order_and_stats():
    async def scenario():
        with tempfile.TemporaryDirectory() as d:
            db = Database(Path(d) / "t.db")
            await db.init()
            await db.add_channel(-1001, "@a", "a", "Chan A", 0)

            # дедупликация очереди
            assert await db.enqueue(-1001, [10], 200) is True
            assert await db.enqueue(-1001, [10], 200) is False
            # альбом = одна публикация; более ранняя дата уходит первой
            assert await db.enqueue(-1001, [8, 9, 7], 100) is True
            assert await db.queue_size() == 2

            first = await db.next_item()
            assert first.msg_ids == [7, 8, 9]
            await db.finish_item(first)

            # уже отправленное не ставится повторно, даже частью альбома
            assert await db.enqueue(-1001, [9], 100) is False
            assert await db.enqueue(-1001, [7, 8, 9], 100) is False

            second = await db.next_item()
            await db.bump_attempt(second.id, "boom")
            assert (await db.next_item()).attempts == 1
            await db.finish_item(second)

            assert await db.queue_size() == 0
            assert await db.count_forwarded() == 2  # альбом считается как одна публикация
            assert await db.get_setting("last_success_ts")
            top = await db.top_channels(0)
            assert top[0]["n"] == 2

            # пропущенный (удалённый) пост не считается пересланным
            await db.enqueue(-1001, [11], 300)
            it = await db.next_item()
            await db.finish_item(it, forwarded=False)
            assert await db.count_forwarded() == 2

            # курсор не откатывается назад
            await db.set_cursor(-1001, 50)
            await db.set_cursor(-1001, 20)
            assert (await db.get_channel(-1001))["cursor"] == 50

            # настройки и очередь переживают переоткрытие базы (перезапуск сервера)
            await db.enqueue(-1001, [12], 400)
            await db.set_setting("paused", "1")
            await db.close()
            db2 = Database(Path(d) / "t.db")
            await db2.init()
            assert await db2.get_setting("paused") == "1"
            assert await db2.queue_size() == 1
            assert await db2.count_channels() == (1, 1)
            await db2.close()

    run(scenario())
