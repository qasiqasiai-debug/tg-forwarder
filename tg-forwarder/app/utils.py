from __future__ import annotations

import re
import time
from datetime import datetime
from zoneinfo import ZoneInfo

_USERNAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{3,31}$")


def parse_channel_ref(raw: str) -> tuple[str, str | int] | None:
    """Разбирает ссылку/имя канала.

    Возвращает ("username", name) | ("invite", hash) | ("id", marked_id) | None.
    """
    s = raw.strip().strip("<>").strip()
    if not s:
        return None
    if re.fullmatch(r"-100\d{5,}", s):
        return "id", int(s)

    m = re.match(r"^(?:https?://)?(?:www\.)?(?:t|telegram)\.me/(.+)$", s, re.I)
    if m:
        path = m.group(1).split("?")[0].strip("/")
        parts = path.split("/")
        head = parts[0]
        if head.startswith("+"):
            return "invite", head[1:]
        if head.lower() == "joinchat" and len(parts) > 1:
            return "invite", parts[1]
        if head == "c" and len(parts) > 1 and parts[1].isdigit():
            return "id", int(f"-100{parts[1]}")
        s = head

    s = s.lstrip("@")
    if _USERNAME_RE.match(s):
        return "username", s
    return None


def split_refs(text: str) -> list[str]:
    """Достаёт ссылки из произвольного текста/файла (по строкам, # — комментарий)."""
    refs: list[str] = []
    for line in text.splitlines():
        line = line.split("#", 1)[0].strip()
        if not line:
            continue
        for tok in re.split(r"[\s,;]+", line):
            if tok:
                refs.append(tok)
    seen, out = set(), []
    for r in refs:
        if r not in seen:
            seen.add(r)
            out.append(r)
    return out


def chunk_lines(lines: list[str], limit: int = 3800) -> list[str]:
    chunks, cur, size = [], [], 0
    for ln in lines:
        if size + len(ln) + 1 > limit and cur:
            chunks.append("\n".join(cur))
            cur, size = [], 0
        cur.append(ln)
        size += len(ln) + 1
    if cur:
        chunks.append("\n".join(cur))
    return chunks


def fmt_ts(ts: int | float | None, tz: str) -> str:
    if not ts:
        return "—"
    return datetime.fromtimestamp(ts, ZoneInfo(tz)).strftime("%d.%m.%Y %H:%M:%S")


def fmt_duration(seconds: float) -> str:
    seconds = int(seconds)
    d, rem = divmod(seconds, 86400)
    h, rem = divmod(rem, 3600)
    m, _ = divmod(rem, 60)
    parts = []
    if d:
        parts.append(f"{d}д")
    if h or d:
        parts.append(f"{h}ч")
    parts.append(f"{m}м")
    return " ".join(parts)


def now() -> int:
    return int(time.time())
