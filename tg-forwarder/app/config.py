from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


class ConfigError(Exception):
    pass


@dataclass(frozen=True)
class Settings:
    api_id: int
    api_hash: str
    bot_token: str
    session_string: str
    owner_id: int
    data_dir: Path
    timezone: str
    expected_channels: int
    default_mode: str
    send_interval: float
    poll_interval: int
    backfill_limit: int
    catchup_max_age_hours: int
    join_delay: float
    channels_file: Path

    @property
    def db_path(self) -> Path:
        return self.data_dir / "forwarder.db"

    @property
    def log_dir(self) -> Path:
        return self.data_dir / "logs"


def load_settings() -> Settings:
    load_dotenv()
    env = os.environ
    required = ["API_ID", "API_HASH", "BOT_TOKEN", "SESSION_STRING", "OWNER_ID"]
    missing = [k for k in required if not env.get(k, "").strip()]
    if missing:
        raise ConfigError("Не заданы переменные окружения: " + ", ".join(missing))
    try:
        api_id = int(env["API_ID"])
        owner_id = int(env["OWNER_ID"])
    except ValueError as e:
        raise ConfigError("API_ID и OWNER_ID должны быть числами") from e

    mode = env.get("DEFAULT_MODE", "forward").strip().lower()
    if mode not in ("forward", "copy"):
        raise ConfigError("DEFAULT_MODE должен быть forward или copy")

    def num(name: str, default, cast):
        try:
            return cast(env.get(name, default))
        except ValueError as e:
            raise ConfigError(f"{name} должен быть числом") from e

    data_dir = Path(env.get("DATA_DIR", "./data")).expanduser()
    data_dir.mkdir(parents=True, exist_ok=True)
    (data_dir / "logs").mkdir(parents=True, exist_ok=True)

    return Settings(
        api_id=api_id,
        api_hash=env["API_HASH"].strip(),
        bot_token=env["BOT_TOKEN"].strip(),
        session_string=env["SESSION_STRING"].strip(),
        owner_id=owner_id,
        data_dir=data_dir,
        timezone=env.get("TIMEZONE", "Europe/Moscow"),
        expected_channels=num("EXPECTED_CHANNELS", 54, int),
        default_mode=mode,
        send_interval=num("SEND_INTERVAL", 1.5, float),
        poll_interval=num("POLL_INTERVAL", 120, int),
        backfill_limit=num("BACKFILL_LIMIT", 100, int),
        catchup_max_age_hours=num("CATCHUP_MAX_AGE_HOURS", 48, int),
        join_delay=num("JOIN_DELAY", 5, float),
        channels_file=Path(env.get("CHANNELS_FILE", "./channels.txt")).expanduser(),
    )
