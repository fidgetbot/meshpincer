from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _default_state_dir() -> Path:
    return Path.home() / ".openclaw" / "state" / "meshpincer"


def _env_int(name: str, default: int) -> int:
    value = os.environ.get(name)
    return default if value is None else int(value, 0)


def _env_int_set(name: str) -> frozenset[int]:
    value = os.environ.get(name, "")
    if not value.strip():
        return frozenset()
    return frozenset(int(item.strip(), 0) for item in value.split(",") if item.strip())


@dataclass(frozen=True, slots=True)
class Settings:
    state_dir: Path
    socket_path: Path
    serial_port: str | None = None
    usb_vid: int = 0x2886
    usb_pid: int = 0x1667
    usb_serial: str | None = None
    query_timeout_seconds: float = 5.0
    refresh_interval_seconds: float = 30.0
    reconnect_initial_seconds: float = 1.0
    reconnect_max_seconds: float = 30.0
    direct_global_cooldown_seconds: float = 5.0
    direct_peer_cooldown_seconds: float = 30.0
    channel_send_allowlist: frozenset[int] = frozenset()
    channel_global_cooldown_seconds: float = 30.0
    channel_per_channel_cooldown_seconds: float = 300.0
    repeater_status_timeout_seconds: float = 15.0
    repeater_status_global_cooldown_seconds: float = 30.0
    repeater_status_peer_cooldown_seconds: float = 60.0
    repeater_acl_timeout_seconds: float = 15.0
    repeater_acl_global_cooldown_seconds: float = 30.0
    repeater_acl_peer_cooldown_seconds: float = 60.0
    repeater_mutation_timeout_seconds: float = 15.0
    repeater_mutation_global_cooldown_seconds: float = 60.0
    repeater_mutation_peer_cooldown_seconds: float = 120.0
    repeater_login_timeout_seconds: float = 15.0
    repeater_login_global_cooldown_seconds: float = 30.0
    repeater_login_peer_cooldown_seconds: float = 60.0
    retention_days: int = 30
    max_messages: int = 50_000
    cursor_max_idle_days: int = 30
    housekeeping_interval_seconds: float = 86_400.0

    def __post_init__(self) -> None:
        if self.retention_days < 1:
            raise ValueError("retention_days must be at least 1")
        if self.max_messages < 1:
            raise ValueError("max_messages must be at least 1")
        if self.cursor_max_idle_days < 1:
            raise ValueError("cursor_max_idle_days must be at least 1")
        if self.housekeeping_interval_seconds <= 0:
            raise ValueError("housekeeping_interval_seconds must be positive")

    @property
    def database_path(self) -> Path:
        return self.state_dir / "meshpincer.db"

    @classmethod
    def from_env(cls) -> Settings:
        state_dir = Path(os.environ.get("MESHPINCER_STATE_DIR", _default_state_dir()))
        socket_path = Path(os.environ.get("MESHPINCER_SOCKET", state_dir / "meshpincer.sock"))
        return cls(
            state_dir=state_dir,
            socket_path=socket_path,
            serial_port=os.environ.get("MESHPINCER_SERIAL_PORT"),
            usb_vid=_env_int("MESHPINCER_USB_VID", 0x2886),
            usb_pid=_env_int("MESHPINCER_USB_PID", 0x1667),
            usb_serial=os.environ.get("MESHPINCER_USB_SERIAL"),
            query_timeout_seconds=float(os.environ.get("MESHPINCER_QUERY_TIMEOUT", "5")),
            refresh_interval_seconds=float(os.environ.get("MESHPINCER_REFRESH_INTERVAL", "30")),
            reconnect_initial_seconds=float(os.environ.get("MESHPINCER_RECONNECT_INITIAL", "1")),
            reconnect_max_seconds=float(os.environ.get("MESHPINCER_RECONNECT_MAX", "30")),
            direct_global_cooldown_seconds=float(
                os.environ.get("MESHPINCER_DIRECT_GLOBAL_COOLDOWN", "5")
            ),
            direct_peer_cooldown_seconds=float(
                os.environ.get("MESHPINCER_DIRECT_PEER_COOLDOWN", "30")
            ),
            channel_send_allowlist=_env_int_set("MESHPINCER_CHANNEL_SEND_ALLOWLIST"),
            channel_global_cooldown_seconds=float(
                os.environ.get("MESHPINCER_CHANNEL_GLOBAL_COOLDOWN", "30")
            ),
            channel_per_channel_cooldown_seconds=float(
                os.environ.get("MESHPINCER_CHANNEL_PER_CHANNEL_COOLDOWN", "300")
            ),
            repeater_status_timeout_seconds=float(
                os.environ.get("MESHPINCER_REPEATER_STATUS_TIMEOUT", "15")
            ),
            repeater_status_global_cooldown_seconds=float(
                os.environ.get("MESHPINCER_REPEATER_STATUS_GLOBAL_COOLDOWN", "30")
            ),
            repeater_status_peer_cooldown_seconds=float(
                os.environ.get("MESHPINCER_REPEATER_STATUS_PEER_COOLDOWN", "60")
            ),
            repeater_acl_timeout_seconds=float(
                os.environ.get("MESHPINCER_REPEATER_ACL_TIMEOUT", "15")
            ),
            repeater_acl_global_cooldown_seconds=float(
                os.environ.get("MESHPINCER_REPEATER_ACL_GLOBAL_COOLDOWN", "30")
            ),
            repeater_acl_peer_cooldown_seconds=float(
                os.environ.get("MESHPINCER_REPEATER_ACL_PEER_COOLDOWN", "60")
            ),
            repeater_mutation_timeout_seconds=float(
                os.environ.get("MESHPINCER_REPEATER_MUTATION_TIMEOUT", "15")
            ),
            repeater_mutation_global_cooldown_seconds=float(
                os.environ.get("MESHPINCER_REPEATER_MUTATION_GLOBAL_COOLDOWN", "60")
            ),
            repeater_mutation_peer_cooldown_seconds=float(
                os.environ.get("MESHPINCER_REPEATER_MUTATION_PEER_COOLDOWN", "120")
            ),
            repeater_login_timeout_seconds=float(
                os.environ.get("MESHPINCER_REPEATER_LOGIN_TIMEOUT", "15")
            ),
            repeater_login_global_cooldown_seconds=float(
                os.environ.get("MESHPINCER_REPEATER_LOGIN_GLOBAL_COOLDOWN", "30")
            ),
            repeater_login_peer_cooldown_seconds=float(
                os.environ.get("MESHPINCER_REPEATER_LOGIN_PEER_COOLDOWN", "60")
            ),
            retention_days=_env_int("MESHPINCER_RETENTION_DAYS", 30),
            max_messages=_env_int("MESHPINCER_MAX_MESSAGES", 50_000),
            cursor_max_idle_days=_env_int("MESHPINCER_CURSOR_MAX_IDLE_DAYS", 30),
            housekeeping_interval_seconds=float(
                os.environ.get("MESHPINCER_HOUSEKEEPING_INTERVAL", "86400")
            ),
        )
