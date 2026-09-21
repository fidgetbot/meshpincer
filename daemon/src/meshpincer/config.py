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
        )
