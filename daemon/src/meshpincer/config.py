from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _default_state_dir() -> Path:
    return Path.home() / ".openclaw" / "state" / "meshpincer"


@dataclass(frozen=True, slots=True)
class Settings:
    state_dir: Path
    socket_path: Path
    serial_port: str | None = None

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
        )
