from __future__ import annotations

import stat

import uvicorn

from .config import Settings


def main() -> None:
    settings = Settings.from_env()
    settings.state_dir.mkdir(parents=True, exist_ok=True)

    if settings.socket_path.exists():
        mode = settings.socket_path.stat().st_mode
        if not stat.S_ISSOCK(mode):
            raise RuntimeError(f"refusing to replace non-socket path: {settings.socket_path}")
        settings.socket_path.unlink()

    uvicorn.run(
        "meshpincer.app:app",
        uds=str(settings.socket_path),
        log_level="info",
    )


if __name__ == "__main__":
    main()
