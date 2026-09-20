from pathlib import Path

import httpx
import pytest

from meshpincer.app import create_app
from meshpincer.config import Settings


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(
        state_dir=tmp_path,
        socket_path=tmp_path / "meshpincer.sock",
    )


@pytest.mark.asyncio
async def test_status_starts_disconnected(settings: Settings) -> None:
    app = create_app(settings)
    transport = httpx.ASGITransport(app=app)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.get("/v1/status")

    assert response.status_code == 200
    assert response.json() == {
        "service": "meshpincer",
        "version": "0.1.0",
        "radio": {
            "connected": False,
            "serial_port": None,
            "public_key": None,
            "firmware_version": None,
            "last_error": None,
        },
        "last_event_id": 0,
    }


@pytest.mark.asyncio
async def test_messages_are_initially_empty(settings: Settings) -> None:
    app = create_app(settings)
    transport = httpx.ASGITransport(app=app)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.get("/v1/messages")

    assert response.status_code == 200
    assert response.json() == []
