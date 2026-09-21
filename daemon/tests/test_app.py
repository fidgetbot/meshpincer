from pathlib import Path

import httpx
import pytest

from meshpincer.app import create_app
from meshpincer.config import Settings
from meshpincer.models import ChannelRecord, ContactRecord, InboundMessage, RadioStatus
from meshpincer.store import Store


class FakeRadioManager:
    def __init__(self) -> None:
        self.started = False

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.started = False

    async def status(self) -> RadioStatus:
        return RadioStatus()

    async def contacts(self) -> list[ContactRecord]:
        return [ContactRecord(public_key="ab" * 32, name="Test peer")]

    async def channels(self, *, include_empty: bool = False) -> list[ChannelRecord]:
        channels = [
            ChannelRecord(index=0, name="Public", configured=True, channel_hash="11"),
            ChannelRecord(index=1, name="", configured=False, channel_hash="37"),
        ]
        return channels if include_empty else channels[:1]


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(
        state_dir=tmp_path,
        socket_path=tmp_path / "meshpincer.sock",
    )


@pytest.mark.asyncio
async def test_status_starts_disconnected(settings: Settings) -> None:
    radio = FakeRadioManager()
    app = create_app(settings, radio_manager=radio)  # type: ignore[arg-type]
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
            "node_name": None,
            "public_key": None,
            "model": None,
            "firmware_version": None,
            "protocol_version": None,
            "radio": None,
            "health": None,
            "telemetry": None,
            "last_refreshed_at": None,
            "last_error": None,
        },
        "last_event_id": 0,
    }


@pytest.mark.asyncio
async def test_messages_are_initially_empty(settings: Settings) -> None:
    app = create_app(settings, radio_manager=FakeRadioManager())  # type: ignore[arg-type]
    transport = httpx.ASGITransport(app=app)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.get("/v1/messages")

    assert response.status_code == 200
    assert response.json() == []


@pytest.mark.asyncio
async def test_contacts_and_channels_come_from_radio_manager(settings: Settings) -> None:
    app = create_app(settings, radio_manager=FakeRadioManager())  # type: ignore[arg-type]
    transport = httpx.ASGITransport(app=app)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            contacts = await client.get("/v1/contacts")
            channels = await client.get("/v1/channels")
            all_channels = await client.get("/v1/channels?include_empty=true")

    assert contacts.json()[0]["name"] == "Test peer"
    assert channels.json() == [
        {"index": 0, "name": "Public", "configured": True, "channel_hash": "11"}
    ]
    assert len(all_channels.json()) == 2


@pytest.mark.asyncio
async def test_events_and_consumer_cursor_api(settings: Settings) -> None:
    app = create_app(settings, radio_manager=FakeRadioManager())  # type: ignore[arg-type]
    transport = httpx.ASGITransport(app=app)
    async with app.router.lifespan_context(app):
        store = Store(settings.database_path)
        message, _ = await store.record_inbound(
            InboundMessage(
                kind="direct",
                peer_key="cd" * 32,
                peer_key_prefix="cd" * 6,
                text="hello",
                mesh_timestamp=42,
            )
        )
        assert message.event_id is not None
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            events = await client.get("/v1/events")
            pending = await client.get("/v1/consumers/native-channel/events")
            cursor = await client.put(
                "/v1/consumers/native-channel/cursor",
                json={"event_id": message.event_id},
            )
            no_pending = await client.get("/v1/consumers/native-channel/events")
            invalid = await client.put(
                "/v1/consumers/native-channel/cursor",
                json={"event_id": message.event_id + 1},
            )

    assert events.status_code == 200
    assert events.json()[0]["kind"] == "message.received"
    assert pending.json() == events.json()
    assert cursor.json() == {
        "consumer_id": "native-channel",
        "event_id": message.event_id,
    }
    assert no_pending.json() == []
    assert invalid.status_code == 422
