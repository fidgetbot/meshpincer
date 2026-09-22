from pathlib import Path

import httpx
import pytest

from meshpincer.app import create_app
from meshpincer.config import Settings
from meshpincer.models import (
    ChannelRecord,
    ContactRecord,
    InboundMessage,
    RadioStatus,
    RepeaterStatus,
)
from meshpincer.radio import DirectSendOutcome
from meshpincer.store import Store


class FakeRadioManager:
    def __init__(self) -> None:
        self.started = False
        self.contact_mutations: list[tuple[str, str]] = []
        self.channel_mutations: list[tuple[str, int, str]] = []
        self.contact_records = {"ab" * 32: ContactRecord(public_key="ab" * 32, name="Test peer")}
        self.channel_records = [
            ChannelRecord(index=0, name="Public", configured=True, channel_hash="11"),
            ChannelRecord(index=1, name="", configured=False, channel_hash="37"),
        ]

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.started = False

    async def status(self) -> RadioStatus:
        return RadioStatus()

    async def contacts(self) -> list[ContactRecord]:
        return list(self.contact_records.values())

    async def upsert_contact(
        self,
        public_key: str,
        name: str,
        node_type: int,
        flags: int,
    ) -> ContactRecord:
        record = ContactRecord(
            public_key=public_key,
            name=name,
            node_type=node_type,
            flags=flags,
            path_length=-1,
        )
        self.contact_records[public_key] = record
        self.contact_mutations.append(("upsert", public_key))
        return record

    async def remove_contact(self, public_key: str) -> str:
        if public_key not in self.contact_records:
            raise ValueError("contact is not known to the companion radio")
        del self.contact_records[public_key]
        self.contact_mutations.append(("remove", public_key))
        return public_key

    async def channels(self, *, include_empty: bool = False) -> list[ChannelRecord]:
        return (
            list(self.channel_records)
            if include_empty
            else [channel for channel in self.channel_records if channel.configured]
        )

    async def set_channel(
        self,
        channel_index: int,
        name: str,
        secret: bytes,
    ) -> ChannelRecord:
        assert channel_index == 1
        assert len(secret) == 16 and any(secret)
        record = ChannelRecord(index=1, name=name, configured=True, channel_hash="42")
        self.channel_records[1] = record
        self.channel_mutations.append(("set", channel_index, name))
        return record

    async def rename_channel(self, channel_index: int, name: str) -> ChannelRecord:
        assert channel_index == 1
        record = self.channel_records[1].model_copy(update={"name": name})
        self.channel_records[1] = record
        self.channel_mutations.append(("rename", channel_index, name))
        return record

    async def clear_channel(self, channel_index: int) -> ChannelRecord:
        assert channel_index == 1
        record = ChannelRecord(index=1, name="", configured=False, channel_hash="37")
        self.channel_records[1] = record
        self.channel_mutations.append(("clear", channel_index, ""))
        return record

    async def send_direct(self, public_key: str, text: str, on_transmitted):
        assert public_key == "ab" * 32
        assert text == "one controlled send"
        await on_transmitted("01020304")
        return DirectSendOutcome(
            ack_code="01020304",
            acknowledged=True,
            trip_time_ms=42,
        )

    async def send_channel(self, channel_index: int, text: str, on_transmitted):
        assert channel_index == 3
        assert text == "Fidget: private send"
        await on_transmitted()

    async def request_repeater_status(self, public_key: str) -> RepeaterStatus:
        assert public_key == "ab" * 32
        return RepeaterStatus(
            public_key=public_key,
            name="Test repeater",
            path_length=1,
            battery_mv=4100,
            packets_received=100,
            packets_sent=50,
            uptime_seconds=3600,
            requested_at="2026-09-21T08:00:00Z",
        )


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
async def test_contact_management_uses_radio_manager_and_records_audit_events(
    settings: Settings,
) -> None:
    radio = FakeRadioManager()
    app = create_app(settings, radio_manager=radio)  # type: ignore[arg-type]
    transport = httpx.ASGITransport(app=app)
    key = "cd" * 32
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            added = await client.put(
                f"/v1/contacts/{key}",
                json={"name": "New peer", "node_type": 1, "flags": 0},
            )
            removed = await client.delete(f"/v1/contacts/{key}")
            events = await client.get("/v1/events")

    assert added.status_code == 200
    assert added.json()["name"] == "New peer"
    assert removed.json() == {"public_key": key, "removed": True}
    assert radio.contact_mutations == [("upsert", key), ("remove", key)]
    assert [event["kind"] for event in events.json()] == [
        "contact.upsert.requested",
        "contact.upsert.succeeded",
        "contact.remove.requested",
        "contact.remove.succeeded",
    ]


@pytest.mark.asyncio
async def test_channel_management_never_returns_or_audits_secret(settings: Settings) -> None:
    radio = FakeRadioManager()
    app = create_app(settings, radio_manager=radio)  # type: ignore[arg-type]
    transport = httpx.ASGITransport(app=app)
    secret_hex = "a5" * 16
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            configured = await client.put(
                "/v1/channels/1",
                json={"name": "Private", "secret_hex": secret_hex},
            )
            renamed = await client.patch(
                "/v1/channels/1",
                json={"name": "Renamed"},
            )
            cleared = await client.delete("/v1/channels/1")
            events = await client.get("/v1/events")

    serialized = str(
        {
            "configured": configured.json(),
            "renamed": renamed.json(),
            "cleared": cleared.json(),
            "events": events.json(),
        }
    )
    assert configured.json()["name"] == "Private"
    assert renamed.json()["name"] == "Renamed"
    assert cleared.json()["configured"] is False
    assert secret_hex not in serialized
    assert radio.channel_mutations == [
        ("set", 1, "Private"),
        ("rename", 1, "Renamed"),
        ("clear", 1, ""),
    ]
    assert [event["kind"] for event in events.json()] == [
        "channel.set.requested",
        "channel.set.succeeded",
        "channel.rename.requested",
        "channel.rename.succeeded",
        "channel.clear.requested",
        "channel.clear.succeeded",
    ]


@pytest.mark.asyncio
async def test_management_validation_rejects_public_slot_and_long_names(
    settings: Settings,
) -> None:
    radio = FakeRadioManager()
    app = create_app(settings, radio_manager=radio)  # type: ignore[arg-type]
    transport = httpx.ASGITransport(app=app)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            public = await client.put(
                "/v1/channels/0",
                json={"name": "No", "secret_hex": "a5" * 16},
            )
            long_name = await client.put(
                f"/v1/contacts/{'cd' * 32}",
                json={"name": "🙂" * 9, "node_type": 1},
            )

    assert public.status_code == 422
    assert long_name.status_code == 422
    assert radio.channel_mutations == []
    assert radio.contact_mutations == []


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


@pytest.mark.asyncio
async def test_direct_send_records_acknowledged_delivery(settings: Settings) -> None:
    app = create_app(settings, radio_manager=FakeRadioManager())  # type: ignore[arg-type]
    transport = httpx.ASGITransport(app=app)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.post(
                "/v1/messages/direct",
                json={"public_key": "ab" * 32, "text": "one controlled send"},
            )
            messages = await client.get("/v1/messages")
            events = await client.get("/v1/events")

    assert response.status_code == 200
    assert response.json() == {
        "message_id": 1,
        "delivery_state": "acknowledged",
        "ack_code": "01020304",
    }
    assert messages.json()[0]["delivery_state"] == "acknowledged"
    assert messages.json()[0]["ack_code"] == "01020304"
    assert [event["kind"] for event in events.json()] == [
        "message.queued",
        "message.transmitted",
        "message.acknowledged",
    ]


@pytest.mark.asyncio
async def test_direct_send_rejects_more_than_160_utf8_bytes(settings: Settings) -> None:
    app = create_app(settings, radio_manager=FakeRadioManager())  # type: ignore[arg-type]
    transport = httpx.ASGITransport(app=app)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.post(
                "/v1/messages/direct",
                json={"public_key": "ab" * 32, "text": "🙂" * 41},
            )

    assert response.status_code == 422
    assert "160 UTF-8 bytes" in response.text


@pytest.mark.asyncio
async def test_channel_send_records_transmit_without_ack(settings: Settings) -> None:
    app = create_app(settings, radio_manager=FakeRadioManager())  # type: ignore[arg-type]
    transport = httpx.ASGITransport(app=app)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.post(
                "/v1/messages/channel",
                json={"channel_index": 3, "text": "Fidget: private send"},
            )
            messages = await client.get("/v1/messages")
            events = await client.get("/v1/events")

    assert response.status_code == 200
    assert response.json() == {
        "message_id": 1,
        "delivery_state": "transmitted",
        "ack_code": None,
    }
    assert messages.json()[0]["kind"] == "channel"
    assert messages.json()[0]["channel_index"] == 3
    assert [event["kind"] for event in events.json()] == [
        "message.queued",
        "message.transmitted",
    ]


@pytest.mark.asyncio
async def test_repeater_status_records_one_request_and_success(settings: Settings) -> None:
    app = create_app(settings, radio_manager=FakeRadioManager())  # type: ignore[arg-type]
    transport = httpx.ASGITransport(app=app)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.get(f"/v1/repeaters/{'ab' * 32}/status")
            events = await client.get("/v1/events")

    assert response.status_code == 200
    assert response.json()["name"] == "Test repeater"
    assert response.json()["battery_mv"] == 4100
    assert [event["kind"] for event in events.json()] == [
        "repeater.status.requested",
        "repeater.status.succeeded",
    ]


@pytest.mark.asyncio
async def test_repeater_status_rejects_invalid_public_key_before_radio(
    settings: Settings,
) -> None:
    app = create_app(settings, radio_manager=FakeRadioManager())  # type: ignore[arg-type]
    transport = httpx.ASGITransport(app=app)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.get("/v1/repeaters/not-a-key/status")

    assert response.status_code == 422


@pytest.mark.asyncio
async def test_repeater_status_records_timeout_without_retry(settings: Settings) -> None:
    radio = FakeRadioManager()
    calls = 0

    async def timed_out(_public_key: str) -> RepeaterStatus:
        nonlocal calls
        calls += 1
        raise TimeoutError("repeater did not return status before the timeout")

    radio.request_repeater_status = timed_out  # type: ignore[method-assign]
    app = create_app(settings, radio_manager=radio)  # type: ignore[arg-type]
    transport = httpx.ASGITransport(app=app)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.get(f"/v1/repeaters/{'ab' * 32}/status")
            events = await client.get("/v1/events")

    assert calls == 1
    assert response.status_code == 504
    assert [event["kind"] for event in events.json()] == [
        "repeater.status.requested",
        "repeater.status.timed_out",
    ]
