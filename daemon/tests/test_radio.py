from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from meshcore.events import Event, EventType

from meshpincer.config import Settings
from meshpincer.discovery import SerialDevice
from meshpincer.models import (
    ContactRouteMode,
    InboundMessage,
    RepeaterConfigSetting,
    RepeaterStatusTransport,
)
from meshpincer.radio import (
    DirectSendOutcome,
    MeshCoreBackend,
    RadioManager,
    RepeaterLoginDenied,
    SendPolicyError,
)
from meshpincer.store import Store


class FakeBackend:
    def __init__(self) -> None:
        self.disconnected = False
        self.receiver = None
        self.contact_path_length = 2
        self.contact_node_type = 1
        self.sent: list[tuple[str, str]] = []
        self.channel_one_configured = False
        self.channel_one_name = ""
        self.channel_sent: list[tuple[int, str]] = []
        self.repeater_status_requests: list[tuple[str, float]] = []
        self.repeater_acl_requests: list[tuple[str, float]] = []
        self.repeater_login_requests: list[tuple[str, str, float]] = []
        self.repeater_commands: list[tuple[str, str, float]] = []
        self.repeater_acl = [{"key": "12" * 6, "perm": 2}]
        self.repeater_advert_interval = 0
        self.contact_records: dict[str, dict[str, Any]] = {
            "34" * 32: {
                "public_key": "34" * 32,
                "adv_name": "Peer",
                "type": self.contact_node_type,
                "flags": 0,
                "out_path_len": self.contact_path_length,
                "last_advert": 100,
                "adv_lat": 47.1,
                "adv_lon": -122.1,
            }
        }
        self.contact_mutations: list[tuple[str, str]] = []
        self.contact_route_mutations: list[tuple[str, ContactRouteMode]] = []
        self.channel_mutations: list[tuple[str, int, str]] = []
        self.autoadd_config = 0x06
        self.autoadd_max_hops = 4
        self.autoadd_mutations: list[int] = []

    async def read_status(self) -> tuple[dict[str, Any], int]:
        return (
            {
                "self": {
                    "name": "Runtime Node",
                    "public_key": "12" * 32,
                    "radio_freq": 910.525,
                    "radio_bw": 62.5,
                    "radio_sf": 7,
                    "radio_cr": 5,
                    "tx_power": 22,
                },
                "device": {
                    "model": "Fake companion",
                    "ver": "v-test",
                    "fw ver": 13,
                },
                "battery": {"level": 4100, "used_kb": 12, "total_kb": 2048},
                "core": {"uptime_secs": 42, "errors": 0, "queue_len": 0},
                "radio": {"noise_floor": -120, "last_rssi": -80, "last_snr": 4.5},
                "packets": {"recv": 3, "sent": 2, "recv_errors": 0},
                "telemetry": {
                    "lpp": [
                        {"type": "voltage", "value": 4.1},
                        {"type": "temperature", "value": 21.5},
                        {
                            "type": "gps",
                            "value": {"latitude": 47.0, "longitude": -122.0, "altitude": 100},
                        },
                    ]
                },
            },
            2,
        )

    async def read_contacts(self) -> list[dict[str, Any]]:
        peer = self.contact_records.get("34" * 32)
        if peer is not None:
            peer["type"] = self.contact_node_type
            peer["out_path_len"] = self.contact_path_length
        return [dict(contact) for contact in self.contact_records.values()]

    async def read_channels(self, count: int) -> list[dict[str, Any]]:
        assert count == 2
        return [
            {
                "channel_idx": 0,
                "channel_name": "Public",
                "channel_hash": "11",
                "configured": True,
                "channel_secret": b"must not escape",
            },
            {
                "channel_idx": 1,
                "channel_name": self.channel_one_name,
                "channel_hash": "37",
                "configured": self.channel_one_configured,
            },
        ]

    async def read_autoadd_config(self) -> dict[str, int]:
        return {"config": self.autoadd_config, "max_hops": self.autoadd_max_hops}

    async def set_autoadd_config(self, config: int) -> None:
        self.autoadd_config = config
        self.autoadd_mutations.append(config)

    async def upsert_contact(
        self,
        public_key: str,
        name: str,
        node_type: int,
        flags: int,
    ) -> None:
        current = self.contact_records.get(public_key, {})
        self.contact_records[public_key] = {
            "public_key": public_key,
            "adv_name": name,
            "type": node_type,
            "flags": flags,
            "out_path_len": current.get("out_path_len", -1),
            "last_advert": current.get("last_advert", 0),
            "adv_lat": current.get("adv_lat", 0.0),
            "adv_lon": current.get("adv_lon", 0.0),
        }
        self.contact_mutations.append(("upsert", public_key))

    async def remove_contact(self, public_key: str) -> None:
        self.contact_records.pop(public_key, None)
        self.contact_mutations.append(("remove", public_key))

    async def set_contact_route(
        self,
        public_key: str,
        mode: ContactRouteMode,
    ) -> None:
        if public_key not in self.contact_records:
            raise ValueError("contact is not known to the companion radio")
        self.contact_path_length = 0 if mode is ContactRouteMode.ZERO_HOP else -1
        self.contact_records[public_key]["out_path_len"] = self.contact_path_length
        self.contact_route_mutations.append((public_key, mode))

    async def set_channel(self, channel_index: int, name: str, secret: bytes) -> None:
        assert channel_index == 1
        self.channel_one_configured = any(secret)
        self.channel_one_name = name
        self.channel_mutations.append(("set", channel_index, name))

    async def rename_channel(self, channel_index: int, name: str) -> None:
        assert self.channel_one_configured
        self.channel_one_name = name
        self.channel_mutations.append(("rename", channel_index, name))

    async def clear_channel(self, channel_index: int) -> None:
        self.channel_one_configured = False
        self.channel_one_name = ""
        self.channel_mutations.append(("clear", channel_index, ""))

    async def start_receiving(self, handler) -> None:
        self.receiver = handler

    async def emit(self, message: InboundMessage) -> None:
        if self.receiver is None:
            raise AssertionError("receive handler was not installed")
        await self.receiver(message)

    async def send_direct(self, public_key: str, text: str, on_transmitted):
        self.sent.append((public_key, text))
        await on_transmitted("01020304")
        return DirectSendOutcome(
            ack_code="01020304",
            acknowledged=True,
            trip_time_ms=42,
        )

    async def send_channel(self, channel_index: int, text: str, on_transmitted):
        self.channel_sent.append((channel_index, text))
        await on_transmitted()

    async def request_repeater_status(
        self,
        public_key: str,
        timeout: float,
        transport: RepeaterStatusTransport = RepeaterStatusTransport.BINARY,
    ):
        self.repeater_status_requests.append((public_key, timeout))
        return {
            "bat": 4095,
            "tx_queue_len": 1,
            "noise_floor": -118,
            "last_rssi": -79,
            "nb_recv": 100,
            "nb_sent": 50,
            "airtime": 12,
            "uptime": 3600,
            "sent_flood": 2,
            "sent_direct": 48,
            "recv_flood": 3,
            "recv_direct": 97,
            "full_evts": 0,
            "last_snr": 5.25,
            "direct_dups": 1,
            "flood_dups": 2,
            "rx_airtime": 20,
            "recv_errors": 0,
        }

    async def login_repeater(self, public_key: str, password: str, timeout: float):
        self.repeater_login_requests.append((public_key, password, timeout))
        return {
            "permissions": 0,
            "is_admin": False,
            "_login_succeeded": True,
        }

    async def request_repeater_acl(self, public_key: str, timeout: float):
        self.repeater_acl_requests.append((public_key, timeout))
        return list(self.repeater_acl)

    async def run_repeater_command(self, public_key: str, command: str, timeout: float):
        self.repeater_commands.append((public_key, command, timeout))
        if command.startswith("setperm "):
            _, key, raw_permissions = command.split()
            prefix = key[:12]
            permissions = int(raw_permissions)
            self.repeater_acl = [item for item in self.repeater_acl if item["key"] != prefix]
            if permissions:
                self.repeater_acl.append({"key": prefix, "perm": permissions})
            return "OK"
        if command == "get advert.interval":
            return f"> {self.repeater_advert_interval}"
        if command.startswith("set advert.interval "):
            self.repeater_advert_interval = int(command.rsplit(" ", 1)[1])
            return "OK"
        return "Err - unsupported"

    async def disconnect(self) -> None:
        self.disconnected = True


async def wait_until_connected(manager: RadioManager) -> None:
    for _ in range(100):
        if (await manager.status()).connected:
            return
        await asyncio.sleep(0.001)
    raise AssertionError("radio manager did not connect")


async def wait_until_receiving(backend: FakeBackend) -> None:
    for _ in range(100):
        if backend.receiver is not None:
            return
        await asyncio.sleep(0.001)
    raise AssertionError("radio manager did not install its receive handler")


async def test_manager_discovers_refreshes_and_sanitizes(tmp_path: Path) -> None:
    config = Settings(
        state_dir=tmp_path,
        socket_path=tmp_path / "meshpincer.sock",
        refresh_interval_seconds=60,
    )
    backend = FakeBackend()
    discovered = SerialDevice(
        port="/dev/cu.dynamic",
        serial_number="private-hardware-id",
        product="Fake companion",
    )
    connector_calls: list[tuple[str, float]] = []

    async def connector(port: str, timeout: float) -> FakeBackend:
        connector_calls.append((port, timeout))
        return backend

    manager = RadioManager(config, discoverer=lambda _settings: discovered, connector=connector)
    await manager.start()
    try:
        await wait_until_connected(manager)
        status = await manager.status()
        contacts = await manager.contacts()
        channels = await manager.channels()
        all_channels = await manager.channels(include_empty=True)
    finally:
        await manager.stop()

    assert connector_calls == [("/dev/cu.dynamic", 5.0)]
    assert status.node_name == "Runtime Node"
    assert status.radio is not None
    assert status.radio.frequency_mhz == 910.525
    assert status.health is not None
    assert status.health.device_errors == 0
    assert status.telemetry is not None
    assert status.telemetry.latitude == 47.0
    assert contacts[0].name == "Peer"
    assert contacts[0].model_dump().keys().isdisjoint({"adv_lat", "adv_lon"})
    assert [channel.name for channel in channels] == ["Public"]
    assert len(all_channels) == 2
    assert "channel_secret" not in repr(all_channels)
    assert "private-hardware-id" not in status.model_dump_json()
    assert backend.disconnected


async def test_manager_mutates_contacts_without_reconnecting(tmp_path: Path) -> None:
    config = Settings(
        state_dir=tmp_path,
        socket_path=tmp_path / "meshpincer.sock",
        refresh_interval_seconds=60,
    )
    backend = FakeBackend()

    async def connector(_port: str, _timeout: float) -> FakeBackend:
        return backend

    manager = RadioManager(
        config,
        discoverer=lambda _settings: SerialDevice(port="/dev/cu.dynamic"),
        connector=connector,
    )
    await manager.start()
    try:
        await wait_until_connected(manager)
        radio_task = manager._task
        added = await manager.upsert_contact("56" * 32, "New peer", 1, 0)
        contacts_after_add = await manager.contacts()
        removed_key = await manager.remove_contact("56" * 32)
        contacts_after_remove = await manager.contacts()

        assert manager._task is radio_task
        assert not backend.disconnected
    finally:
        await manager.stop()

    assert added.name == "New peer"
    assert added.path_length == -1
    assert any(item.public_key == "56" * 32 for item in contacts_after_add)
    assert removed_key == "56" * 32
    assert all(item.public_key != "56" * 32 for item in contacts_after_remove)
    assert backend.contact_mutations == [
        ("upsert", "56" * 32),
        ("remove", "56" * 32),
    ]


async def test_manager_updates_contact_route_without_reconnecting(tmp_path: Path) -> None:
    config = Settings(
        state_dir=tmp_path,
        socket_path=tmp_path / "meshpincer.sock",
        refresh_interval_seconds=60,
    )
    backend = FakeBackend()

    async def connector(_port: str, _timeout: float) -> FakeBackend:
        return backend

    manager = RadioManager(
        config,
        discoverer=lambda _settings: SerialDevice(port="/dev/cu.dynamic"),
        connector=connector,
    )
    await manager.start()
    try:
        await wait_until_connected(manager)
        radio_task = manager._task
        direct = await manager.set_contact_route("34" * 32, ContactRouteMode.ZERO_HOP)
        flood = await manager.set_contact_route("34" * 32, ContactRouteMode.FLOOD)

        assert manager._task is radio_task
        assert not backend.disconnected
    finally:
        await manager.stop()

    assert direct.path_length == 0
    assert flood.path_length == -1
    assert backend.contact_route_mutations == [
        ("34" * 32, ContactRouteMode.ZERO_HOP),
        ("34" * 32, ContactRouteMode.FLOOD),
    ]


async def test_manager_mutates_private_channel_without_reconnecting(tmp_path: Path) -> None:
    config = Settings(
        state_dir=tmp_path,
        socket_path=tmp_path / "meshpincer.sock",
        refresh_interval_seconds=60,
    )
    backend = FakeBackend()

    async def connector(_port: str, _timeout: float) -> FakeBackend:
        return backend

    manager = RadioManager(
        config,
        discoverer=lambda _settings: SerialDevice(port="/dev/cu.dynamic"),
        connector=connector,
    )
    await manager.start()
    try:
        await wait_until_connected(manager)
        radio_task = manager._task
        configured = await manager.set_channel(1, "Private", bytes.fromhex("ab" * 16))
        renamed = await manager.rename_channel(1, "Renamed")
        cleared = await manager.clear_channel(1)

        assert manager._task is radio_task
        assert not backend.disconnected
    finally:
        await manager.stop()

    assert (configured.name, configured.configured) == ("Private", True)
    assert (renamed.name, renamed.configured) == ("Renamed", True)
    assert (cleared.name, cleared.configured) == ("", False)
    assert backend.channel_mutations == [
        ("set", 1, "Private"),
        ("rename", 1, "Renamed"),
        ("clear", 1, ""),
    ]


async def test_manager_enables_oldest_non_favorite_overwrite_without_reconnecting(
    tmp_path: Path,
) -> None:
    config = Settings(
        state_dir=tmp_path,
        socket_path=tmp_path / "meshpincer.sock",
        refresh_interval_seconds=60,
    )
    backend = FakeBackend()

    async def connector(_port: str, _timeout: float) -> FakeBackend:
        return backend

    manager = RadioManager(
        config,
        discoverer=lambda _settings: SerialDevice(port="/dev/cu.dynamic"),
        connector=connector,
    )
    await manager.start()
    try:
        await wait_until_connected(manager)
        radio_task = manager._task
        before = await manager.autoadd_config()
        updated = await manager.set_overwrite_oldest_non_favorite(True)

        assert manager._task is radio_task
        assert not backend.disconnected
    finally:
        await manager.stop()

    assert before.config == 0x06
    assert before.overwrite_oldest_non_favorite is False
    assert updated.config == 0x07
    assert updated.max_hops == 4
    assert updated.overwrite_oldest_non_favorite is True
    assert backend.autoadd_mutations == [0x07]


async def test_manager_rejects_public_and_invalid_channel_before_mutation(
    tmp_path: Path,
) -> None:
    config = Settings(
        state_dir=tmp_path,
        socket_path=tmp_path / "meshpincer.sock",
        refresh_interval_seconds=60,
    )
    backend = FakeBackend()

    async def connector(_port: str, _timeout: float) -> FakeBackend:
        return backend

    manager = RadioManager(
        config,
        discoverer=lambda _settings: SerialDevice(port="/dev/cu.dynamic"),
        connector=connector,
    )
    await manager.start()
    try:
        await wait_until_connected(manager)
        with pytest.raises(SendPolicyError, match="Public"):
            await manager.set_channel(0, "Public replacement", bytes.fromhex("ab" * 16))
        with pytest.raises(ValueError, match="slot range"):
            await manager.set_channel(2, "Out of range", bytes.fromhex("ab" * 16))
    finally:
        await manager.stop()

    assert backend.channel_mutations == []


async def test_manager_retries_after_connection_failure(tmp_path: Path) -> None:
    config = Settings(
        state_dir=tmp_path,
        socket_path=tmp_path / "meshpincer.sock",
        refresh_interval_seconds=60,
        reconnect_initial_seconds=0.001,
        reconnect_max_seconds=0.002,
    )
    backend = FakeBackend()
    attempts = 0

    async def connector(_port: str, _timeout: float) -> FakeBackend:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise ConnectionError("temporary failure")
        return backend

    manager = RadioManager(
        config,
        discoverer=lambda _settings: SerialDevice(port="/dev/cu.dynamic"),
        connector=connector,
    )
    await manager.start()
    try:
        await wait_until_connected(manager)
        status = await manager.status()
    finally:
        await manager.stop()

    assert attempts == 2
    assert status.connected
    assert status.last_error is None


async def test_manager_persists_received_messages(tmp_path: Path) -> None:
    config = Settings(
        state_dir=tmp_path,
        socket_path=tmp_path / "meshpincer.sock",
        refresh_interval_seconds=60,
    )
    store = Store(config.database_path)
    await store.initialize()
    backend = FakeBackend()

    async def connector(_port: str, _timeout: float) -> FakeBackend:
        return backend

    manager = RadioManager(
        config,
        discoverer=lambda _settings: SerialDevice(port="/dev/cu.dynamic"),
        connector=connector,
        message_handler=store.record_inbound,
    )
    await manager.start()
    try:
        await wait_until_receiving(backend)
        inbound = InboundMessage(
            kind="channel",
            channel_index=0,
            text="Agent: hello",
            mesh_timestamp=10,
            snr=2.5,
            path_length=1,
        )
        await backend.emit(inbound)
        await backend.emit(inbound)
    finally:
        await manager.stop()

    messages = await store.list_messages(after_id=0, limit=100)
    events = await store.list_events(after_id=0, limit=100)
    assert len(messages) == 1
    assert messages[0].text == "Agent: hello"
    assert len(events) == 1


async def test_meshcore_backend_normalizes_direct_and_channel_events() -> None:
    class FakeClient:
        def __init__(self) -> None:
            self.callbacks = {}
            self.fetching = False

        def subscribe(self, event_type, callback):
            self.callbacks[event_type] = callback
            return event_type

        def get_contact_by_key_prefix(self, prefix: str):
            return {"public_key": "ab" * 32} if prefix == "ab" * 6 else None

        async def start_auto_message_fetching(self) -> None:
            self.fetching = True

    client = FakeClient()
    backend = MeshCoreBackend(client, timeout=5.0)  # type: ignore[arg-type]
    messages: list[InboundMessage] = []

    async def handle(message: InboundMessage) -> None:
        messages.append(message)

    await backend.start_receiving(handle)
    await client.callbacks[EventType.CONTACT_MSG_RECV](
        SimpleNamespace(
            payload={
                "pubkey_prefix": "ab" * 6,
                "text": "private hello",
                "sender_timestamp": 100,
                "SNR": 4.25,
                "path_len": 2,
                "txt_type": 0,
            }
        )
    )
    await client.callbacks[EventType.CHANNEL_MSG_RECV](
        SimpleNamespace(
            payload={
                "channel_idx": 3,
                "text": "Untrusted label: hello",
                "sender_timestamp": 101,
                "SNR": 2.0,
                "path_len": 1,
                "txt_type": 0,
            }
        )
    )

    assert client.fetching
    assert messages == [
        InboundMessage(
            kind="direct",
            peer_key="ab" * 32,
            peer_key_prefix="ab" * 6,
            text="private hello",
            mesh_timestamp=100,
            snr=4.25,
            path_length=2,
            text_type=0,
        ),
        InboundMessage(
            kind="channel",
            channel_index=3,
            text="Untrusted label: hello",
            mesh_timestamp=101,
            snr=2.0,
            path_length=1,
            text_type=0,
        ),
    ]


async def test_manager_allows_one_learned_multihop_direct_send(tmp_path: Path) -> None:
    config = Settings(
        state_dir=tmp_path,
        socket_path=tmp_path / "meshpincer.sock",
        refresh_interval_seconds=60,
    )
    backend = FakeBackend()
    backend.contact_path_length = 12

    async def connector(_port: str, _timeout: float) -> FakeBackend:
        return backend

    manager = RadioManager(
        config,
        discoverer=lambda _settings: SerialDevice(port="/dev/cu.dynamic"),
        connector=connector,
    )
    await manager.start()
    states: list[str] = []

    async def on_transmitted(ack_code: str) -> None:
        states.append(ack_code)

    try:
        await wait_until_connected(manager)
        outcome = await manager.send_direct(
            "34" * 32,
            "one controlled send",
            on_transmitted,
        )
    finally:
        await manager.stop()

    assert backend.sent == [("34" * 32, "one controlled send")]
    assert states == ["01020304"]
    assert outcome.acknowledged


async def test_manager_rejects_unknown_route_before_transmit(tmp_path: Path) -> None:
    config = Settings(
        state_dir=tmp_path,
        socket_path=tmp_path / "meshpincer.sock",
        refresh_interval_seconds=60,
    )
    backend = FakeBackend()
    backend.contact_path_length = -1

    async def connector(_port: str, _timeout: float) -> FakeBackend:
        return backend

    manager = RadioManager(
        config,
        discoverer=lambda _settings: SerialDevice(port="/dev/cu.dynamic"),
        connector=connector,
    )
    await manager.start()
    try:
        await wait_until_connected(manager)
        with pytest.raises(ValueError, match="learned route"):
            await manager.send_direct(
                "34" * 32,
                "must not transmit",
                lambda _ack: asyncio.sleep(0),
            )
    finally:
        await manager.stop()

    assert backend.sent == []


async def test_manager_rate_limits_repeated_direct_send(tmp_path: Path) -> None:
    config = Settings(
        state_dir=tmp_path,
        socket_path=tmp_path / "meshpincer.sock",
        refresh_interval_seconds=60,
        direct_global_cooldown_seconds=5,
        direct_peer_cooldown_seconds=30,
    )
    backend = FakeBackend()
    backend.contact_path_length = 0

    async def connector(_port: str, _timeout: float) -> FakeBackend:
        return backend

    manager = RadioManager(
        config,
        discoverer=lambda _settings: SerialDevice(port="/dev/cu.dynamic"),
        connector=connector,
    )

    async def on_transmitted(_ack_code: str) -> None:
        pass

    await manager.start()
    try:
        await wait_until_connected(manager)
        await manager.send_direct("34" * 32, "first", on_transmitted)
        with pytest.raises(SendPolicyError, match="cooldown"):
            await manager.send_direct("34" * 32, "second", on_transmitted)
    finally:
        await manager.stop()

    assert backend.sent == [("34" * 32, "first")]


async def test_manager_allows_one_allowlisted_private_channel_send(tmp_path: Path) -> None:
    config = Settings(
        state_dir=tmp_path,
        socket_path=tmp_path / "meshpincer.sock",
        refresh_interval_seconds=60,
        channel_send_allowlist=frozenset({1}),
    )
    backend = FakeBackend()
    backend.channel_one_configured = True

    async def connector(_port: str, _timeout: float) -> FakeBackend:
        return backend

    manager = RadioManager(
        config,
        discoverer=lambda _settings: SerialDevice(port="/dev/cu.dynamic"),
        connector=connector,
    )
    transmitted = False

    async def on_transmitted() -> None:
        nonlocal transmitted
        transmitted = True

    await manager.start()
    try:
        await wait_until_connected(manager)
        await manager.send_channel(1, "Fidget: private test", on_transmitted)
    finally:
        await manager.stop()

    assert transmitted
    assert backend.channel_sent == [(1, "Fidget: private test")]


async def test_manager_rejects_public_and_unallowlisted_channel_sends(tmp_path: Path) -> None:
    config = Settings(
        state_dir=tmp_path,
        socket_path=tmp_path / "meshpincer.sock",
        refresh_interval_seconds=60,
        channel_send_allowlist=frozenset({0}),
    )
    backend = FakeBackend()

    async def connector(_port: str, _timeout: float) -> FakeBackend:
        return backend

    manager = RadioManager(
        config,
        discoverer=lambda _settings: SerialDevice(port="/dev/cu.dynamic"),
        connector=connector,
    )
    await manager.start()
    try:
        await wait_until_connected(manager)
        with pytest.raises(SendPolicyError, match="Public"):
            await manager.send_channel(0, "must not transmit", lambda: asyncio.sleep(0))
        with pytest.raises(SendPolicyError, match="allowlisted"):
            await manager.send_channel(1, "must not transmit", lambda: asyncio.sleep(0))
    finally:
        await manager.stop()

    assert backend.channel_sent == []


async def test_manager_rate_limits_repeated_private_channel_send(tmp_path: Path) -> None:
    config = Settings(
        state_dir=tmp_path,
        socket_path=tmp_path / "meshpincer.sock",
        refresh_interval_seconds=60,
        channel_send_allowlist=frozenset({1}),
        channel_global_cooldown_seconds=30,
        channel_per_channel_cooldown_seconds=300,
    )
    backend = FakeBackend()
    backend.channel_one_configured = True

    async def connector(_port: str, _timeout: float) -> FakeBackend:
        return backend

    manager = RadioManager(
        config,
        discoverer=lambda _settings: SerialDevice(port="/dev/cu.dynamic"),
        connector=connector,
    )
    await manager.start()
    try:
        await wait_until_connected(manager)
        await manager.send_channel(1, "first", lambda: asyncio.sleep(0))
        with pytest.raises(SendPolicyError, match="cooldown"):
            await manager.send_channel(1, "second", lambda: asyncio.sleep(0))
    finally:
        await manager.stop()

    assert backend.channel_sent == [(1, "first")]


async def test_manager_requests_one_known_repeater_status_and_rate_limits(tmp_path: Path) -> None:
    config = Settings(
        state_dir=tmp_path,
        socket_path=tmp_path / "meshpincer.sock",
        refresh_interval_seconds=60,
        repeater_status_timeout_seconds=7,
        repeater_status_global_cooldown_seconds=30,
        repeater_status_peer_cooldown_seconds=60,
    )
    backend = FakeBackend()
    backend.contact_node_type = 2

    async def connector(_port: str, _timeout: float) -> FakeBackend:
        return backend

    manager = RadioManager(
        config,
        discoverer=lambda _settings: SerialDevice(port="/dev/cu.dynamic"),
        connector=connector,
    )
    await manager.start()
    try:
        await wait_until_connected(manager)
        status = await manager.request_repeater_status("34" * 32)
        with pytest.raises(SendPolicyError, match="cooldown"):
            await manager.request_repeater_status("34" * 32)
        manager._last_repeater_status -= 60
        manager._last_repeater_status_by_peer["34" * 32] -= 60
        second_status = await manager.request_repeater_status("34" * 32)
    finally:
        await manager.stop()

    assert backend.repeater_status_requests == [("34" * 32, 7), ("34" * 32, 7)]
    assert status.name == "Peer"
    assert second_status.name == "Peer"
    assert status.battery_mv == 4095
    assert status.last_snr_db == 5.25
    assert status.receive_errors == 0


async def test_manager_rejects_non_repeater_status_target_without_transmit(
    tmp_path: Path,
) -> None:
    config = Settings(
        state_dir=tmp_path,
        socket_path=tmp_path / "meshpincer.sock",
        refresh_interval_seconds=60,
    )
    backend = FakeBackend()

    async def connector(_port: str, _timeout: float) -> FakeBackend:
        return backend

    manager = RadioManager(
        config,
        discoverer=lambda _settings: SerialDevice(port="/dev/cu.dynamic"),
        connector=connector,
    )
    await manager.start()
    try:
        await wait_until_connected(manager)
        with pytest.raises(ValueError, match="not a repeater"):
            await manager.request_repeater_status("34" * 32)
    finally:
        await manager.stop()

    assert backend.repeater_status_requests == []


async def test_manager_requests_repeater_acl_once_and_rate_limits(tmp_path: Path) -> None:
    config = Settings(
        state_dir=tmp_path,
        socket_path=tmp_path / "meshpincer.sock",
        refresh_interval_seconds=60,
        repeater_acl_timeout_seconds=7,
    )
    backend = FakeBackend()
    backend.contact_node_type = 2

    async def connector(_port: str, _timeout: float) -> FakeBackend:
        return backend

    manager = RadioManager(
        config,
        discoverer=lambda _settings: SerialDevice(port="/dev/cu.dynamic"),
        connector=connector,
    )
    await manager.start()
    try:
        await wait_until_connected(manager)
        acl = await manager.request_repeater_acl("34" * 32)
        with pytest.raises(SendPolicyError, match="cooldown"):
            await manager.request_repeater_acl("34" * 32)
    finally:
        await manager.stop()

    assert backend.repeater_acl_requests == [("34" * 32, 7)]
    assert acl.entries[0].public_key_prefix == "12" * 6
    assert acl.entries[0].permissions == 2


async def test_meshcore_backend_requests_repeater_acl_once() -> None:
    class FakeCommands:
        def __init__(self) -> None:
            self.calls = []

        async def req_acl_sync(self, public_key: str, timeout: float):
            self.calls.append((public_key, timeout))
            return [{"key": "ab" * 6, "perm": 3}]

    commands = FakeCommands()
    backend = MeshCoreBackend(SimpleNamespace(commands=commands), timeout=5)
    result = await backend.request_repeater_acl("cd" * 32, timeout=11)
    assert commands.calls == [("cd" * 32, 11)]
    assert result == [{"key": "ab" * 6, "perm": 3}]


async def test_meshcore_backend_correlates_tagged_repeater_command_reply() -> None:
    callbacks = []

    class Subscription:
        def unsubscribe(self) -> None:
            callbacks.clear()

    class FakeCommands:
        async def send_cmd(self, public_key: str, command: str):
            assert public_key == "cd" * 32
            tag = command[:2]
            for callback in list(callbacks):
                callback(
                    Event(
                        EventType.CONTACT_MSG_RECV,
                        {
                            "pubkey_prefix": "cd" * 6,
                            "txt_type": 1,
                            "text": f"{tag}|OK",
                        },
                    )
                )
            return Event(EventType.MSG_SENT, {"expected_ack": b"1234"})

    class FakeClient:
        commands = FakeCommands()

        def subscribe(self, event_type, callback):
            assert event_type is EventType.CONTACT_MSG_RECV
            callbacks.append(callback)
            return Subscription()

    backend = MeshCoreBackend(FakeClient(), timeout=5)  # type: ignore[arg-type]
    assert await backend.run_repeater_command("cd" * 32, "get advert.interval", 1) == "OK"
    assert callbacks == []


async def test_manager_updates_acl_with_pre_and_post_readback(tmp_path: Path) -> None:
    config = Settings(
        state_dir=tmp_path,
        socket_path=tmp_path / "meshpincer.sock",
        refresh_interval_seconds=60,
        repeater_mutation_global_cooldown_seconds=0,
        repeater_mutation_peer_cooldown_seconds=0,
    )
    backend = FakeBackend()
    backend.contact_node_type = 2

    async def connector(_port: str, _timeout: float) -> FakeBackend:
        return backend

    manager = RadioManager(
        config,
        discoverer=lambda _settings: SerialDevice(port="/dev/cu.dynamic"),
        connector=connector,
    )
    await manager.start()
    try:
        await wait_until_connected(manager)
        result = await manager.set_repeater_acl_permission("34" * 32, "56" * 32, 1)
    finally:
        await manager.stop()

    assert result.previous_permissions is None
    assert result.permissions == 1
    assert result.verified is True
    assert backend.repeater_commands == [("34" * 32, f"setperm {'56' * 32} 1", 15.0)]
    assert backend.repeater_acl_requests == [("34" * 32, 15.0), ("34" * 32, 15.0)]


async def test_manager_refuses_to_mutate_own_acl_entry(tmp_path: Path) -> None:
    config = Settings(
        state_dir=tmp_path,
        socket_path=tmp_path / "meshpincer.sock",
        refresh_interval_seconds=60,
        repeater_mutation_global_cooldown_seconds=0,
        repeater_mutation_peer_cooldown_seconds=0,
    )
    backend = FakeBackend()
    backend.contact_node_type = 2

    async def connector(_port: str, _timeout: float) -> FakeBackend:
        return backend

    manager = RadioManager(
        config,
        discoverer=lambda _settings: SerialDevice(port="/dev/cu.dynamic"),
        connector=connector,
    )
    await manager.start()
    try:
        await wait_until_connected(manager)
        with pytest.raises(ValueError, match="own ACL"):
            await manager.set_repeater_acl_permission("34" * 32, "12" * 32, 1)
    finally:
        await manager.stop()

    assert backend.repeater_commands == []


async def test_manager_configures_repeater_with_read_change_readback(tmp_path: Path) -> None:
    config = Settings(
        state_dir=tmp_path,
        socket_path=tmp_path / "meshpincer.sock",
        refresh_interval_seconds=60,
        repeater_mutation_global_cooldown_seconds=0,
        repeater_mutation_peer_cooldown_seconds=0,
    )
    backend = FakeBackend()
    backend.contact_node_type = 2

    async def connector(_port: str, _timeout: float) -> FakeBackend:
        return backend

    manager = RadioManager(
        config,
        discoverer=lambda _settings: SerialDevice(port="/dev/cu.dynamic"),
        connector=connector,
    )
    await manager.start()
    try:
        await wait_until_connected(manager)
        result = await manager.configure_repeater(
            "34" * 32, RepeaterConfigSetting.LOCAL_ADVERT_INTERVAL_MINUTES, 60
        )
    finally:
        await manager.stop()

    assert result.previous_value == 0
    assert result.value == 60
    assert result.verified is True
    assert [command for _, command, _ in backend.repeater_commands] == [
        "get advert.interval",
        "set advert.interval 60",
        "get advert.interval",
    ]


async def test_manager_reads_repeater_config_without_mutation(tmp_path: Path) -> None:
    config = Settings(
        state_dir=tmp_path,
        socket_path=tmp_path / "meshpincer.sock",
        refresh_interval_seconds=60,
        repeater_config_read_global_cooldown_seconds=0,
        repeater_config_read_peer_cooldown_seconds=0,
    )
    backend = FakeBackend()
    backend.contact_node_type = 2
    backend.repeater_advert_interval = 120

    async def connector(_port: str, _timeout: float) -> FakeBackend:
        return backend

    manager = RadioManager(
        config,
        discoverer=lambda _settings: SerialDevice(port="/dev/cu.dynamic"),
        connector=connector,
    )
    await manager.start()
    try:
        await wait_until_connected(manager)
        result = await manager.read_repeater_config(
            "34" * 32, RepeaterConfigSetting.LOCAL_ADVERT_INTERVAL_MINUTES
        )
    finally:
        await manager.stop()

    assert result.value == 120
    assert backend.repeater_advert_interval == 120
    assert [command for _, command, _ in backend.repeater_commands] == ["get advert.interval"]


async def test_manager_logs_into_one_known_repeater_and_rate_limits(tmp_path: Path) -> None:
    config = Settings(
        state_dir=tmp_path,
        socket_path=tmp_path / "meshpincer.sock",
        refresh_interval_seconds=60,
        repeater_login_timeout_seconds=7,
        repeater_login_global_cooldown_seconds=30,
        repeater_login_peer_cooldown_seconds=60,
    )
    backend = FakeBackend()
    backend.contact_node_type = 2

    async def connector(_port: str, _timeout: float) -> FakeBackend:
        return backend

    manager = RadioManager(
        config,
        discoverer=lambda _settings: SerialDevice(port="/dev/cu.dynamic"),
        connector=connector,
    )
    await manager.start()
    try:
        await wait_until_connected(manager)
        result = await manager.login_repeater("34" * 32, "guest-pass")
        with pytest.raises(SendPolicyError, match="cooldown"):
            await manager.login_repeater("34" * 32, "guest-pass")
    finally:
        await manager.stop()

    assert backend.repeater_login_requests == [("34" * 32, "guest-pass", 7)]
    assert result.name == "Peer"
    assert result.is_admin is False


async def test_manager_reports_repeater_login_denial(tmp_path: Path) -> None:
    config = Settings(
        state_dir=tmp_path,
        socket_path=tmp_path / "meshpincer.sock",
        refresh_interval_seconds=60,
    )
    backend = FakeBackend()
    backend.contact_node_type = 2

    async def denied(_public_key: str, _password: str, _timeout: float):
        return {"_login_succeeded": False}

    backend.login_repeater = denied  # type: ignore[method-assign]

    async def connector(_port: str, _timeout: float) -> FakeBackend:
        return backend

    manager = RadioManager(
        config,
        discoverer=lambda _settings: SerialDevice(port="/dev/cu.dynamic"),
        connector=connector,
    )
    await manager.start()
    try:
        await wait_until_connected(manager)
        with pytest.raises(RepeaterLoginDenied, match="rejected"):
            await manager.login_repeater("34" * 32, "wrong")
    finally:
        await manager.stop()


async def test_meshcore_backend_correlates_early_ack() -> None:
    expected_ack = bytes.fromhex("01020304")

    class FakeCommands:
        def __init__(self, client) -> None:
            self.client = client

        async def send_msg(self, public_key: str, text: str, attempt: int):
            assert public_key == "ab" * 32
            assert text == "one controlled send"
            assert attempt == 0
            self.client.ack_callback(
                Event(
                    EventType.ACK,
                    {"code": "01020304", "trip_time": 42},
                    {"code": "01020304"},
                )
            )
            return Event(
                EventType.MSG_SENT,
                {
                    "type": 0,
                    "expected_ack": expected_ack,
                    "suggested_timeout": 100,
                },
            )

    class FakeClient:
        def __init__(self) -> None:
            self.ack_callback = None
            self.commands = FakeCommands(self)

        def subscribe(self, event_type, callback):
            assert event_type == EventType.ACK
            self.ack_callback = callback
            return SimpleNamespace(unsubscribe=lambda: None)

    backend = MeshCoreBackend(FakeClient(), timeout=5.0)  # type: ignore[arg-type]
    transmitted: list[str] = []

    async def on_transmitted(ack_code: str) -> None:
        transmitted.append(ack_code)

    outcome = await backend.send_direct(
        "ab" * 32,
        "one controlled send",
        on_transmitted,
    )

    assert transmitted == ["01020304"]
    assert outcome == DirectSendOutcome(
        ack_code="01020304",
        acknowledged=True,
        trip_time_ms=42,
    )


async def test_meshcore_backend_submits_channel_message_without_fake_ack() -> None:
    class FakeCommands:
        async def send_chan_msg(self, channel_index: int, text: str):
            assert channel_index == 3
            assert text == "Fidget: private test"
            return Event(EventType.OK, {})

    backend = MeshCoreBackend(
        SimpleNamespace(commands=FakeCommands()),  # type: ignore[arg-type]
        timeout=5.0,
    )
    transmitted = False

    async def on_transmitted() -> None:
        nonlocal transmitted
        transmitted = True

    await backend.send_channel(3, "Fidget: private test", on_transmitted)

    assert transmitted


async def test_meshcore_backend_upserts_contact_without_losing_route() -> None:
    class FakeCommands:
        def __init__(self) -> None:
            self.contact = None

        async def add_contact(self, contact):
            self.contact = dict(contact)
            return Event(EventType.OK, {})

    commands = FakeCommands()
    key = "ab" * 32
    client = SimpleNamespace(
        commands=commands,
        contacts={
            key: {
                "public_key": key,
                "adv_name": "Old",
                "type": 1,
                "flags": 0,
                "out_path_len": 2,
                "out_path_hash_mode": 0,
                "out_path": "0102",
                "last_advert": 123,
                "adv_lat": 47.0,
                "adv_lon": -122.0,
            }
        },
    )
    backend = MeshCoreBackend(client, timeout=5.0)  # type: ignore[arg-type]

    await backend.upsert_contact(key, "Renamed", 1, 4)

    assert commands.contact is not None
    assert commands.contact["adv_name"] == "Renamed"
    assert commands.contact["flags"] == 4
    assert commands.contact["out_path_len"] == 2
    assert commands.contact["out_path"] == "0102"
    assert commands.contact["last_advert"] == 123


async def test_meshcore_backend_sets_zero_hop_and_resets_flood_route() -> None:
    class FakeCommands:
        def __init__(self) -> None:
            self.changed: tuple[str, int] | None = None
            self.reset: str | None = None

        async def change_contact_path(self, contact, path, path_hash_mode=None):
            assert contact["public_key"] == "ab" * 32
            self.changed = (path, path_hash_mode)
            return Event(EventType.OK, {})

        async def reset_path(self, public_key):
            self.reset = public_key
            return Event(EventType.OK, {})

    commands = FakeCommands()
    key = "ab" * 32
    client = SimpleNamespace(
        commands=commands,
        contacts={
            key: {
                "public_key": key,
                "adv_name": "Repeater",
                "type": 2,
                "flags": 1,
                "out_path_len": -1,
                "out_path_hash_mode": -1,
                "out_path": "",
                "last_advert": 123,
            }
        },
    )
    backend = MeshCoreBackend(client, timeout=5.0)  # type: ignore[arg-type]

    await backend.set_contact_route(key, ContactRouteMode.ZERO_HOP)
    await backend.set_contact_route(key, ContactRouteMode.FLOOD)

    assert commands.changed == ("", None)
    assert commands.reset == key


async def test_meshcore_backend_reads_and_sets_autoadd_config() -> None:
    class FakeCommands:
        def __init__(self) -> None:
            self.updated: int | None = None

        async def get_autoadd_config(self):
            return Event(EventType.AUTOADD_CONFIG, {"config": 0x06, "max_hops": 4})

        async def set_autoadd_config(self, config: int):
            self.updated = config
            return Event(EventType.OK, {})

    commands = FakeCommands()
    backend = MeshCoreBackend(
        SimpleNamespace(commands=commands),  # type: ignore[arg-type]
        timeout=5.0,
    )

    assert await backend.read_autoadd_config() == {"config": 0x06, "max_hops": 4}
    await backend.set_autoadd_config(0x07)
    assert commands.updated == 0x07


async def test_meshcore_backend_renames_channel_with_existing_secret() -> None:
    secret = bytes.fromhex("a5" * 16)

    class FakeCommands:
        def __init__(self) -> None:
            self.set_args = None

        async def get_channel(self, channel_index: int):
            assert channel_index == 1
            return Event(
                EventType.CHANNEL_INFO,
                {"channel_name": "Old", "channel_secret": secret},
            )

        async def set_channel(self, channel_index: int, name: str, actual_secret: bytes):
            self.set_args = (channel_index, name, actual_secret)
            return Event(EventType.OK, {})

    commands = FakeCommands()
    backend = MeshCoreBackend(
        SimpleNamespace(commands=commands),  # type: ignore[arg-type]
        timeout=5.0,
    )

    await backend.rename_channel(1, "Renamed")

    assert commands.set_args == (1, "Renamed", secret)


async def test_meshcore_backend_requests_repeater_status_once() -> None:
    class FakeSubscription:
        def unsubscribe(self) -> None:
            pass

    class FakeCommands:
        def __init__(self) -> None:
            self.calls: list[tuple[str, float]] = []

        async def req_status_sync(self, public_key: str, timeout: float):
            self.calls.append((public_key, timeout))
            return {"bat": 4200, "recv_errors": 0}

    commands = FakeCommands()
    client = SimpleNamespace(
        commands=commands,
        subscribe=lambda *_args: FakeSubscription(),
    )
    backend = MeshCoreBackend(client, timeout=5.0)  # type: ignore[arg-type]

    result = await backend.request_repeater_status("ab" * 32, timeout=11)

    assert commands.calls == [("ab" * 32, 11)]
    assert result == {
        "bat": 4200,
        "recv_errors": 0,
        "_response_correlation": "request_tag",
    }


async def test_status_diagnostics_count_unparsed_response_without_payload_leak(caplog) -> None:
    callbacks = {}
    removed = []

    def subscribe(kind, callback):
        callbacks[kind] = callback
        return SimpleNamespace(unsubscribe=lambda: removed.append(kind))

    async def request(_public_key, timeout):
        callbacks[EventType.BINARY_RESPONSE](
            Event(EventType.BINARY_RESPONSE, {"data": "private-payload"})
        )
        return None

    backend = MeshCoreBackend(
        SimpleNamespace(commands=SimpleNamespace(req_status_sync=request), subscribe=subscribe),
        timeout=5,
    )
    assert await backend.request_repeater_status("ab" * 32, timeout=1) is None
    assert "'binary_responses': 1" in caplog.text
    assert "'matching_status': 0" in caplog.text
    assert "private-payload" not in caplog.text
    assert set(removed) == {EventType.BINARY_RESPONSE, EventType.STATUS_RESPONSE}


async def test_read_status_reads_clock_without_setting_it() -> None:
    from datetime import UTC, datetime

    calls = []
    now = int(datetime.now(UTC).timestamp())

    class Commands:
        def __getattr__(self, name):
            async def command(**_kwargs):
                calls.append(name)
                assert name != "set_time"
                return Event(EventType.OK, {"time": now} if name == "get_time" else {})

            return command

    backend = MeshCoreBackend(SimpleNamespace(commands=Commands()), timeout=5)
    snapshot, _ = await backend.read_status()
    assert snapshot["clock"]["time"] == now
    assert abs(snapshot["clock"]["offset_seconds"]) < 2
    assert calls.count("get_time") == 1


async def test_local_advert_is_zero_hop_and_rate_limited(tmp_path: Path) -> None:
    calls = []

    async def send_advert(*, flood):
        calls.append(flood)
        return Event(EventType.OK, {})

    backend = MeshCoreBackend(
        SimpleNamespace(commands=SimpleNamespace(send_advert=send_advert)), timeout=5
    )
    manager = RadioManager(Settings(state_dir=tmp_path, socket_path=tmp_path / "test.sock"))
    manager._backend = backend
    manager._status.connected = True
    await manager.advertise_local()
    with pytest.raises(SendPolicyError):
        await manager.advertise_local()
    assert calls == [False]


async def test_meshcore_backend_correlates_untagged_status_by_requested_peer() -> None:
    callback = None

    class FakeSubscription:
        def unsubscribe(self) -> None:
            pass

    class FakeCommands:
        async def req_status_sync(self, _public_key: str, timeout: float):
            assert timeout == 11
            assert callback is not None
            callback(
                SimpleNamespace(
                    payload={"pubkey_pre": "ab" * 6, "bat": 4200},
                    attributes={"pubkey_prefix": "ab" * 6},
                )
            )
            return None

    def subscribe(event_type, event_callback):
        nonlocal callback
        if event_type is EventType.BINARY_RESPONSE:
            return FakeSubscription()
        assert event_type is EventType.STATUS_RESPONSE
        callback = event_callback
        return FakeSubscription()

    client = SimpleNamespace(commands=FakeCommands(), subscribe=subscribe)
    backend = MeshCoreBackend(client, timeout=5.0)  # type: ignore[arg-type]

    result = await backend.request_repeater_status("ab" * 32, timeout=11)

    assert result == {
        "pubkey_pre": "ab" * 6,
        "bat": 4200,
        "_response_correlation": "peer_prefix",
    }


async def test_meshcore_backend_rejects_untagged_status_from_other_peer() -> None:
    callback = None

    class FakeSubscription:
        def unsubscribe(self) -> None:
            pass

    class FakeCommands:
        async def req_status_sync(self, _public_key: str, timeout: float):
            assert callback is not None
            callback(
                SimpleNamespace(
                    payload={"pubkey_pre": "cd" * 6, "bat": 4200},
                    attributes={"pubkey_prefix": "cd" * 6},
                )
            )
            return None

    def subscribe(_event_type, event_callback):
        nonlocal callback
        callback = event_callback
        return FakeSubscription()

    client = SimpleNamespace(commands=FakeCommands(), subscribe=subscribe)
    backend = MeshCoreBackend(client, timeout=5.0)  # type: ignore[arg-type]

    assert await backend.request_repeater_status("ab" * 32, timeout=11) is None


async def test_meshcore_backend_requests_legacy_status_once_by_peer_prefix() -> None:
    callback = None

    class FakeSubscription:
        def unsubscribe(self) -> None:
            pass

    class FakeCommands:
        def __init__(self) -> None:
            self.calls: list[str] = []

        async def send_statusreq(self, public_key: str):
            self.calls.append(public_key)
            assert callback is not None
            callback(
                Event(
                    EventType.STATUS_RESPONSE,
                    {"pubkey_pre": "ab" * 6, "bat": 4200},
                    {"pubkey_prefix": "ab" * 6},
                )
            )
            return Event(EventType.MSG_SENT, {"suggested_timeout": 4000})

    def subscribe(event_type, event_callback):
        nonlocal callback
        if event_type is EventType.BINARY_RESPONSE:
            return FakeSubscription()
        assert event_type is EventType.STATUS_RESPONSE
        callback = event_callback
        return FakeSubscription()

    commands = FakeCommands()
    client = SimpleNamespace(commands=commands, subscribe=subscribe)
    backend = MeshCoreBackend(client, timeout=5.0)  # type: ignore[arg-type]

    result = await backend.request_repeater_status(
        "ab" * 32,
        timeout=11,
        transport=RepeaterStatusTransport.LEGACY,
    )

    assert commands.calls == ["ab" * 32]
    assert result == {
        "pubkey_pre": "ab" * 6,
        "bat": 4200,
        "_response_correlation": "peer_prefix",
    }


async def test_meshcore_backend_correlates_repeater_login_by_exact_peer_prefix() -> None:
    callbacks = {}

    class FakeSubscription:
        def unsubscribe(self) -> None:
            pass

    class FakeCommands:
        async def send_login(self, public_key: str, password: str):
            assert public_key == "ab" * 32
            assert password == "guest-pass"
            callbacks[EventType.LOGIN_SUCCESS](
                Event(
                    EventType.LOGIN_SUCCESS,
                    {"pubkey_prefix": "cd" * 6, "permissions": 1, "is_admin": True},
                    {"pubkey_prefix": "cd" * 6},
                )
            )
            callbacks[EventType.LOGIN_SUCCESS](
                Event(
                    EventType.LOGIN_SUCCESS,
                    {"pubkey_prefix": "ab" * 6, "permissions": 0, "is_admin": False},
                    {"pubkey_prefix": "ab" * 6},
                )
            )
            return Event(EventType.MSG_SENT, {"suggested_timeout": 4000})

    def subscribe(event_type, event_callback):
        callbacks[event_type] = event_callback
        return FakeSubscription()

    client = SimpleNamespace(commands=FakeCommands(), subscribe=subscribe)
    backend = MeshCoreBackend(client, timeout=5.0)  # type: ignore[arg-type]

    result = await backend.login_repeater("ab" * 32, "guest-pass", timeout=1)

    assert result == {
        "pubkey_prefix": "ab" * 6,
        "permissions": 0,
        "is_admin": False,
        "_login_succeeded": True,
    }


async def test_meshcore_backend_reports_exact_peer_login_failure() -> None:
    callbacks = {}

    class FakeSubscription:
        def unsubscribe(self) -> None:
            pass

    class FakeCommands:
        async def send_login(self, _public_key: str, _password: str):
            callbacks[EventType.LOGIN_FAILED](
                Event(
                    EventType.LOGIN_FAILED,
                    {"pubkey_prefix": "ab" * 6},
                    {"pubkey_prefix": "ab" * 6},
                )
            )
            return Event(EventType.MSG_SENT, {"suggested_timeout": 4000})

    def subscribe(event_type, event_callback):
        callbacks[event_type] = event_callback
        return FakeSubscription()

    client = SimpleNamespace(commands=FakeCommands(), subscribe=subscribe)
    backend = MeshCoreBackend(client, timeout=5.0)  # type: ignore[arg-type]

    result = await backend.login_repeater("ab" * 32, "wrong", timeout=1)

    assert result == {
        "pubkey_prefix": "ab" * 6,
        "_login_succeeded": False,
    }
