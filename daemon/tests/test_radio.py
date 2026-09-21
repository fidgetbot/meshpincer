from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

from meshpincer.config import Settings
from meshpincer.discovery import SerialDevice
from meshpincer.radio import RadioManager


class FakeBackend:
    def __init__(self) -> None:
        self.disconnected = False

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
        return [
            {
                "public_key": "34" * 32,
                "adv_name": "Peer",
                "type": 1,
                "flags": 0,
                "out_path_len": 2,
                "last_advert": 100,
                "adv_lat": 47.1,
                "adv_lon": -122.1,
            }
        ]

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
                "channel_name": "",
                "channel_hash": "37",
                "configured": False,
            },
        ]

    async def disconnect(self) -> None:
        self.disconnected = True


async def wait_until_connected(manager: RadioManager) -> None:
    for _ in range(100):
        if (await manager.status()).connected:
            return
        await asyncio.sleep(0.001)
    raise AssertionError("radio manager did not connect")


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
