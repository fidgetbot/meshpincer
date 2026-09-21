from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Mapping, Sequence
from datetime import UTC, datetime
from typing import Any, Protocol

from meshcore import MeshCore

from .config import Settings
from .discovery import SerialDevice, discover_serial_device
from .models import (
    ChannelRecord,
    ContactRecord,
    RadioHealth,
    RadioProfile,
    RadioStatus,
    RadioTelemetry,
)

logger = logging.getLogger(__name__)


class RadioBackend(Protocol):
    async def read_status(self) -> tuple[dict[str, Any], int]: ...

    async def read_contacts(self) -> Sequence[Mapping[str, Any]]: ...

    async def read_channels(self, count: int) -> Sequence[Mapping[str, Any]]: ...

    async def disconnect(self) -> None: ...


Discoverer = Callable[[Settings], SerialDevice]
Connector = Callable[[str, float], Awaitable[RadioBackend]]


def _event_payload(event: Any, operation: str) -> dict[str, Any]:
    if event is None:
        raise ConnectionError(f"{operation} returned no response")
    if event.is_error():
        reason = (
            event.payload.get("reason", "device error")
            if isinstance(event.payload, dict)
            else "device error"
        )
        raise ConnectionError(f"{operation} failed: {reason}")
    if not isinstance(event.payload, dict):
        raise ConnectionError(f"{operation} returned an invalid response")
    return event.payload


class MeshCoreBackend:
    def __init__(self, client: MeshCore, timeout: float) -> None:
        self.client = client
        self.timeout = timeout

    @classmethod
    async def connect(cls, port: str, timeout: float) -> MeshCoreBackend:
        client = await MeshCore.create_serial(
            port,
            default_timeout=timeout,
            auto_reconnect=False,
        )
        if client is None:
            raise ConnectionError("MeshCore serial handshake failed")
        return cls(client, timeout)

    async def read_status(self) -> tuple[dict[str, Any], int]:
        commands = self.client.commands
        self_info = _event_payload(
            await commands.send_appstart(timeout=self.timeout),
            "self info",
        )
        device = _event_payload(await commands.send_device_query(), "device info")
        battery = _event_payload(await commands.get_bat(), "battery info")
        core = _event_payload(await commands.get_stats_core(), "core stats")
        radio = _event_payload(await commands.get_stats_radio(), "radio stats")
        packets = _event_payload(await commands.get_stats_packets(), "packet stats")
        telemetry = _event_payload(await commands.get_self_telemetry(), "self telemetry")
        return (
            {
                "self": self_info,
                "device": device,
                "battery": battery,
                "core": core,
                "radio": radio,
                "packets": packets,
                "telemetry": telemetry,
            },
            int(device.get("max_channels", 0)),
        )

    async def read_contacts(self) -> Sequence[Mapping[str, Any]]:
        _event_payload(
            await self.client.commands.get_contacts(timeout=self.timeout),
            "contacts",
        )
        return list(self.client.contacts.values())

    async def read_channels(self, count: int) -> Sequence[Mapping[str, Any]]:
        channels = []
        for index in range(count):
            channel = _event_payload(
                await self.client.commands.get_channel(index),
                f"channel {index}",
            )
            secret = channel.get("channel_secret", b"")
            channels.append(
                {
                    "channel_idx": index,
                    "channel_name": channel.get("channel_name", ""),
                    "channel_hash": channel.get("channel_hash"),
                    "configured": bool(secret and any(secret)),
                }
            )
        return channels

    async def disconnect(self) -> None:
        await self.client.disconnect()


async def connect_meshcore(port: str, timeout: float) -> RadioBackend:
    return await MeshCoreBackend.connect(port, timeout)


def _telemetry(payload: Mapping[str, Any]) -> RadioTelemetry:
    result = RadioTelemetry()
    for entry in payload.get("lpp", []):
        if not isinstance(entry, Mapping):
            continue
        kind = entry.get("type")
        value = entry.get("value")
        if kind == "voltage" and isinstance(value, int | float):
            result.voltage = float(value)
        elif kind == "temperature" and isinstance(value, int | float):
            result.temperature_c = float(value)
        elif kind == "gps" and isinstance(value, Mapping):
            result.latitude = _optional_float(value.get("latitude", value.get("lat")))
            result.longitude = _optional_float(value.get("longitude", value.get("lon")))
            result.altitude_m = _optional_float(value.get("altitude", value.get("alt")))
    return result


def _optional_float(value: Any) -> float | None:
    return float(value) if isinstance(value, int | float) else None


class RadioManager:
    def __init__(
        self,
        settings: Settings,
        *,
        discoverer: Discoverer = discover_serial_device,
        connector: Connector = connect_meshcore,
    ) -> None:
        self.settings = settings
        self._discoverer = discoverer
        self._connector = connector
        self._status = RadioStatus(serial_port=settings.serial_port)
        self._contacts: list[ContactRecord] = []
        self._channels: list[ChannelRecord] = []
        self._backend: RadioBackend | None = None
        self._task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        if self._task is None:
            self._stop.clear()
            self._task = asyncio.create_task(self._run(), name="meshpincer-radio")

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        await self._disconnect()

    async def status(self) -> RadioStatus:
        async with self._lock:
            return self._status.model_copy(deep=True)

    async def contacts(self) -> list[ContactRecord]:
        async with self._lock:
            return [contact.model_copy(deep=True) for contact in self._contacts]

    async def channels(self, *, include_empty: bool = False) -> list[ChannelRecord]:
        async with self._lock:
            channels = (
                self._channels
                if include_empty
                else [item for item in self._channels if item.configured]
            )
            return [channel.model_copy(deep=True) for channel in channels]

    async def _run(self) -> None:
        delay = self.settings.reconnect_initial_seconds
        while not self._stop.is_set():
            try:
                device = await asyncio.to_thread(self._discoverer, self.settings)
                backend = await self._connector(device.port, self.settings.query_timeout_seconds)
                self._backend = backend
                await self._refresh(device, backend)
                delay = self.settings.reconnect_initial_seconds
                while not self._stop.is_set():
                    await self._wait(self.settings.refresh_interval_seconds)
                    if not self._stop.is_set():
                        await self._refresh(device, backend)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("MeshCore radio unavailable: %s", exc)
                async with self._lock:
                    self._status.connected = False
                    self._status.last_error = str(exc)
                await self._disconnect()
                await self._wait(delay)
                delay = min(delay * 2, self.settings.reconnect_max_seconds)

    async def _refresh(self, device: SerialDevice, backend: RadioBackend) -> None:
        payloads, max_channels = await backend.read_status()
        contacts = await backend.read_contacts()
        channels = await backend.read_channels(max_channels)
        self_info = payloads["self"]
        device_info = payloads["device"]
        battery = payloads["battery"]
        core = payloads["core"]
        radio = payloads["radio"]
        packets = payloads["packets"]

        status = RadioStatus(
            connected=True,
            serial_port=device.port,
            node_name=self_info.get("name"),
            public_key=self_info.get("public_key"),
            model=device_info.get("model") or device.product,
            firmware_version=device_info.get("ver"),
            protocol_version=device_info.get("fw ver"),
            radio=RadioProfile(
                frequency_mhz=float(self_info["radio_freq"]),
                bandwidth_khz=float(self_info["radio_bw"]),
                spreading_factor=int(self_info["radio_sf"]),
                coding_rate=int(self_info["radio_cr"]),
                tx_power_dbm=int(self_info["tx_power"]),
            ),
            health=RadioHealth(
                battery_mv=core.get("battery_mv", battery.get("level")),
                storage_used_kb=battery.get("used_kb"),
                storage_total_kb=battery.get("total_kb"),
                uptime_seconds=core.get("uptime_secs"),
                device_errors=core.get("errors"),
                queue_length=core.get("queue_len"),
                noise_floor_dbm=radio.get("noise_floor"),
                last_rssi_dbm=radio.get("last_rssi"),
                last_snr_db=radio.get("last_snr"),
                packets_received=packets.get("recv"),
                packets_sent=packets.get("sent"),
                receive_errors=packets.get("recv_errors"),
            ),
            telemetry=_telemetry(payloads["telemetry"]),
            last_refreshed_at=datetime.now(UTC),
        )
        contact_records = [
            ContactRecord(
                public_key=str(contact["public_key"]),
                name=str(contact.get("adv_name", "")),
                node_type=contact.get("type"),
                flags=contact.get("flags"),
                path_length=contact.get("out_path_len"),
                last_advert=contact.get("last_advert"),
            )
            for contact in contacts
        ]
        channel_records = [
            ChannelRecord(
                index=int(channel["channel_idx"]),
                name=str(channel.get("channel_name", "")),
                configured=bool(channel.get("configured")),
                channel_hash=channel.get("channel_hash"),
            )
            for channel in channels
        ]
        async with self._lock:
            self._status = status
            self._contacts = contact_records
            self._channels = channel_records

    async def _disconnect(self) -> None:
        backend, self._backend = self._backend, None
        if backend is not None:
            try:
                await backend.disconnect()
            except Exception as exc:
                logger.debug("error while disconnecting MeshCore radio: %s", exc)

    async def _wait(self, delay: float) -> None:
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=delay)
        except TimeoutError:
            pass
