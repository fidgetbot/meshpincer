from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

from meshcore import MeshCore
from meshcore.events import EventType

from .config import Settings
from .discovery import SerialDevice, discover_serial_device
from .models import (
    AutoAddConfig,
    ChannelRecord,
    ContactRecord,
    ContactRouteMode,
    InboundMessage,
    RadioHealth,
    RadioProfile,
    RadioStatus,
    RadioTelemetry,
    RepeaterStatus,
)

logger = logging.getLogger(__name__)


class RadioBackend(Protocol):
    async def read_status(self) -> tuple[dict[str, Any], int]: ...

    async def read_contacts(self) -> Sequence[Mapping[str, Any]]: ...

    async def read_channels(self, count: int) -> Sequence[Mapping[str, Any]]: ...

    async def read_autoadd_config(self) -> Mapping[str, Any]: ...

    async def set_autoadd_config(self, config: int) -> None: ...

    async def upsert_contact(
        self,
        public_key: str,
        name: str,
        node_type: int,
        flags: int,
    ) -> None: ...

    async def remove_contact(self, public_key: str) -> None: ...

    async def set_contact_route(
        self,
        public_key: str,
        mode: ContactRouteMode,
    ) -> None: ...

    async def set_channel(self, channel_index: int, name: str, secret: bytes) -> None: ...

    async def rename_channel(self, channel_index: int, name: str) -> None: ...

    async def clear_channel(self, channel_index: int) -> None: ...

    async def start_receiving(self, handler: InboundHandler) -> None: ...

    async def send_direct(
        self,
        public_key: str,
        text: str,
        on_transmitted: TransmittedHandler,
    ) -> DirectSendOutcome: ...

    async def send_channel(
        self,
        channel_index: int,
        text: str,
        on_transmitted: ChannelTransmittedHandler,
    ) -> None: ...

    async def request_repeater_status(
        self,
        public_key: str,
        timeout: float,
    ) -> Mapping[str, Any] | None: ...

    async def disconnect(self) -> None: ...


Discoverer = Callable[[Settings], SerialDevice]
Connector = Callable[[str, float], Awaitable[RadioBackend]]
InboundHandler = Callable[[InboundMessage], Awaitable[Any]]
TransmittedHandler = Callable[[str], Awaitable[Any]]
ChannelTransmittedHandler = Callable[[], Awaitable[Any]]


@dataclass(frozen=True, slots=True)
class DirectSendOutcome:
    ack_code: str
    acknowledged: bool
    trip_time_ms: int | None = None


class SendPolicyError(ValueError):
    pass


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
        self._message_subscriptions: list[Any] = []
        self._receiving = False

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

    async def read_autoadd_config(self) -> Mapping[str, Any]:
        return _event_payload(
            await self.client.commands.get_autoadd_config(),
            "auto-add configuration",
        )

    async def set_autoadd_config(self, config: int) -> None:
        _event_payload(
            await self.client.commands.set_autoadd_config(config),
            "auto-add configuration update",
        )

    async def upsert_contact(
        self,
        public_key: str,
        name: str,
        node_type: int,
        flags: int,
    ) -> None:
        normalized_key = public_key.lower()
        current = next(
            (
                dict(contact)
                for contact in self.client.contacts.values()
                if str(contact.get("public_key", "")).lower() == normalized_key
            ),
            None,
        )
        contact = current or {
            "public_key": normalized_key,
            "out_path_len": -1,
            "out_path_hash_mode": 0,
            "out_path": "",
            "last_advert": 0,
            "adv_lat": 0.0,
            "adv_lon": 0.0,
        }
        contact.update(
            {
                "public_key": normalized_key,
                "adv_name": name,
                "type": node_type,
                "flags": flags,
            }
        )
        _event_payload(
            await self.client.commands.add_contact(contact),
            "contact upsert",
        )

    async def remove_contact(self, public_key: str) -> None:
        _event_payload(
            await self.client.commands.remove_contact(public_key),
            "contact removal",
        )

    async def set_contact_route(
        self,
        public_key: str,
        mode: ContactRouteMode,
    ) -> None:
        normalized_key = public_key.lower()
        contact = next(
            (
                contact
                for contact in self.client.contacts.values()
                if str(contact.get("public_key", "")).lower() == normalized_key
            ),
            None,
        )
        if contact is None:
            raise ValueError("contact is not known to the companion radio")
        if mode is ContactRouteMode.ZERO_HOP:
            current_hash_mode = int(contact.get("out_path_hash_mode", -1))
            # Flood contacts are decoded with hash mode -1. Passing that value
            # to meshcore_py makes its path-size calculation divide by zero.
            # Let the library query the device's configured hash mode instead.
            path_hash_mode = current_hash_mode if current_hash_mode >= 0 else None
            _event_payload(
                await self.client.commands.change_contact_path(
                    contact,
                    "",
                    path_hash_mode=path_hash_mode,
                ),
                "contact route update",
            )
            return
        if mode is ContactRouteMode.FLOOD:
            _event_payload(
                await self.client.commands.reset_path(normalized_key),
                "contact route reset",
            )
            return
        raise ValueError("unsupported contact route mode")

    async def set_channel(self, channel_index: int, name: str, secret: bytes) -> None:
        _event_payload(
            await self.client.commands.set_channel(channel_index, name, secret),
            f"channel {channel_index} configuration",
        )

    async def rename_channel(self, channel_index: int, name: str) -> None:
        channel = _event_payload(
            await self.client.commands.get_channel(channel_index),
            f"channel {channel_index}",
        )
        secret = channel.get("channel_secret")
        if not isinstance(secret, bytes) or len(secret) != 16 or not any(secret):
            raise ValueError("channel is not configured")
        await self.set_channel(channel_index, name, secret)

    async def clear_channel(self, channel_index: int) -> None:
        await self.set_channel(channel_index, "", bytes(16))

    async def start_receiving(self, handler: InboundHandler) -> None:
        if self._receiving:
            return

        async def direct_message(event: Any) -> None:
            payload = event.payload
            prefix = str(payload.get("pubkey_prefix", "")) or None
            contact = self.client.get_contact_by_key_prefix(prefix) if prefix else None
            peer_key = str(contact["public_key"]) if contact else None
            await handler(
                InboundMessage(
                    kind="direct",
                    peer_key=peer_key,
                    peer_key_prefix=prefix,
                    text=str(payload.get("text", "")),
                    mesh_timestamp=payload.get("sender_timestamp"),
                    snr=payload.get("SNR"),
                    path_length=payload.get("path_len"),
                    text_type=payload.get("txt_type"),
                )
            )

        async def channel_message(event: Any) -> None:
            payload = event.payload
            await handler(
                InboundMessage(
                    kind="channel",
                    channel_index=payload.get("channel_idx"),
                    text=str(payload.get("text", "")),
                    mesh_timestamp=payload.get("sender_timestamp"),
                    snr=payload.get("SNR"),
                    path_length=payload.get("path_len"),
                    text_type=payload.get("txt_type"),
                )
            )

        self._message_subscriptions = [
            self.client.subscribe(EventType.CONTACT_MSG_RECV, direct_message),
            self.client.subscribe(EventType.CHANNEL_MSG_RECV, channel_message),
        ]
        await self.client.start_auto_message_fetching()
        self._receiving = True

    async def send_direct(
        self,
        public_key: str,
        text: str,
        on_transmitted: TransmittedHandler,
    ) -> DirectSendOutcome:
        loop = asyncio.get_running_loop()
        acknowledged: asyncio.Future[int | None] = loop.create_future()
        early_acks: dict[str, int | None] = {}
        expected_ack: str | None = None

        def on_ack(event: Any) -> None:
            nonlocal expected_ack
            code = str(event.attributes.get("code", ""))
            trip_time = event.payload.get("trip_time") if isinstance(event.payload, dict) else None
            if expected_ack is not None and code == expected_ack:
                if not acknowledged.done():
                    acknowledged.set_result(trip_time)
            else:
                early_acks[code] = trip_time

        subscription = self.client.subscribe(EventType.ACK, on_ack)
        try:
            event = await self.client.commands.send_msg(
                public_key,
                text,
                attempt=0,
            )
            payload = _event_payload(event, "direct message send")
            if int(payload.get("type", -1)) != 0:
                raise ConnectionError("device used flood routing for a direct-only send")
            raw_ack = payload.get("expected_ack")
            expected_ack = raw_ack.hex() if isinstance(raw_ack, bytes) else str(raw_ack)
            if not expected_ack:
                raise ConnectionError("direct message send returned no ACK code")
            await on_transmitted(expected_ack)
            if expected_ack in early_acks and not acknowledged.done():
                acknowledged.set_result(early_acks[expected_ack])
            timeout = max(float(payload.get("suggested_timeout", 0)) / 1000 * 1.2, 1.0)
            try:
                trip_time = await asyncio.wait_for(asyncio.shield(acknowledged), timeout)
            except TimeoutError:
                return DirectSendOutcome(ack_code=expected_ack, acknowledged=False)
            return DirectSendOutcome(
                ack_code=expected_ack,
                acknowledged=True,
                trip_time_ms=trip_time,
            )
        finally:
            subscription.unsubscribe()

    async def send_channel(
        self,
        channel_index: int,
        text: str,
        on_transmitted: ChannelTransmittedHandler,
    ) -> None:
        event = await self.client.commands.send_chan_msg(channel_index, text)
        _event_payload(event, "channel message send")
        await on_transmitted()

    async def request_repeater_status(
        self,
        public_key: str,
        timeout: float,
    ) -> Mapping[str, Any] | None:
        requested_prefix = public_key[:12].lower()
        loop = asyncio.get_running_loop()
        peer_response: asyncio.Future[Mapping[str, Any]] = loop.create_future()

        def capture_peer_response(event: Any) -> None:
            payload = event.payload if isinstance(event.payload, Mapping) else {}
            event_prefix = str(
                event.attributes.get("pubkey_prefix") or payload.get("pubkey_pre") or ""
            ).lower()
            if event_prefix == requested_prefix and not peer_response.done():
                peer_response.set_result(payload)

        subscription = self.client.subscribe(EventType.STATUS_RESPONSE, capture_peer_response)
        try:
            payload = await self.client.commands.req_status_sync(public_key, timeout=timeout)
            if payload is not None:
                return {**payload, "_response_correlation": "request_tag"}
            if peer_response.done():
                logger.info("Accepted repeater status correlated by requested public-key prefix")
                return {
                    **peer_response.result(),
                    "_response_correlation": "peer_prefix",
                }
            return None
        finally:
            subscription.unsubscribe()

    async def stop_receiving(self) -> None:
        if self._receiving:
            await self.client.stop_auto_message_fetching()
        for subscription in self._message_subscriptions:
            self.client.unsubscribe(subscription)
        self._message_subscriptions = []
        self._receiving = False

    async def disconnect(self) -> None:
        await self.stop_receiving()
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
        message_handler: InboundHandler | None = None,
    ) -> None:
        self.settings = settings
        self._discoverer = discoverer
        self._connector = connector
        self._message_handler = message_handler
        self._status = RadioStatus(serial_port=settings.serial_port)
        self._contacts: list[ContactRecord] = []
        self._channels: list[ChannelRecord] = []
        self._backend: RadioBackend | None = None
        self._task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()
        self._lock = asyncio.Lock()
        self._operation_lock = asyncio.Lock()
        self._last_direct_send = float("-inf")
        self._last_direct_send_by_peer: dict[str, float] = {}
        self._last_channel_send = float("-inf")
        self._last_channel_send_by_index: dict[int, float] = {}
        self._last_repeater_status = float("-inf")
        self._last_repeater_status_by_peer: dict[str, float] = {}

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

    async def autoadd_config(self) -> AutoAddConfig:
        async with self._operation_lock:
            backend = await self._connected_backend()
            return self._autoadd_record(await backend.read_autoadd_config())

    async def set_overwrite_oldest_non_favorite(self, enabled: bool) -> AutoAddConfig:
        async with self._operation_lock:
            backend = await self._connected_backend()
            current = self._autoadd_record(await backend.read_autoadd_config())
            updated = current.config | 0x01 if enabled else current.config & ~0x01
            if updated != current.config:
                await backend.set_autoadd_config(updated)
            readback = self._autoadd_record(await backend.read_autoadd_config())
            if readback.overwrite_oldest_non_favorite != enabled:
                raise ConnectionError("auto-add configuration did not read back from the radio")
            return readback

    async def upsert_contact(
        self,
        public_key: str,
        name: str,
        node_type: int,
        flags: int,
    ) -> ContactRecord:
        normalized_key = public_key.lower()
        async with self._operation_lock:
            backend = await self._connected_backend()
            await backend.upsert_contact(normalized_key, name, node_type, flags)
            contacts = await backend.read_contacts()
            records = self._contact_records(contacts)
            contact = next(
                (item for item in records if item.public_key.lower() == normalized_key),
                None,
            )
            if contact is None:
                raise ConnectionError("contact upsert did not read back from the radio")
            async with self._lock:
                self._contacts = records
            return contact.model_copy(deep=True)

    async def remove_contact(self, public_key: str) -> str:
        normalized_key = public_key.lower()
        async with self._operation_lock:
            backend = await self._connected_backend()
            async with self._lock:
                exists = any(item.public_key.lower() == normalized_key for item in self._contacts)
            if not exists:
                raise ValueError("contact is not known to the companion radio")
            await backend.remove_contact(normalized_key)
            contacts = await backend.read_contacts()
            records = self._contact_records(contacts)
            if any(item.public_key.lower() == normalized_key for item in records):
                raise ConnectionError("contact removal did not read back from the radio")
            async with self._lock:
                self._contacts = records
            return normalized_key

    async def set_contact_route(
        self,
        public_key: str,
        mode: ContactRouteMode,
    ) -> ContactRecord:
        normalized_key = public_key.lower()
        expected_path_length = 0 if mode is ContactRouteMode.ZERO_HOP else -1
        async with self._operation_lock:
            backend = await self._connected_backend()
            async with self._lock:
                exists = any(item.public_key.lower() == normalized_key for item in self._contacts)
            if not exists:
                raise ValueError("contact is not known to the companion radio")
            await backend.set_contact_route(normalized_key, mode)
            contacts = await backend.read_contacts()
            records = self._contact_records(contacts)
            contact = next(
                (item for item in records if item.public_key.lower() == normalized_key),
                None,
            )
            if contact is None:
                raise ConnectionError("contact route update did not read back from the radio")
            if contact.path_length != expected_path_length:
                raise ConnectionError("contact route update did not read back from the radio")
            async with self._lock:
                self._contacts = records
            return contact.model_copy(deep=True)

    async def set_channel(
        self,
        channel_index: int,
        name: str,
        secret: bytes,
    ) -> ChannelRecord:
        self._validate_private_channel_index(channel_index)
        async with self._operation_lock:
            backend = await self._connected_backend()
            await self._validate_channel_exists(channel_index)
            await backend.set_channel(channel_index, name, secret)
            return await self._refresh_channel_cache(backend, channel_index)

    async def rename_channel(self, channel_index: int, name: str) -> ChannelRecord:
        self._validate_private_channel_index(channel_index)
        async with self._operation_lock:
            backend = await self._connected_backend()
            await self._validate_channel_exists(channel_index)
            await backend.rename_channel(channel_index, name)
            return await self._refresh_channel_cache(backend, channel_index)

    async def clear_channel(self, channel_index: int) -> ChannelRecord:
        self._validate_private_channel_index(channel_index)
        async with self._operation_lock:
            backend = await self._connected_backend()
            await self._validate_channel_exists(channel_index)
            await backend.clear_channel(channel_index)
            return await self._refresh_channel_cache(backend, channel_index)

    async def send_direct(
        self,
        public_key: str,
        text: str,
        on_transmitted: TransmittedHandler,
    ) -> DirectSendOutcome:
        normalized_key = public_key.lower()
        async with self._operation_lock:
            async with self._lock:
                backend = self._backend
                connected = self._status.connected
                contact = next(
                    (item for item in self._contacts if item.public_key.lower() == normalized_key),
                    None,
                )
            if not connected or backend is None:
                raise ConnectionError("MeshCore radio is not connected")
            if contact is None:
                raise ValueError("direct-message recipient is not a known contact")
            if contact.path_length is None or contact.path_length < 0:
                raise ValueError("direct-only send requires a learned route")
            now = asyncio.get_running_loop().time()
            global_remaining = (
                self._last_direct_send + self.settings.direct_global_cooldown_seconds - now
            )
            peer_remaining = (
                self._last_direct_send_by_peer.get(normalized_key, float("-inf"))
                + self.settings.direct_peer_cooldown_seconds
                - now
            )
            remaining = max(global_remaining, peer_remaining)
            if remaining > 0:
                raise SendPolicyError(f"direct-message cooldown active for {remaining:.1f}s")

            async def mark_transmitted(ack_code: str) -> None:
                sent_at = asyncio.get_running_loop().time()
                self._last_direct_send = sent_at
                self._last_direct_send_by_peer[normalized_key] = sent_at
                await on_transmitted(ack_code)

            return await backend.send_direct(normalized_key, text, mark_transmitted)

    async def send_channel(
        self,
        channel_index: int,
        text: str,
        on_transmitted: ChannelTransmittedHandler,
    ) -> None:
        async with self._operation_lock:
            async with self._lock:
                backend = self._backend
                connected = self._status.connected
                channel = next(
                    (item for item in self._channels if item.index == channel_index),
                    None,
                )
            if not connected or backend is None:
                raise ConnectionError("MeshCore radio is not connected")
            if channel_index == 0:
                raise SendPolicyError("Public channel transmission is disabled")
            if channel_index not in self.settings.channel_send_allowlist:
                raise SendPolicyError("channel is not allowlisted for transmission")
            if channel is None or not channel.configured:
                raise ValueError("channel is not configured on the companion radio")

            now = asyncio.get_running_loop().time()
            global_remaining = (
                self._last_channel_send + self.settings.channel_global_cooldown_seconds - now
            )
            channel_remaining = (
                self._last_channel_send_by_index.get(channel_index, float("-inf"))
                + self.settings.channel_per_channel_cooldown_seconds
                - now
            )
            remaining = max(global_remaining, channel_remaining)
            if remaining > 0:
                raise SendPolicyError(f"channel-message cooldown active for {remaining:.1f}s")

            async def mark_transmitted() -> None:
                sent_at = asyncio.get_running_loop().time()
                self._last_channel_send = sent_at
                self._last_channel_send_by_index[channel_index] = sent_at
                await on_transmitted()

            await backend.send_channel(channel_index, text, mark_transmitted)

    async def request_repeater_status(self, public_key: str) -> RepeaterStatus:
        normalized_key = public_key.lower()
        async with self._operation_lock:
            async with self._lock:
                backend = self._backend
                connected = self._status.connected
                contact = next(
                    (item for item in self._contacts if item.public_key.lower() == normalized_key),
                    None,
                )
            if not connected or backend is None:
                raise ConnectionError("MeshCore radio is not connected")
            if contact is None:
                raise ValueError("repeater is not a known contact")
            if contact.node_type != 2:
                raise ValueError("contact is not a repeater")

            now = asyncio.get_running_loop().time()
            global_remaining = (
                self._last_repeater_status
                + self.settings.repeater_status_global_cooldown_seconds
                - now
            )
            peer_remaining = (
                self._last_repeater_status_by_peer.get(normalized_key, float("-inf"))
                + self.settings.repeater_status_peer_cooldown_seconds
                - now
            )
            remaining = max(global_remaining, peer_remaining)
            if remaining > 0:
                raise SendPolicyError(f"repeater-status cooldown active for {remaining:.1f}s")

            self._last_repeater_status = now
            self._last_repeater_status_by_peer[normalized_key] = now
            payload = await backend.request_repeater_status(
                normalized_key,
                self.settings.repeater_status_timeout_seconds,
            )
            if payload is None:
                raise TimeoutError("repeater did not return status before the timeout")

            return RepeaterStatus(
                public_key=normalized_key,
                name=contact.name,
                path_length=contact.path_length,
                response_correlation=payload.get("_response_correlation", "request_tag"),
                battery_mv=payload.get("bat"),
                tx_queue_length=payload.get("tx_queue_len"),
                noise_floor_dbm=payload.get("noise_floor"),
                last_rssi_dbm=payload.get("last_rssi"),
                packets_received=payload.get("nb_recv"),
                packets_sent=payload.get("nb_sent"),
                airtime=payload.get("airtime"),
                uptime_seconds=payload.get("uptime"),
                sent_flood=payload.get("sent_flood"),
                sent_direct=payload.get("sent_direct"),
                received_flood=payload.get("recv_flood"),
                received_direct=payload.get("recv_direct"),
                full_events=payload.get("full_evts"),
                last_snr_db=payload.get("last_snr"),
                direct_duplicates=payload.get("direct_dups"),
                flood_duplicates=payload.get("flood_dups"),
                receive_airtime=payload.get("rx_airtime"),
                receive_errors=payload.get("recv_errors"),
                requested_at=datetime.now(UTC),
            )

    async def _run(self) -> None:
        delay = self.settings.reconnect_initial_seconds
        while not self._stop.is_set():
            try:
                device = await asyncio.to_thread(self._discoverer, self.settings)
                backend = await self._connector(device.port, self.settings.query_timeout_seconds)
                self._backend = backend
                await self._refresh(device, backend)
                if self._message_handler is not None:
                    await backend.start_receiving(self._message_handler)
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
        async with self._operation_lock:
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
        contact_records = self._contact_records(contacts)
        channel_records = self._channel_records(channels)
        async with self._lock:
            self._status = status
            self._contacts = contact_records
            self._channels = channel_records

    async def _connected_backend(self) -> RadioBackend:
        async with self._lock:
            backend = self._backend
            connected = self._status.connected
        if not connected or backend is None:
            raise ConnectionError("MeshCore radio is not connected")
        return backend

    async def _refresh_channel_cache(
        self,
        backend: RadioBackend,
        channel_index: int,
    ) -> ChannelRecord:
        async with self._lock:
            channel_count = len(self._channels)
        records = self._channel_records(await backend.read_channels(channel_count))
        channel = next(item for item in records if item.index == channel_index)
        async with self._lock:
            self._channels = records
        return channel.model_copy(deep=True)

    async def _validate_channel_exists(self, channel_index: int) -> None:
        async with self._lock:
            channel_count = len(self._channels)
        if channel_index >= channel_count:
            raise ValueError("channel index is outside the companion radio's slot range")

    @staticmethod
    def _contact_records(contacts: Sequence[Mapping[str, Any]]) -> list[ContactRecord]:
        return [
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

    @staticmethod
    def _autoadd_record(payload: Mapping[str, Any]) -> AutoAddConfig:
        config = int(payload.get("config", 0))
        max_hops_value = payload.get("max_hops")
        return AutoAddConfig(
            config=config,
            max_hops=int(max_hops_value) if max_hops_value is not None else None,
            overwrite_oldest_non_favorite=bool(config & 0x01),
        )

    @staticmethod
    def _channel_records(channels: Sequence[Mapping[str, Any]]) -> list[ChannelRecord]:
        return [
            ChannelRecord(
                index=int(channel["channel_idx"]),
                name=str(channel.get("channel_name", "")),
                configured=bool(channel.get("configured")),
                channel_hash=channel.get("channel_hash"),
            )
            for channel in channels
        ]

    def _validate_private_channel_index(self, channel_index: int) -> None:
        if channel_index == 0:
            raise SendPolicyError("Public channel configuration is disabled")
        if channel_index < 0:
            raise ValueError("channel index must be non-negative")

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
