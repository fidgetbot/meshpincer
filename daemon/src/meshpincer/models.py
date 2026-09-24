from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator


class DeliveryState(StrEnum):
    RECEIVED = "received"
    QUEUED = "queued"
    TRANSMITTED = "transmitted"
    ACKNOWLEDGED = "acknowledged"
    TIMED_OUT = "timed_out"
    FAILED = "failed"


class RepeaterStatusTransport(StrEnum):
    BINARY = "binary"
    LEGACY = "legacy"


class RadioStatus(BaseModel):
    connected: bool = False
    serial_port: str | None = None
    node_name: str | None = None
    public_key: str | None = None
    model: str | None = None
    firmware_version: str | None = None
    protocol_version: int | None = None
    radio: RadioProfile | None = None
    health: RadioHealth | None = None
    telemetry: RadioTelemetry | None = None
    last_refreshed_at: datetime | None = None
    last_error: str | None = None


class RadioProfile(BaseModel):
    frequency_mhz: float
    bandwidth_khz: float
    spreading_factor: int
    coding_rate: int
    tx_power_dbm: int


class RadioHealth(BaseModel):
    battery_mv: int | None = None
    storage_used_kb: int | None = None
    storage_total_kb: int | None = None
    uptime_seconds: int | None = None
    device_errors: int | None = None
    queue_length: int | None = None
    noise_floor_dbm: int | None = None
    last_rssi_dbm: int | None = None
    last_snr_db: float | None = None
    packets_received: int | None = None
    packets_sent: int | None = None
    receive_errors: int | None = None


class RadioTelemetry(BaseModel):
    voltage: float | None = None
    temperature_c: float | None = None
    latitude: float | None = None
    longitude: float | None = None
    altitude_m: float | None = None


class ContactRecord(BaseModel):
    public_key: str
    name: str
    node_type: int | None = None
    flags: int | None = None
    path_length: int | None = None
    last_advert: int | None = None


class ContactRouteMode(StrEnum):
    ZERO_HOP = "zero_hop"
    FLOOD = "flood"


class UpdateContactRouteRequest(BaseModel):
    mode: ContactRouteMode


class UpsertContactRequest(BaseModel):
    name: str = Field(min_length=1)
    node_type: int = Field(ge=0, le=4)
    flags: int = Field(default=0, ge=0, le=255)

    @field_validator("name")
    @classmethod
    def validate_name_bytes(cls, value: str) -> str:
        if len(value.encode("utf-8")) > 32:
            raise ValueError("contact name must be at most 32 UTF-8 bytes")
        return value


class AutoAddConfig(BaseModel):
    config: int = Field(ge=0, le=255)
    max_hops: int | None = Field(default=None, ge=0, le=64)
    overwrite_oldest_non_favorite: bool


class UpdateAutoAddConfigRequest(BaseModel):
    overwrite_oldest_non_favorite: bool


class ChannelRecord(BaseModel):
    index: int = Field(ge=0)
    name: str
    configured: bool
    channel_hash: str | None = None


class SetChannelRequest(BaseModel):
    name: str = Field(min_length=1)
    secret_hex: str = Field(pattern=r"^[0-9A-Fa-f]{32}$", exclude=True)

    @field_validator("name")
    @classmethod
    def validate_name_bytes(cls, value: str) -> str:
        if len(value.encode("utf-8")) > 32:
            raise ValueError("channel name must be at most 32 UTF-8 bytes")
        return value


class RenameChannelRequest(BaseModel):
    name: str = Field(min_length=1)

    @field_validator("name")
    @classmethod
    def validate_name_bytes(cls, value: str) -> str:
        if len(value.encode("utf-8")) > 32:
            raise ValueError("channel name must be at most 32 UTF-8 bytes")
        return value


class RepeaterStatus(BaseModel):
    public_key: str
    name: str
    path_length: int | None = None
    transport: RepeaterStatusTransport = RepeaterStatusTransport.BINARY
    response_correlation: Literal["request_tag", "peer_prefix"] = "request_tag"
    battery_mv: int | None = None
    tx_queue_length: int | None = None
    noise_floor_dbm: int | None = None
    last_rssi_dbm: int | None = None
    packets_received: int | None = None
    packets_sent: int | None = None
    airtime: int | None = None
    uptime_seconds: int | None = None
    sent_flood: int | None = None
    sent_direct: int | None = None
    received_flood: int | None = None
    received_direct: int | None = None
    full_events: int | None = None
    last_snr_db: float | None = None
    direct_duplicates: int | None = None
    flood_duplicates: int | None = None
    receive_airtime: int | None = None
    receive_errors: int | None = None
    requested_at: datetime


class DatabaseStatus(BaseModel):
    database_bytes: int = Field(ge=0)
    wal_bytes: int = Field(ge=0)
    message_count: int = Field(ge=0)
    event_count: int = Field(ge=0)
    consumer_count: int = Field(ge=0)
    oldest_message_at: datetime | None = None
    oldest_event_at: datetime | None = None
    earliest_available_event_id: int | None = Field(default=None, ge=0)
    pruned_through_event_id: int = Field(default=0, ge=0)
    last_housekeeping_at: datetime | None = None
    next_housekeeping_at: datetime | None = None
    retention_days: int = Field(ge=1)
    max_messages: int = Field(ge=1)
    cursor_max_idle_days: int = Field(ge=1)


class ServiceStatus(BaseModel):
    service: str = "meshpincer"
    version: str
    radio: RadioStatus
    last_event_id: int = 0
    database: DatabaseStatus


class HousekeepingRequest(BaseModel):
    dry_run: bool = True


class HousekeepingResult(BaseModel):
    dry_run: bool
    safe_through_event_id: int = Field(ge=0)
    prune_through_event_id: int = Field(ge=0)
    messages_pruned: int = Field(ge=0)
    events_pruned: int = Field(ge=0)
    cursors_expired: int = Field(ge=0)
    checkpointed: bool = False
    completed_at: datetime


class MessageRecord(BaseModel):
    id: int
    event_id: int | None = None
    direction: str
    kind: str
    peer_key: str | None = None
    peer_key_prefix: str | None = None
    channel_index: int | None = None
    text: str
    mesh_timestamp: int | None = None
    snr: float | None = None
    path_length: int | None = None
    text_type: int | None = None
    ack_code: str | None = None
    recorded_at: datetime
    delivery_state: DeliveryState


class InboundMessage(BaseModel):
    kind: Literal["direct", "channel"]
    text: str
    peer_key: str | None = None
    peer_key_prefix: str | None = None
    channel_index: int | None = Field(default=None, ge=0)
    mesh_timestamp: int | None = None
    snr: float | None = None
    path_length: int | None = None
    text_type: int | None = None


class EventRecord(BaseModel):
    id: int
    kind: str
    payload: dict[str, Any]
    recorded_at: datetime


class ConsumerCursor(BaseModel):
    consumer_id: str
    event_id: int = Field(ge=0)
    history_gap: bool = False
    history_gap_before: int | None = Field(default=None, ge=1)
    expired_at: datetime | None = None


class AdvanceCursorRequest(BaseModel):
    event_id: int = Field(ge=0)


class SendMessageRequest(BaseModel):
    text: str = Field(min_length=1)
    public_key: str | None = Field(default=None, pattern=r"^[0-9A-Fa-f]{64}$")
    channel_index: int | None = Field(default=None, ge=0)

    @field_validator("text")
    @classmethod
    def validate_text_bytes(cls, value: str) -> str:
        if len(value.encode("utf-8")) > 160:
            raise ValueError("text must be at most 160 UTF-8 bytes")
        return value


class SendMessageResult(BaseModel):
    message_id: int
    delivery_state: DeliveryState
    ack_code: str | None = None


class RepeaterConfigRequest(BaseModel):
    settings: dict[str, str | int | float | bool]
