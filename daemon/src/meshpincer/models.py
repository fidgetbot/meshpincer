from __future__ import annotations

from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, Field


class DeliveryState(StrEnum):
    QUEUED = "queued"
    TRANSMITTED = "transmitted"
    ACKNOWLEDGED = "acknowledged"
    TIMED_OUT = "timed_out"
    FAILED = "failed"


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


class ChannelRecord(BaseModel):
    index: int = Field(ge=0)
    name: str
    configured: bool
    channel_hash: str | None = None


class ServiceStatus(BaseModel):
    service: str = "meshpincer"
    version: str
    radio: RadioStatus
    last_event_id: int = 0


class MessageRecord(BaseModel):
    id: int
    direction: str
    kind: str
    peer_key: str | None = None
    channel_index: int | None = None
    text: str
    mesh_timestamp: int | None = None
    recorded_at: datetime
    delivery_state: DeliveryState


class SendMessageRequest(BaseModel):
    text: str = Field(min_length=1, max_length=1024)
    public_key: str | None = None
    channel_index: int | None = Field(default=None, ge=0)


class SendMessageResult(BaseModel):
    message_id: int
    delivery_state: DeliveryState


class RepeaterConfigRequest(BaseModel):
    settings: dict[str, str | int | float | bool]
