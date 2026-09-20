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
    public_key: str | None = None
    firmware_version: str | None = None
    last_error: str | None = None


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
