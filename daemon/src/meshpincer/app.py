from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Path, Query

from . import __version__
from .config import Settings
from .models import (
    AdvanceCursorRequest,
    ChannelRecord,
    ConsumerCursor,
    ContactRecord,
    DeliveryState,
    EventRecord,
    MessageRecord,
    RenameChannelRequest,
    RepeaterConfigRequest,
    RepeaterStatus,
    SendMessageRequest,
    SendMessageResult,
    ServiceStatus,
    SetChannelRequest,
    UpsertContactRequest,
)
from .radio import RadioManager, SendPolicyError
from .store import Store


def create_app(
    settings: Settings | None = None,
    radio_manager: RadioManager | None = None,
) -> FastAPI:
    resolved = settings or Settings.from_env()
    store = Store(resolved.database_path)
    radio = radio_manager or RadioManager(resolved, message_handler=store.record_inbound)

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        resolved.state_dir.mkdir(parents=True, exist_ok=True)
        await store.initialize()
        await radio.start()
        try:
            yield
        finally:
            await radio.stop()

    app = FastAPI(title="MeshPincer", version=__version__, lifespan=lifespan)

    @app.get("/v1/status", response_model=ServiceStatus)
    async def status() -> ServiceStatus:
        return ServiceStatus(
            version=__version__,
            radio=await radio.status(),
            last_event_id=await store.last_event_id(),
        )

    @app.get("/v1/contacts", response_model=list[ContactRecord])
    async def contacts() -> list[ContactRecord]:
        return await radio.contacts()

    @app.put("/v1/contacts/{public_key}", response_model=ContactRecord)
    async def upsert_contact(
        request: UpsertContactRequest,
        public_key: str = Path(pattern=r"^[0-9A-Fa-f]{64}$"),
    ) -> ContactRecord:
        normalized_key = public_key.lower()
        await store.append_event(
            "contact.upsert.requested",
            {
                "public_key": normalized_key,
                "name": request.name,
                "node_type": request.node_type,
                "flags": request.flags,
            },
        )
        try:
            contact = await radio.upsert_contact(
                normalized_key,
                request.name,
                request.node_type,
                request.flags,
            )
        except ValueError as exc:
            await store.append_event(
                "contact.upsert.rejected",
                {"public_key": normalized_key, "reason": str(exc)},
            )
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        except Exception as exc:
            await store.append_event(
                "contact.upsert.failed",
                {"public_key": normalized_key, "reason": str(exc)},
            )
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        await store.append_event("contact.upsert.succeeded", contact.model_dump(mode="json"))
        return contact

    @app.delete("/v1/contacts/{public_key}")
    async def remove_contact(
        public_key: str = Path(pattern=r"^[0-9A-Fa-f]{64}$"),
    ) -> dict[str, object]:
        normalized_key = public_key.lower()
        await store.append_event("contact.remove.requested", {"public_key": normalized_key})
        try:
            removed_key = await radio.remove_contact(normalized_key)
        except ValueError as exc:
            await store.append_event(
                "contact.remove.rejected",
                {"public_key": normalized_key, "reason": str(exc)},
            )
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        except Exception as exc:
            await store.append_event(
                "contact.remove.failed",
                {"public_key": normalized_key, "reason": str(exc)},
            )
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        await store.append_event("contact.remove.succeeded", {"public_key": removed_key})
        return {"public_key": removed_key, "removed": True}

    @app.get("/v1/channels", response_model=list[ChannelRecord])
    async def channels(include_empty: bool = False) -> list[ChannelRecord]:
        return await radio.channels(include_empty=include_empty)

    @app.put("/v1/channels/{channel_index}", response_model=ChannelRecord)
    async def set_channel(
        request: SetChannelRequest,
        channel_index: int = Path(ge=1),
    ) -> ChannelRecord:
        await store.append_event(
            "channel.set.requested",
            {"channel_index": channel_index, "name": request.name},
        )
        try:
            channel = await radio.set_channel(
                channel_index,
                request.name,
                bytes.fromhex(request.secret_hex),
            )
        except ValueError as exc:
            await store.append_event(
                "channel.set.rejected",
                {"channel_index": channel_index, "reason": str(exc)},
            )
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        except Exception as exc:
            await store.append_event(
                "channel.set.failed",
                {"channel_index": channel_index, "reason": str(exc)},
            )
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        await store.append_event("channel.set.succeeded", channel.model_dump(mode="json"))
        return channel

    @app.patch("/v1/channels/{channel_index}", response_model=ChannelRecord)
    async def rename_channel(
        request: RenameChannelRequest,
        channel_index: int = Path(ge=1),
    ) -> ChannelRecord:
        await store.append_event(
            "channel.rename.requested",
            {"channel_index": channel_index, "name": request.name},
        )
        try:
            channel = await radio.rename_channel(channel_index, request.name)
        except ValueError as exc:
            await store.append_event(
                "channel.rename.rejected",
                {"channel_index": channel_index, "reason": str(exc)},
            )
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        except Exception as exc:
            await store.append_event(
                "channel.rename.failed",
                {"channel_index": channel_index, "reason": str(exc)},
            )
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        await store.append_event("channel.rename.succeeded", channel.model_dump(mode="json"))
        return channel

    @app.delete("/v1/channels/{channel_index}", response_model=ChannelRecord)
    async def clear_channel(
        channel_index: int = Path(ge=1),
    ) -> ChannelRecord:
        await store.append_event(
            "channel.clear.requested",
            {"channel_index": channel_index},
        )
        try:
            channel = await radio.clear_channel(channel_index)
        except ValueError as exc:
            await store.append_event(
                "channel.clear.rejected",
                {"channel_index": channel_index, "reason": str(exc)},
            )
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        except Exception as exc:
            await store.append_event(
                "channel.clear.failed",
                {"channel_index": channel_index, "reason": str(exc)},
            )
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        await store.append_event("channel.clear.succeeded", channel.model_dump(mode="json"))
        return channel

    @app.get("/v1/messages", response_model=list[MessageRecord])
    async def messages(
        after_id: int = Query(default=0, ge=0),
        limit: int = Query(default=100, ge=1, le=500),
    ) -> list[MessageRecord]:
        return await store.list_messages(after_id=after_id, limit=limit)

    @app.get("/v1/events", response_model=list[EventRecord])
    async def events(
        after_id: int = Query(default=0, ge=0),
        limit: int = Query(default=100, ge=1, le=500),
    ) -> list[EventRecord]:
        return await store.list_events(after_id=after_id, limit=limit)

    @app.get(
        "/v1/consumers/{consumer_id}/events",
        response_model=list[EventRecord],
    )
    async def consumer_events(
        consumer_id: str = Path(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$"),
        limit: int = Query(default=100, ge=1, le=500),
    ) -> list[EventRecord]:
        cursor = await store.get_cursor(consumer_id)
        return await store.list_events(after_id=cursor.event_id, limit=limit)

    @app.get(
        "/v1/consumers/{consumer_id}/cursor",
        response_model=ConsumerCursor,
    )
    async def consumer_cursor(
        consumer_id: str = Path(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$"),
    ) -> ConsumerCursor:
        return await store.get_cursor(consumer_id)

    @app.put(
        "/v1/consumers/{consumer_id}/cursor",
        response_model=ConsumerCursor,
    )
    async def advance_consumer_cursor(
        request: AdvanceCursorRequest,
        consumer_id: str = Path(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$"),
    ) -> ConsumerCursor:
        try:
            return await store.advance_cursor(consumer_id, request.event_id)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @app.post("/v1/messages/direct", response_model=SendMessageResult)
    async def send_direct(request: SendMessageRequest) -> SendMessageResult:
        if not request.public_key:
            raise HTTPException(status_code=422, detail="public_key is required")
        message = await store.queue_outbound_direct(request.public_key, request.text)

        async def mark_transmitted(ack_code: str) -> None:
            await store.transition_outbound(
                message.id,
                state=DeliveryState.TRANSMITTED,
                ack_code=ack_code,
            )

        try:
            outcome = await radio.send_direct(
                request.public_key,
                request.text,
                mark_transmitted,
            )
        except SendPolicyError as exc:
            failed = await store.transition_outbound(
                message.id,
                state=DeliveryState.FAILED,
            )
            raise HTTPException(
                status_code=429,
                detail={"message_id": failed.id, "reason": str(exc)},
            ) from exc
        except ValueError as exc:
            failed = await store.transition_outbound(
                message.id,
                state=DeliveryState.FAILED,
            )
            raise HTTPException(
                status_code=422,
                detail={"message_id": failed.id, "reason": str(exc)},
            ) from exc
        except Exception as exc:
            failed = await store.transition_outbound(
                message.id,
                state=DeliveryState.FAILED,
            )
            raise HTTPException(
                status_code=503,
                detail={"message_id": failed.id, "reason": str(exc)},
            ) from exc

        final_state = (
            DeliveryState.ACKNOWLEDGED if outcome.acknowledged else DeliveryState.TIMED_OUT
        )
        final = await store.transition_outbound(
            message.id,
            state=final_state,
            ack_code=outcome.ack_code,
        )
        return SendMessageResult(
            message_id=final.id,
            delivery_state=final.delivery_state,
            ack_code=final.ack_code,
        )

    @app.post("/v1/messages/channel", response_model=SendMessageResult)
    async def send_channel(request: SendMessageRequest) -> SendMessageResult:
        if request.channel_index is None:
            raise HTTPException(status_code=422, detail="channel_index is required")
        message = await store.queue_outbound_channel(request.channel_index, request.text)

        async def mark_transmitted() -> None:
            await store.transition_outbound(
                message.id,
                state=DeliveryState.TRANSMITTED,
            )

        try:
            await radio.send_channel(
                request.channel_index,
                request.text,
                mark_transmitted,
            )
        except SendPolicyError as exc:
            failed = await store.transition_outbound(message.id, state=DeliveryState.FAILED)
            raise HTTPException(
                status_code=429,
                detail={"message_id": failed.id, "reason": str(exc)},
            ) from exc
        except ValueError as exc:
            failed = await store.transition_outbound(message.id, state=DeliveryState.FAILED)
            raise HTTPException(
                status_code=422,
                detail={"message_id": failed.id, "reason": str(exc)},
            ) from exc
        except Exception as exc:
            failed = await store.transition_outbound(message.id, state=DeliveryState.FAILED)
            raise HTTPException(
                status_code=503,
                detail={"message_id": failed.id, "reason": str(exc)},
            ) from exc

        return SendMessageResult(
            message_id=message.id,
            delivery_state=DeliveryState.TRANSMITTED,
            ack_code=None,
        )

    @app.get("/v1/repeaters/{public_key}/status", response_model=RepeaterStatus)
    async def repeater_status(
        public_key: str = Path(pattern=r"^[0-9A-Fa-f]{64}$"),
    ) -> RepeaterStatus:
        normalized_key = public_key.lower()
        await store.append_event(
            "repeater.status.requested",
            {"public_key": normalized_key},
        )
        try:
            result = await radio.request_repeater_status(normalized_key)
        except SendPolicyError as exc:
            await store.append_event(
                "repeater.status.rate_limited",
                {"public_key": normalized_key, "reason": str(exc)},
            )
            raise HTTPException(status_code=429, detail=str(exc)) from exc
        except ValueError as exc:
            await store.append_event(
                "repeater.status.rejected",
                {"public_key": normalized_key, "reason": str(exc)},
            )
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        except TimeoutError as exc:
            await store.append_event(
                "repeater.status.timed_out",
                {"public_key": normalized_key},
            )
            raise HTTPException(status_code=504, detail=str(exc)) from exc
        except Exception as exc:
            await store.append_event(
                "repeater.status.failed",
                {"public_key": normalized_key, "reason": str(exc)},
            )
            raise HTTPException(status_code=503, detail=str(exc)) from exc

        await store.append_event(
            "repeater.status.succeeded",
            result.model_dump(mode="json"),
        )
        return result

    @app.patch("/v1/repeaters/{public_key}/config")
    async def configure_repeater(
        public_key: str,
        request: RepeaterConfigRequest,
    ) -> dict[str, object]:
        raise HTTPException(
            status_code=501,
            detail={"public_key": public_key, "settings": request.settings},
        )

    return app


app = create_app()
