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
    EventRecord,
    MessageRecord,
    RepeaterConfigRequest,
    SendMessageRequest,
    SendMessageResult,
    ServiceStatus,
)
from .radio import RadioManager
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

    @app.get("/v1/channels", response_model=list[ChannelRecord])
    async def channels(include_empty: bool = False) -> list[ChannelRecord]:
        return await radio.channels(include_empty=include_empty)

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
        raise HTTPException(status_code=503, detail="radio integration is not configured")

    @app.post("/v1/messages/channel", response_model=SendMessageResult)
    async def send_channel(request: SendMessageRequest) -> SendMessageResult:
        if request.channel_index is None:
            raise HTTPException(status_code=422, detail="channel_index is required")
        raise HTTPException(status_code=503, detail="radio integration is not configured")

    @app.get("/v1/repeaters/{public_key}/status")
    async def repeater_status(public_key: str) -> dict[str, str]:
        raise HTTPException(
            status_code=501,
            detail=f"repeater status is not implemented for {public_key}",
        )

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
