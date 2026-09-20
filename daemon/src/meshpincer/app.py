from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query

from . import __version__
from .config import Settings
from .models import (
    MessageRecord,
    RadioStatus,
    RepeaterConfigRequest,
    SendMessageRequest,
    SendMessageResult,
    ServiceStatus,
)
from .store import Store


def create_app(settings: Settings | None = None) -> FastAPI:
    resolved = settings or Settings.from_env()
    store = Store(resolved.database_path)
    radio = RadioStatus(serial_port=resolved.serial_port)

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        resolved.state_dir.mkdir(parents=True, exist_ok=True)
        await store.initialize()
        yield

    app = FastAPI(title="MeshPincer", version=__version__, lifespan=lifespan)

    @app.get("/v1/status", response_model=ServiceStatus)
    async def status() -> ServiceStatus:
        return ServiceStatus(
            version=__version__,
            radio=radio,
            last_event_id=await store.last_event_id(),
        )

    @app.get("/v1/messages", response_model=list[MessageRecord])
    async def messages(
        after_id: int = Query(default=0, ge=0),
        limit: int = Query(default=100, ge=1, le=500),
    ) -> list[MessageRecord]:
        return await store.list_messages(after_id=after_id, limit=limit)

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
