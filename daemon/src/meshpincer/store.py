from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import aiosqlite

from .models import DeliveryState, MessageRecord

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    kind TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    recorded_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    direction TEXT NOT NULL CHECK (direction IN ('inbound', 'outbound')),
    kind TEXT NOT NULL CHECK (kind IN ('direct', 'channel')),
    peer_key TEXT,
    channel_index INTEGER,
    text TEXT NOT NULL,
    mesh_timestamp INTEGER,
    recorded_at TEXT NOT NULL,
    delivery_state TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS messages_recorded_at_idx ON messages(recorded_at);
CREATE INDEX IF NOT EXISTS events_kind_idx ON events(kind);
"""


class Store:
    def __init__(self, path: Path) -> None:
        self.path = path

    async def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        async with aiosqlite.connect(self.path) as db:
            await db.executescript(SCHEMA)
            await db.commit()

    async def last_event_id(self) -> int:
        async with aiosqlite.connect(self.path) as db:
            cursor = await db.execute("SELECT COALESCE(MAX(id), 0) FROM events")
            row = await cursor.fetchone()
        return int(row[0]) if row else 0

    async def append_event(self, kind: str, payload: dict[str, Any]) -> int:
        recorded_at = datetime.now(UTC).isoformat()
        async with aiosqlite.connect(self.path) as db:
            cursor = await db.execute(
                "INSERT INTO events(kind, payload_json, recorded_at) VALUES (?, ?, ?)",
                (kind, json.dumps(payload, separators=(",", ":")), recorded_at),
            )
            await db.commit()
            if cursor.lastrowid is None:
                raise RuntimeError("event insert did not return an id")
            return int(cursor.lastrowid)

    async def list_messages(self, after_id: int, limit: int) -> list[MessageRecord]:
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                """
                SELECT id, direction, kind, peer_key, channel_index, text,
                       mesh_timestamp, recorded_at, delivery_state
                FROM messages
                WHERE id > ?
                ORDER BY id ASC
                LIMIT ?
                """,
                (after_id, limit),
            )
            rows = await cursor.fetchall()
        return [
            MessageRecord(
                id=row["id"],
                direction=row["direction"],
                kind=row["kind"],
                peer_key=row["peer_key"],
                channel_index=row["channel_index"],
                text=row["text"],
                mesh_timestamp=row["mesh_timestamp"],
                recorded_at=datetime.fromisoformat(row["recorded_at"]),
                delivery_state=DeliveryState(row["delivery_state"]),
            )
            for row in rows
        ]
