from __future__ import annotations

import json
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

import aiosqlite

from .models import ConsumerCursor, DeliveryState, EventRecord, InboundMessage, MessageRecord

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
    event_id INTEGER UNIQUE REFERENCES events(id),
    dedupe_key TEXT UNIQUE,
    direction TEXT NOT NULL CHECK (direction IN ('inbound', 'outbound')),
    kind TEXT NOT NULL CHECK (kind IN ('direct', 'channel')),
    peer_key TEXT,
    peer_key_prefix TEXT,
    channel_index INTEGER,
    text TEXT NOT NULL,
    mesh_timestamp INTEGER,
    snr REAL,
    path_length INTEGER,
    text_type INTEGER,
    recorded_at TEXT NOT NULL,
    delivery_state TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS consumer_cursors (
    consumer_id TEXT PRIMARY KEY,
    event_id INTEGER NOT NULL DEFAULT 0 CHECK (event_id >= 0),
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS messages_recorded_at_idx ON messages(recorded_at);
CREATE INDEX IF NOT EXISTS events_kind_idx ON events(kind);
"""

MESSAGE_MIGRATIONS = {
    "event_id": "ALTER TABLE messages ADD COLUMN event_id INTEGER REFERENCES events(id)",
    "dedupe_key": "ALTER TABLE messages ADD COLUMN dedupe_key TEXT",
    "peer_key_prefix": "ALTER TABLE messages ADD COLUMN peer_key_prefix TEXT",
    "snr": "ALTER TABLE messages ADD COLUMN snr REAL",
    "path_length": "ALTER TABLE messages ADD COLUMN path_length INTEGER",
    "text_type": "ALTER TABLE messages ADD COLUMN text_type INTEGER",
}

POST_MIGRATION_SCHEMA = """
CREATE UNIQUE INDEX IF NOT EXISTS messages_event_id_idx ON messages(event_id);
CREATE UNIQUE INDEX IF NOT EXISTS messages_dedupe_key_idx ON messages(dedupe_key);
"""


class Store:
    def __init__(self, path: Path) -> None:
        self.path = path

    async def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        async with aiosqlite.connect(self.path) as db:
            await db.executescript(SCHEMA)
            cursor = await db.execute("PRAGMA table_info(messages)")
            columns = {str(row[1]) for row in await cursor.fetchall()}
            for column, statement in MESSAGE_MIGRATIONS.items():
                if column not in columns:
                    await db.execute(statement)
            await db.executescript(POST_MIGRATION_SCHEMA)
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

    async def record_inbound(self, message: InboundMessage) -> tuple[MessageRecord, bool]:
        dedupe_key = _inbound_dedupe_key(message)
        recorded_at = datetime.now(UTC)
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            await db.execute("BEGIN IMMEDIATE")
            cursor = await db.execute(
                """
                INSERT OR IGNORE INTO messages(
                    event_id, dedupe_key, direction, kind, peer_key, peer_key_prefix,
                    channel_index, text, mesh_timestamp, snr, path_length, text_type,
                    recorded_at, delivery_state
                ) VALUES (NULL, ?, 'inbound', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    dedupe_key,
                    message.kind,
                    message.peer_key,
                    message.peer_key_prefix,
                    message.channel_index,
                    message.text,
                    message.mesh_timestamp,
                    message.snr,
                    message.path_length,
                    message.text_type,
                    recorded_at.isoformat(),
                    DeliveryState.RECEIVED.value,
                ),
            )
            inserted = cursor.rowcount == 1
            if inserted:
                if cursor.lastrowid is None:
                    raise RuntimeError("message insert did not return an id")
                message_id = int(cursor.lastrowid)
                event_payload = {
                    "message_id": message_id,
                    "direction": "inbound",
                    **message.model_dump(mode="json"),
                }
                event_cursor = await db.execute(
                    "INSERT INTO events(kind, payload_json, recorded_at) VALUES (?, ?, ?)",
                    (
                        "message.received",
                        json.dumps(event_payload, separators=(",", ":")),
                        recorded_at.isoformat(),
                    ),
                )
                if event_cursor.lastrowid is None:
                    raise RuntimeError("event insert did not return an id")
                event_id = int(event_cursor.lastrowid)
                await db.execute(
                    "UPDATE messages SET event_id = ? WHERE id = ?",
                    (event_id, message_id),
                )
            else:
                existing = await db.execute(
                    "SELECT * FROM messages WHERE dedupe_key = ?",
                    (dedupe_key,),
                )
                row = await existing.fetchone()
                if row is None:
                    raise RuntimeError("deduplicated message could not be read back")
                await db.commit()
                return _message_from_row(row), False

            existing = await db.execute("SELECT * FROM messages WHERE id = ?", (message_id,))
            row = await existing.fetchone()
            await db.commit()
        if row is None:
            raise RuntimeError("inserted message could not be read back")
        return _message_from_row(row), True

    async def list_messages(self, after_id: int, limit: int) -> list[MessageRecord]:
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                """
                SELECT id, event_id, direction, kind, peer_key, peer_key_prefix,
                       channel_index, text, mesh_timestamp, snr, path_length,
                       text_type, recorded_at, delivery_state
                FROM messages
                WHERE id > ?
                ORDER BY id ASC
                LIMIT ?
                """,
                (after_id, limit),
            )
            rows = await cursor.fetchall()
        return [_message_from_row(row) for row in rows]

    async def list_events(self, after_id: int, limit: int) -> list[EventRecord]:
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                """
                SELECT id, kind, payload_json, recorded_at
                FROM events
                WHERE id > ?
                ORDER BY id ASC
                LIMIT ?
                """,
                (after_id, limit),
            )
            rows = await cursor.fetchall()
        return [
            EventRecord(
                id=row["id"],
                kind=row["kind"],
                payload=json.loads(row["payload_json"]),
                recorded_at=datetime.fromisoformat(row["recorded_at"]),
            )
            for row in rows
        ]

    async def get_cursor(self, consumer_id: str) -> ConsumerCursor:
        async with aiosqlite.connect(self.path) as db:
            cursor = await db.execute(
                "SELECT event_id FROM consumer_cursors WHERE consumer_id = ?",
                (consumer_id,),
            )
            row = await cursor.fetchone()
        return ConsumerCursor(consumer_id=consumer_id, event_id=int(row[0]) if row else 0)

    async def advance_cursor(self, consumer_id: str, event_id: int) -> ConsumerCursor:
        updated_at = datetime.now(UTC).isoformat()
        async with aiosqlite.connect(self.path) as db:
            await db.execute("BEGIN IMMEDIATE")
            latest_cursor = await db.execute("SELECT COALESCE(MAX(id), 0) FROM events")
            latest_row = await latest_cursor.fetchone()
            latest = int(latest_row[0]) if latest_row else 0
            if event_id > latest:
                await db.rollback()
                raise ValueError(f"event_id {event_id} is beyond latest event {latest}")
            await db.execute(
                """
                INSERT INTO consumer_cursors(consumer_id, event_id, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(consumer_id) DO UPDATE SET
                    event_id = MAX(consumer_cursors.event_id, excluded.event_id),
                    updated_at = CASE
                        WHEN excluded.event_id >= consumer_cursors.event_id
                        THEN excluded.updated_at
                        ELSE consumer_cursors.updated_at
                    END
                """,
                (consumer_id, event_id, updated_at),
            )
            cursor = await db.execute(
                "SELECT event_id FROM consumer_cursors WHERE consumer_id = ?",
                (consumer_id,),
            )
            row = await cursor.fetchone()
            await db.commit()
        if row is None:
            raise RuntimeError("consumer cursor could not be read back")
        return ConsumerCursor(consumer_id=consumer_id, event_id=int(row[0]))


def _inbound_dedupe_key(message: InboundMessage) -> str:
    canonical = json.dumps(
        {
            "kind": message.kind,
            "peer_key": message.peer_key,
            "peer_key_prefix": message.peer_key_prefix,
            "channel_index": message.channel_index,
            "text": message.text,
            "mesh_timestamp": message.mesh_timestamp,
            "text_type": message.text_type,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return sha256(canonical.encode()).hexdigest()


def _message_from_row(row: aiosqlite.Row) -> MessageRecord:
    return MessageRecord(
        id=row["id"],
        event_id=row["event_id"],
        direction=row["direction"],
        kind=row["kind"],
        peer_key=row["peer_key"],
        peer_key_prefix=row["peer_key_prefix"],
        channel_index=row["channel_index"],
        text=row["text"],
        mesh_timestamp=row["mesh_timestamp"],
        snr=row["snr"],
        path_length=row["path_length"],
        text_type=row["text_type"],
        recorded_at=datetime.fromisoformat(row["recorded_at"]),
        delivery_state=DeliveryState(row["delivery_state"]),
    )
