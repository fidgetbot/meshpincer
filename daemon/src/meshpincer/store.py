from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Any

import aiosqlite

from .models import (
    ConsumerCursor,
    DatabaseStatus,
    DeliveryState,
    EventRecord,
    HousekeepingResult,
    InboundMessage,
    MessageRecord,
)

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
    ack_code TEXT,
    recorded_at TEXT NOT NULL,
    delivery_state TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS consumer_cursors (
    consumer_id TEXT PRIMARY KEY,
    event_id INTEGER NOT NULL DEFAULT 0 CHECK (event_id >= 0),
    updated_at TEXT NOT NULL,
    history_gap_before INTEGER,
    expired_at TEXT
);

CREATE TABLE IF NOT EXISTS housekeeping_state (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    pruned_through_event_id INTEGER NOT NULL DEFAULT 0,
    last_run_at TEXT,
    next_run_at TEXT
);

CREATE INDEX IF NOT EXISTS messages_recorded_at_idx ON messages(recorded_at);
CREATE INDEX IF NOT EXISTS events_kind_idx ON events(kind);
CREATE INDEX IF NOT EXISTS events_recorded_at_idx ON events(recorded_at);
CREATE INDEX IF NOT EXISTS consumer_cursors_updated_at_idx ON consumer_cursors(updated_at);
"""

MESSAGE_MIGRATIONS = {
    "event_id": "ALTER TABLE messages ADD COLUMN event_id INTEGER REFERENCES events(id)",
    "dedupe_key": "ALTER TABLE messages ADD COLUMN dedupe_key TEXT",
    "peer_key_prefix": "ALTER TABLE messages ADD COLUMN peer_key_prefix TEXT",
    "snr": "ALTER TABLE messages ADD COLUMN snr REAL",
    "path_length": "ALTER TABLE messages ADD COLUMN path_length INTEGER",
    "text_type": "ALTER TABLE messages ADD COLUMN text_type INTEGER",
    "ack_code": "ALTER TABLE messages ADD COLUMN ack_code TEXT",
}

POST_MIGRATION_SCHEMA = """
CREATE UNIQUE INDEX IF NOT EXISTS messages_event_id_idx ON messages(event_id);
CREATE UNIQUE INDEX IF NOT EXISTS messages_dedupe_key_idx ON messages(dedupe_key);
"""

CURSOR_MIGRATIONS = {
    "history_gap_before": "ALTER TABLE consumer_cursors ADD COLUMN history_gap_before INTEGER",
    "expired_at": "ALTER TABLE consumer_cursors ADD COLUMN expired_at TEXT",
}


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
            cursor = await db.execute("PRAGMA table_info(consumer_cursors)")
            cursor_columns = {str(row[1]) for row in await cursor.fetchall()}
            for column, statement in CURSOR_MIGRATIONS.items():
                if column not in cursor_columns:
                    await db.execute(statement)
            await db.executescript(POST_MIGRATION_SCHEMA)
            await db.commit()

    async def last_event_id(self) -> int:
        async with aiosqlite.connect(self.path) as db:
            cursor = await db.execute(
                """
                SELECT MAX(
                    COALESCE((SELECT MAX(id) FROM events), 0),
                    COALESCE((
                        SELECT pruned_through_event_id
                        FROM housekeeping_state
                        WHERE singleton = 1
                    ), 0)
                )
                """
            )
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
                       text_type, ack_code, recorded_at, delivery_state
                FROM messages
                WHERE id > ?
                ORDER BY id ASC
                LIMIT ?
                """,
                (after_id, limit),
            )
            rows = await cursor.fetchall()
        return [_message_from_row(row) for row in rows]

    async def queue_outbound_direct(self, public_key: str, text: str) -> MessageRecord:
        recorded_at = datetime.now(UTC)
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            await db.execute("BEGIN IMMEDIATE")
            cursor = await db.execute(
                """
                INSERT INTO messages(
                    event_id, dedupe_key, direction, kind, peer_key, peer_key_prefix,
                    channel_index, text, mesh_timestamp, snr, path_length, text_type,
                    ack_code, recorded_at, delivery_state
                ) VALUES (NULL, NULL, 'outbound', 'direct', ?, ?, NULL, ?, NULL, NULL,
                          NULL, 0, NULL, ?, ?)
                """,
                (
                    public_key.lower(),
                    public_key[:12].lower(),
                    text,
                    recorded_at.isoformat(),
                    DeliveryState.QUEUED.value,
                ),
            )
            if cursor.lastrowid is None:
                raise RuntimeError("outbound message insert did not return an id")
            message_id = int(cursor.lastrowid)
            event_id = await _insert_delivery_event(
                db,
                message_id=message_id,
                state=DeliveryState.QUEUED,
                recorded_at=recorded_at,
            )
            await db.execute(
                "UPDATE messages SET event_id = ? WHERE id = ?",
                (event_id, message_id),
            )
            cursor = await db.execute("SELECT * FROM messages WHERE id = ?", (message_id,))
            row = await cursor.fetchone()
            await db.commit()
        if row is None:
            raise RuntimeError("queued outbound message could not be read back")
        return _message_from_row(row)

    async def queue_outbound_channel(self, channel_index: int, text: str) -> MessageRecord:
        recorded_at = datetime.now(UTC)
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            await db.execute("BEGIN IMMEDIATE")
            cursor = await db.execute(
                """
                INSERT INTO messages(
                    event_id, dedupe_key, direction, kind, peer_key, peer_key_prefix,
                    channel_index, text, mesh_timestamp, snr, path_length, text_type,
                    ack_code, recorded_at, delivery_state
                ) VALUES (NULL, NULL, 'outbound', 'channel', NULL, NULL, ?, ?, NULL, NULL,
                          NULL, 0, NULL, ?, ?)
                """,
                (
                    channel_index,
                    text,
                    recorded_at.isoformat(),
                    DeliveryState.QUEUED.value,
                ),
            )
            if cursor.lastrowid is None:
                raise RuntimeError("outbound channel message insert did not return an id")
            message_id = int(cursor.lastrowid)
            event_id = await _insert_delivery_event(
                db,
                message_id=message_id,
                state=DeliveryState.QUEUED,
                recorded_at=recorded_at,
            )
            await db.execute(
                "UPDATE messages SET event_id = ? WHERE id = ?",
                (event_id, message_id),
            )
            cursor = await db.execute("SELECT * FROM messages WHERE id = ?", (message_id,))
            row = await cursor.fetchone()
            await db.commit()
        if row is None:
            raise RuntimeError("queued outbound channel message could not be read back")
        return _message_from_row(row)

    async def transition_outbound(
        self,
        message_id: int,
        state: DeliveryState,
        *,
        ack_code: str | None = None,
    ) -> MessageRecord:
        allowed = {
            DeliveryState.QUEUED: {DeliveryState.TRANSMITTED, DeliveryState.FAILED},
            DeliveryState.TRANSMITTED: {
                DeliveryState.ACKNOWLEDGED,
                DeliveryState.TIMED_OUT,
                DeliveryState.FAILED,
            },
        }
        recorded_at = datetime.now(UTC)
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            await db.execute("BEGIN IMMEDIATE")
            cursor = await db.execute(
                "SELECT * FROM messages WHERE id = ? AND direction = 'outbound'",
                (message_id,),
            )
            row = await cursor.fetchone()
            if row is None:
                await db.rollback()
                raise ValueError(f"outbound message {message_id} was not found")
            current = DeliveryState(row["delivery_state"])
            if state not in allowed.get(current, set()):
                await db.rollback()
                raise ValueError(f"invalid outbound transition: {current.value} -> {state.value}")
            resolved_ack = ack_code or row["ack_code"]
            await db.execute(
                "UPDATE messages SET delivery_state = ?, ack_code = ? WHERE id = ?",
                (state.value, resolved_ack, message_id),
            )
            event_id = await _insert_delivery_event(
                db,
                message_id=message_id,
                state=state,
                recorded_at=recorded_at,
                ack_code=resolved_ack,
            )
            await db.execute(
                "UPDATE messages SET event_id = ? WHERE id = ?",
                (event_id, message_id),
            )
            cursor = await db.execute("SELECT * FROM messages WHERE id = ?", (message_id,))
            updated = await cursor.fetchone()
            await db.commit()
        if updated is None:
            raise RuntimeError("updated outbound message could not be read back")
        return _message_from_row(updated)

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
            await db.execute("BEGIN IMMEDIATE")
            state_cursor = await db.execute(
                "SELECT pruned_through_event_id FROM housekeeping_state WHERE singleton = 1"
            )
            state_row = await state_cursor.fetchone()
            pruned_through = int(state_row[0]) if state_row else 0
            updated_at = datetime.now(UTC).isoformat()
            await db.execute(
                """
                INSERT OR IGNORE INTO consumer_cursors(
                    consumer_id, event_id, updated_at, history_gap_before, expired_at
                ) VALUES (?, 0, ?, ?, NULL)
                """,
                (
                    consumer_id,
                    updated_at,
                    pruned_through + 1 if pruned_through > 0 else None,
                ),
            )
            await db.execute(
                "UPDATE consumer_cursors SET updated_at = ? WHERE consumer_id = ?",
                (updated_at, consumer_id),
            )
            cursor = await db.execute(
                """
                SELECT event_id, history_gap_before, expired_at
                FROM consumer_cursors
                WHERE consumer_id = ?
                """,
                (consumer_id,),
            )
            row = await cursor.fetchone()
            await db.commit()
        if row is None:
            raise RuntimeError("consumer cursor could not be created")
        gap_before = int(row[1]) if row[1] is not None else None
        return ConsumerCursor(
            consumer_id=consumer_id,
            event_id=int(row[0]),
            history_gap=gap_before is not None,
            history_gap_before=gap_before,
            expired_at=datetime.fromisoformat(row[2]) if row[2] else None,
        )

    async def advance_cursor(self, consumer_id: str, event_id: int) -> ConsumerCursor:
        updated_at = datetime.now(UTC).isoformat()
        async with aiosqlite.connect(self.path) as db:
            await db.execute("BEGIN IMMEDIATE")
            latest_cursor = await db.execute(
                """
                SELECT MAX(
                    COALESCE((SELECT MAX(id) FROM events), 0),
                    COALESCE((
                        SELECT pruned_through_event_id
                        FROM housekeeping_state
                        WHERE singleton = 1
                    ), 0)
                )
                """
            )
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
                    END,
                    history_gap_before = CASE
                        WHEN excluded.event_id >= consumer_cursors.event_id THEN NULL
                        ELSE consumer_cursors.history_gap_before
                    END,
                    expired_at = CASE
                        WHEN excluded.event_id >= consumer_cursors.event_id THEN NULL
                        ELSE consumer_cursors.expired_at
                    END
                """,
                (consumer_id, event_id, updated_at),
            )
            cursor = await db.execute(
                """
                SELECT event_id, history_gap_before, expired_at
                FROM consumer_cursors
                WHERE consumer_id = ?
                """,
                (consumer_id,),
            )
            row = await cursor.fetchone()
            await db.commit()
        if row is None:
            raise RuntimeError("consumer cursor could not be read back")
        gap_before = int(row[1]) if row[1] is not None else None
        return ConsumerCursor(
            consumer_id=consumer_id,
            event_id=int(row[0]),
            history_gap=gap_before is not None,
            history_gap_before=gap_before,
            expired_at=datetime.fromisoformat(row[2]) if row[2] else None,
        )

    async def database_status(
        self,
        *,
        retention_days: int,
        max_messages: int,
        cursor_max_idle_days: int,
    ) -> DatabaseStatus:
        async with aiosqlite.connect(self.path) as db:
            cursor = await db.execute(
                """
                SELECT
                    (SELECT COUNT(*) FROM messages),
                    (SELECT COUNT(*) FROM events),
                    (SELECT COUNT(*) FROM consumer_cursors),
                    (SELECT MIN(recorded_at) FROM messages),
                    (SELECT MIN(recorded_at) FROM events),
                    (SELECT MIN(id) FROM events),
                    (SELECT pruned_through_event_id FROM housekeeping_state WHERE singleton = 1),
                    (SELECT last_run_at FROM housekeeping_state WHERE singleton = 1),
                    (SELECT next_run_at FROM housekeeping_state WHERE singleton = 1)
                """
            )
            row = await cursor.fetchone()
        database_bytes = self.path.stat().st_size if self.path.exists() else 0
        wal_path = Path(f"{self.path}-wal")
        wal_bytes = wal_path.stat().st_size if wal_path.exists() else 0
        assert row is not None
        return DatabaseStatus(
            database_bytes=database_bytes,
            wal_bytes=wal_bytes,
            message_count=int(row[0]),
            event_count=int(row[1]),
            consumer_count=int(row[2]),
            oldest_message_at=datetime.fromisoformat(row[3]) if row[3] else None,
            oldest_event_at=datetime.fromisoformat(row[4]) if row[4] else None,
            earliest_available_event_id=int(row[5]) if row[5] is not None else None,
            pruned_through_event_id=int(row[6]) if row[6] is not None else 0,
            last_housekeeping_at=datetime.fromisoformat(row[7]) if row[7] else None,
            next_housekeeping_at=datetime.fromisoformat(row[8]) if row[8] else None,
            retention_days=retention_days,
            max_messages=max_messages,
            cursor_max_idle_days=cursor_max_idle_days,
        )

    async def run_housekeeping(
        self,
        *,
        retention_days: int,
        max_messages: int,
        cursor_max_idle_days: int,
        interval_seconds: float,
        dry_run: bool,
        now: datetime | None = None,
    ) -> HousekeepingResult:
        current = now or datetime.now(UTC)
        retention_before = (current - timedelta(days=retention_days)).isoformat()
        active_after = (current - timedelta(days=cursor_max_idle_days)).isoformat()
        completed_at = current.isoformat()
        next_run_at = (current + timedelta(seconds=interval_seconds)).isoformat()

        async with aiosqlite.connect(self.path) as db:
            await db.execute("PRAGMA foreign_keys=ON")
            await db.execute("BEGIN IMMEDIATE")
            latest_cursor = await db.execute(
                """
                SELECT MAX(
                    COALESCE((SELECT MAX(id) FROM events), 0),
                    COALESCE((
                        SELECT pruned_through_event_id
                        FROM housekeeping_state
                        WHERE singleton = 1
                    ), 0)
                )
                """
            )
            latest_row = await latest_cursor.fetchone()
            latest = int(latest_row[0]) if latest_row else 0

            active_cursor = await db.execute(
                "SELECT MIN(event_id) FROM consumer_cursors WHERE updated_at >= ?",
                (active_after,),
            )
            active_row = await active_cursor.fetchone()
            safe_through = (
                int(active_row[0]) if active_row and active_row[0] is not None else latest
            )

            age_cursor = await db.execute(
                "SELECT MIN(id) FROM events WHERE id <= ? AND recorded_at >= ?",
                (safe_through, retention_before),
            )
            age_row = await age_cursor.fetchone()
            age_cutoff = int(age_row[0]) - 1 if age_row and age_row[0] is not None else safe_through

            count_cursor = await db.execute("SELECT COUNT(*) FROM messages")
            count_row = await count_cursor.fetchone()
            message_count = int(count_row[0]) if count_row else 0
            overflow = max(message_count - max_messages, 0)
            cap_cutoff = 0
            if overflow:
                eligible_cursor = await db.execute(
                    """
                    SELECT COUNT(*)
                    FROM messages
                    WHERE event_id IS NOT NULL AND event_id <= ?
                    """,
                    (safe_through,),
                )
                eligible_row = await eligible_cursor.fetchone()
                eligible = int(eligible_row[0]) if eligible_row else 0
                prune_count = min(overflow, eligible)
                if prune_count:
                    cap_cursor = await db.execute(
                        """
                        SELECT event_id
                        FROM messages
                        WHERE event_id IS NOT NULL AND event_id <= ?
                        ORDER BY event_id ASC
                        LIMIT 1 OFFSET ?
                        """,
                        (safe_through, prune_count - 1),
                    )
                    cap_row = await cap_cursor.fetchone()
                    cap_cutoff = int(cap_row[0]) if cap_row else 0

            prune_through = min(max(age_cutoff, cap_cutoff), safe_through)
            messages_cursor = await db.execute(
                "SELECT COUNT(*) FROM messages WHERE event_id IS NOT NULL AND event_id <= ?",
                (prune_through,),
            )
            messages_row = await messages_cursor.fetchone()
            messages_pruned = int(messages_row[0]) if messages_row else 0
            events_cursor = await db.execute(
                "SELECT COUNT(*) FROM events WHERE id <= ?",
                (prune_through,),
            )
            events_row = await events_cursor.fetchone()
            events_pruned = int(events_row[0]) if events_row else 0
            expired_cursor = await db.execute(
                """
                SELECT COUNT(*)
                FROM consumer_cursors
                WHERE updated_at < ? AND event_id < ?
                """,
                (active_after, prune_through),
            )
            expired_row = await expired_cursor.fetchone()
            cursors_expired = int(expired_row[0]) if expired_row else 0

            if dry_run:
                await db.rollback()
            else:
                await db.rollback()
                messages_pruned = 0
                events_pruned = 0
                while prune_through > 0:
                    await db.execute("BEGIN IMMEDIATE")
                    batch_cursor = await db.execute(
                        "SELECT id FROM events WHERE id <= ? ORDER BY id ASC LIMIT 1000",
                        (prune_through,),
                    )
                    batch_rows = await batch_cursor.fetchall()
                    if not batch_rows:
                        await db.rollback()
                        break
                    batch_through = int(batch_rows[-1][0])
                    deleted_messages = await db.execute(
                        "DELETE FROM messages WHERE event_id IS NOT NULL AND event_id <= ?",
                        (batch_through,),
                    )
                    deleted_events = await db.execute(
                        "DELETE FROM events WHERE id <= ?",
                        (batch_through,),
                    )
                    await db.execute(
                        """
                        UPDATE consumer_cursors
                        SET event_id = ?, history_gap_before = ?, expired_at = ?
                        WHERE updated_at < ? AND event_id < ?
                        """,
                        (
                            batch_through,
                            batch_through + 1,
                            completed_at,
                            active_after,
                            batch_through,
                        ),
                    )
                    await db.execute(
                        """
                        INSERT INTO housekeeping_state(
                            singleton, pruned_through_event_id, last_run_at, next_run_at
                        ) VALUES (1, ?, ?, ?)
                        ON CONFLICT(singleton) DO UPDATE SET
                            pruned_through_event_id = MAX(
                                housekeeping_state.pruned_through_event_id,
                                excluded.pruned_through_event_id
                            ),
                            last_run_at = excluded.last_run_at,
                            next_run_at = excluded.next_run_at
                        """,
                        (batch_through, completed_at, next_run_at),
                    )
                    await db.commit()
                    messages_pruned += max(deleted_messages.rowcount, 0)
                    events_pruned += max(deleted_events.rowcount, 0)

                await db.execute("BEGIN IMMEDIATE")
                await db.execute(
                    """
                    INSERT INTO housekeeping_state(
                        singleton, pruned_through_event_id, last_run_at, next_run_at
                    ) VALUES (1, ?, ?, ?)
                    ON CONFLICT(singleton) DO UPDATE SET
                        pruned_through_event_id = MAX(
                            housekeeping_state.pruned_through_event_id,
                            excluded.pruned_through_event_id
                        ),
                        last_run_at = excluded.last_run_at,
                        next_run_at = excluded.next_run_at
                    """,
                    (prune_through, completed_at, next_run_at),
                )
                await db.commit()

        checkpointed = False
        if not dry_run and (messages_pruned or events_pruned):
            async with aiosqlite.connect(self.path) as checkpoint_db:
                checkpoint_cursor = await checkpoint_db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                checkpoint_row = await checkpoint_cursor.fetchone()
            checkpointed = bool(checkpoint_row and int(checkpoint_row[0]) == 0)

        return HousekeepingResult(
            dry_run=dry_run,
            safe_through_event_id=safe_through,
            prune_through_event_id=prune_through,
            messages_pruned=messages_pruned,
            events_pruned=events_pruned,
            cursors_expired=cursors_expired,
            checkpointed=checkpointed,
            completed_at=current,
        )


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


async def _insert_delivery_event(
    db: aiosqlite.Connection,
    *,
    message_id: int,
    state: DeliveryState,
    recorded_at: datetime,
    ack_code: str | None = None,
) -> int:
    payload: dict[str, Any] = {
        "message_id": message_id,
        "direction": "outbound",
        "delivery_state": state.value,
    }
    if ack_code is not None:
        payload["ack_code"] = ack_code
    cursor = await db.execute(
        "INSERT INTO events(kind, payload_json, recorded_at) VALUES (?, ?, ?)",
        (
            f"message.{state.value}",
            json.dumps(payload, separators=(",", ":")),
            recorded_at.isoformat(),
        ),
    )
    if cursor.lastrowid is None:
        raise RuntimeError("delivery event insert did not return an id")
    return int(cursor.lastrowid)


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
        ack_code=row["ack_code"],
        recorded_at=datetime.fromisoformat(row["recorded_at"]),
        delivery_state=DeliveryState(row["delivery_state"]),
    )
