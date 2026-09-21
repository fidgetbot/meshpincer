from __future__ import annotations

import asyncio
from pathlib import Path

import aiosqlite
import pytest

from meshpincer.models import DeliveryState, InboundMessage
from meshpincer.store import Store


@pytest.fixture
async def store(tmp_path: Path) -> Store:
    result = Store(tmp_path / "meshpincer.db")
    await result.initialize()
    return result


async def test_inbound_message_and_event_are_atomic_and_deduplicated(store: Store) -> None:
    inbound = InboundMessage(
        kind="direct",
        peer_key="ab" * 32,
        peer_key_prefix="ab" * 6,
        text="hello",
        mesh_timestamp=1234,
        snr=5.25,
        path_length=2,
        text_type=0,
    )

    first, inserted = await store.record_inbound(inbound)
    duplicate, duplicate_inserted = await store.record_inbound(inbound)
    messages = await store.list_messages(after_id=0, limit=100)
    events = await store.list_events(after_id=0, limit=100)

    assert inserted
    assert not duplicate_inserted
    assert duplicate.id == first.id
    assert duplicate.event_id == first.event_id
    assert len(messages) == 1
    assert messages[0].delivery_state == DeliveryState.RECEIVED
    assert messages[0].snr == 5.25
    assert len(events) == 1
    assert events[0].kind == "message.received"
    assert events[0].payload["message_id"] == first.id
    assert events[0].payload["peer_key"] == "ab" * 32


async def test_concurrent_duplicate_callbacks_create_one_event(store: Store) -> None:
    inbound = InboundMessage(
        kind="channel",
        channel_index=0,
        text="Agent: concise update",
        mesh_timestamp=5678,
        snr=3.0,
        path_length=1,
        text_type=0,
    )

    results = await asyncio.gather(*(store.record_inbound(inbound) for _ in range(5)))

    assert sum(inserted for _message, inserted in results) == 1
    assert len(await store.list_messages(after_id=0, limit=100)) == 1
    assert len(await store.list_events(after_id=0, limit=100)) == 1


async def test_consumer_cursor_is_durable_monotonic_and_bounded(
    store: Store,
    tmp_path: Path,
) -> None:
    first, _ = await store.record_inbound(
        InboundMessage(kind="channel", channel_index=0, text="one", mesh_timestamp=1)
    )
    second, _ = await store.record_inbound(
        InboundMessage(kind="channel", channel_index=0, text="two", mesh_timestamp=2)
    )
    assert first.event_id is not None
    assert second.event_id is not None

    assert (await store.get_cursor("native-channel")).event_id == 0
    cursor = await store.advance_cursor("native-channel", first.event_id)
    assert cursor.event_id == first.event_id
    cursor = await store.advance_cursor("native-channel", 0)
    assert cursor.event_id == first.event_id
    assert [event.id for event in await store.list_events(cursor.event_id, 100)] == [
        second.event_id
    ]

    restarted = Store(tmp_path / "meshpincer.db")
    await restarted.initialize()
    assert (await restarted.get_cursor("native-channel")).event_id == first.event_id
    with pytest.raises(ValueError, match="beyond latest event"):
        await restarted.advance_cursor("native-channel", second.event_id + 1)


async def test_initialize_migrates_the_original_message_schema(tmp_path: Path) -> None:
    database = tmp_path / "legacy.db"
    async with aiosqlite.connect(database) as db:
        await db.executescript(
            """
            CREATE TABLE events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                kind TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                recorded_at TEXT NOT NULL
            );
            CREATE TABLE messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                direction TEXT NOT NULL,
                kind TEXT NOT NULL,
                peer_key TEXT,
                channel_index INTEGER,
                text TEXT NOT NULL,
                mesh_timestamp INTEGER,
                recorded_at TEXT NOT NULL,
                delivery_state TEXT NOT NULL
            );
            """
        )
        await db.commit()

    store = Store(database)
    await store.initialize()

    async with aiosqlite.connect(database) as db:
        cursor = await db.execute("PRAGMA table_info(messages)")
        columns = {str(row[1]) for row in await cursor.fetchall()}
        cursor = await db.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'consumer_cursors'"
        )
        cursor_table = await cursor.fetchone()

    assert {
        "event_id",
        "dedupe_key",
        "peer_key_prefix",
        "snr",
        "path_length",
        "text_type",
    } <= columns
    assert cursor_table is not None
