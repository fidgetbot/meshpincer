from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
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


async def test_same_protocol_message_over_different_paths_is_deduplicated(
    store: Store,
) -> None:
    direct = InboundMessage(
        kind="direct",
        peer_key="ab" * 32,
        peer_key_prefix="ab" * 6,
        text="same transmission",
        mesh_timestamp=5678,
        snr=12.0,
        path_length=0,
        text_type=0,
    )
    flood = direct.model_copy(update={"snr": 3.0, "path_length": 255})

    first, inserted = await store.record_inbound(direct)
    duplicate, duplicate_inserted = await store.record_inbound(flood)

    assert inserted
    assert not duplicate_inserted
    assert duplicate.id == first.id
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
        cursor = await db.execute("PRAGMA table_info(consumer_cursors)")
        cursor_columns = {str(row[1]) for row in await cursor.fetchall()}
        cursor = await db.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'housekeeping_state'"
        )
        housekeeping_table = await cursor.fetchone()

    assert {
        "event_id",
        "dedupe_key",
        "peer_key_prefix",
        "snr",
        "path_length",
        "text_type",
        "ack_code",
    } <= columns
    assert cursor_table is not None
    assert {"history_gap_before", "expired_at"} <= cursor_columns
    assert housekeeping_table is not None


async def test_outbound_delivery_transitions_are_durable(store: Store) -> None:
    queued = await store.queue_outbound_direct("ab" * 32, "one controlled send")
    transmitted = await store.transition_outbound(
        queued.id,
        DeliveryState.TRANSMITTED,
        ack_code="01020304",
    )
    acknowledged = await store.transition_outbound(
        queued.id,
        DeliveryState.ACKNOWLEDGED,
    )

    assert queued.delivery_state == DeliveryState.QUEUED
    assert transmitted.delivery_state == DeliveryState.TRANSMITTED
    assert transmitted.ack_code == "01020304"
    assert acknowledged.delivery_state == DeliveryState.ACKNOWLEDGED
    assert acknowledged.ack_code == "01020304"
    assert [event.kind for event in await store.list_events(0, 100)] == [
        "message.queued",
        "message.transmitted",
        "message.acknowledged",
    ]


async def test_outbound_delivery_rejects_invalid_transition(store: Store) -> None:
    queued = await store.queue_outbound_direct("ab" * 32, "one controlled send")

    with pytest.raises(ValueError, match="queued -> acknowledged"):
        await store.transition_outbound(queued.id, DeliveryState.ACKNOWLEDGED)


async def test_outbound_channel_transmit_is_durable_without_fake_ack(store: Store) -> None:
    queued = await store.queue_outbound_channel(3, "Fidget: private hello")
    transmitted = await store.transition_outbound(queued.id, DeliveryState.TRANSMITTED)

    assert queued.kind == "channel"
    assert queued.channel_index == 3
    assert queued.peer_key is None
    assert transmitted.delivery_state == DeliveryState.TRANSMITTED
    assert transmitted.ack_code is None
    assert [event.kind for event in await store.list_events(0, 100)] == [
        "message.queued",
        "message.transmitted",
    ]


async def test_housekeeping_prunes_only_events_consumed_by_active_cursors(store: Store) -> None:
    records = [
        await store.record_inbound(
            InboundMessage(kind="channel", channel_index=0, text=f"message {index}")
        )
        for index in range(3)
    ]
    messages = [record for record, _inserted in records]
    assert all(message.event_id is not None for message in messages)
    old = (datetime.now(UTC) - timedelta(days=60)).isoformat()
    async with aiosqlite.connect(store.path) as db:
        await db.execute("UPDATE messages SET recorded_at = ?", (old,))
        await db.execute("UPDATE events SET recorded_at = ?", (old,))
        await db.commit()
    await store.advance_cursor("active", int(messages[0].event_id or 0))

    result = await store.run_housekeeping(
        retention_days=30,
        max_messages=50_000,
        cursor_max_idle_days=30,
        interval_seconds=86_400,
        dry_run=False,
    )

    assert result.safe_through_event_id == messages[0].event_id
    assert result.messages_pruned == 1
    assert result.events_pruned == 1
    assert [message.text for message in await store.list_messages(0, 100)] == [
        "message 1",
        "message 2",
    ]
    assert (await store.get_cursor("active")).history_gap is False


async def test_housekeeping_expires_stale_cursor_and_reports_history_gap(store: Store) -> None:
    records = [
        await store.record_inbound(
            InboundMessage(kind="channel", channel_index=0, text=f"message {index}")
        )
        for index in range(3)
    ]
    messages = [record for record, _inserted in records]
    assert all(message.event_id is not None for message in messages)
    await store.advance_cursor("abandoned", int(messages[0].event_id or 0))
    old = (datetime.now(UTC) - timedelta(days=60)).isoformat()
    async with aiosqlite.connect(store.path) as db:
        await db.execute(
            "UPDATE events SET recorded_at = ? WHERE id <= ?",
            (old, messages[1].event_id),
        )
        await db.execute(
            "UPDATE messages SET recorded_at = ? WHERE event_id <= ?",
            (old, messages[1].event_id),
        )
        await db.execute(
            "UPDATE consumer_cursors SET updated_at = ? WHERE consumer_id = 'abandoned'",
            (old,),
        )
        await db.commit()

    result = await store.run_housekeeping(
        retention_days=30,
        max_messages=50_000,
        cursor_max_idle_days=30,
        interval_seconds=86_400,
        dry_run=False,
    )
    cursor = await store.get_cursor("abandoned")

    assert result.cursors_expired == 1
    assert result.prune_through_event_id == messages[1].event_id
    assert cursor.event_id == messages[1].event_id
    assert cursor.history_gap is True
    assert cursor.history_gap_before == int(messages[1].event_id or 0) + 1
    assert cursor.expired_at is not None
    assert [event.id for event in await store.list_events(cursor.event_id, 100)] == [
        messages[2].event_id
    ]
    cleared = await store.advance_cursor("abandoned", int(messages[2].event_id or 0))
    assert cleared.history_gap is False


async def test_housekeeping_enforces_message_cap_and_dry_run_is_non_mutating(
    store: Store,
) -> None:
    for index in range(5):
        await store.record_inbound(
            InboundMessage(kind="channel", channel_index=0, text=f"message {index}")
        )
    latest = await store.last_event_id()
    await store.advance_cursor("active", latest)

    preview = await store.run_housekeeping(
        retention_days=30,
        max_messages=2,
        cursor_max_idle_days=30,
        interval_seconds=86_400,
        dry_run=True,
    )
    assert preview.messages_pruned == 3
    assert len(await store.list_messages(0, 100)) == 5

    applied = await store.run_housekeeping(
        retention_days=30,
        max_messages=2,
        cursor_max_idle_days=30,
        interval_seconds=86_400,
        dry_run=False,
    )
    assert applied.messages_pruned == 3
    assert applied.events_pruned == 3
    assert applied.checkpointed
    assert [message.text for message in await store.list_messages(0, 100)] == [
        "message 3",
        "message 4",
    ]
    assert await store.last_event_id() == latest


async def test_new_consumer_reports_gap_after_history_was_pruned(store: Store) -> None:
    message, _ = await store.record_inbound(
        InboundMessage(kind="channel", channel_index=0, text="old")
    )
    assert message.event_id is not None
    old = (datetime.now(UTC) - timedelta(days=60)).isoformat()
    async with aiosqlite.connect(store.path) as db:
        await db.execute("UPDATE messages SET recorded_at = ?", (old,))
        await db.execute("UPDATE events SET recorded_at = ?", (old,))
        await db.commit()
    await store.run_housekeeping(
        retention_days=30,
        max_messages=50_000,
        cursor_max_idle_days=30,
        interval_seconds=86_400,
        dry_run=False,
    )

    cursor = await store.get_cursor("new-consumer")
    assert cursor.event_id == 0
    assert cursor.history_gap is True
    assert cursor.history_gap_before == message.event_id + 1
