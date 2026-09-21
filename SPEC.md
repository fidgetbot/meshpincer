# MeshPincer Specification

## Product definition

MeshPincer is built by an OpenClaw agent for other OpenClaw agents. It is a
MeshCore operator and messaging integration that lets an agent inspect
messages, send direct or channel messages, query a companion radio, administer
known repeater nodes, and participate in MeshCore as a native OpenClaw channel.

## Initial hardware target

- Seeed Studio Wio Tracker L1 Pro
- Official MeshCore USB companion-radio firmware
- Direct USB connection to an always-on macOS OpenClaw host

## Goals

- Maintain one reliable, reconnecting serial session with the companion radio.
- Persist inbound and outbound messages independently of OpenClaw sessions.
- Report delivery state as queued, transmitted, acknowledged, or timed out.
- Expose typed OpenClaw tools for status, inbox, sending, and repeater admin.
- Expose a native MeshCore channel from the same durable event stream.
- Identify contacts by MeshCore public key and channels by stable device slot.
- Keep channel secrets and repeater credentials out of ordinary application
  logs and SQLite records.

## Non-goals for the first milestone

- Acting as a MeshCore repeater or room server.
- MQTT or internet-hosted transport.
- A browser UI.
- Multi-radio clustering.
- General arbitrary remote CLI execution.

## Components

### `meshpincerd`

Python 3.12 daemon using `meshcore_py`, asyncio, FastAPI, and SQLite. It owns the
serial port, auto-fetches messages, maintains caches, tracks acknowledgements,
and exposes a versioned API over a Unix-domain socket.

### OpenClaw plugin

TypeScript plugin using the OpenClaw Plugin SDK and TypeBox. It registers these
operator-tool contracts:

- `meshcore_status`
- `meshcore_messages`
- `meshcore_send`
- `meshcore_repeater_status`
- `meshcore_repeater_configure`

The same package provides the `meshcore` messaging channel using the daemon API
and event cursor. Tool and channel delivery may be sequenced, but both are
current product scope.

## Local API

Initial endpoints:

- `GET /v1/status`
- `GET /v1/messages?after_id=<id>&limit=<n>`
- `POST /v1/messages/direct`
- `POST /v1/messages/channel`
- `GET /v1/repeaters/{public_key}/status`
- `PATCH /v1/repeaters/{public_key}/config`
- `GET /v1/events?after_id=<id>` (planned resumable stream)

The API is local-only. It must not bind a TCP listener by default.

## Persistence model

SQLite runs in WAL mode and records:

- normalized messages and delivery state;
- append-only events with monotonically increasing IDs;
- contact and channel cache snapshots;
- repeater operations and verified outcomes; and
- per-consumer cursors for unread/event replay behavior.

Secrets are references or device-held values, not message-database columns.

## Identity and display names

- The companion radio owns the cryptographic node identity and device/advert
  name. MeshPincer reads both from self-info after every connection or
  reconnect; neither is compiled into the daemon or plugin.
- Direct-message peers are normalized by public key, never by display name.
- Channel sender names are unverified application text. Outbound channel
  formatting uses an optional configured sender-label override and otherwise
  defaults to the name currently reported by the radio.
- Received channel sender labels are treated as untrusted display metadata and
  are never promoted to durable peer identity.
- Tests and examples use generic fixture values rather than product-owner or
  operator-specific names.

## Delivery milestones

These milestones describe implementation order, not optional or future product
scope. Operator tools and the native MeshCore channel are both part of
MeshPincer.

### M0 — repository scaffold

- Valid OpenClaw tool plugin package
- Python service and SQLite schema foundations
- Unix-socket API contract
- Unit tests and CI-ready commands

### M1 — hardware proof

- Discover and verify the Wio by USB identity and MeshCore public key
- Query radio status, contacts, and channel slots
- Receive and persist direct/channel messages
- Send test messages and correlate acknowledgements

Status as of 2026-09-20: USB discovery and read-only device, radio, contact,
channel, statistics, and GNSS queries have been proven on a Wio Tracker L1 Pro.
The Seattle network profile was applied and verified across a fresh serial
connection. Receive/persistence and RF send/acknowledgement acceptance remain
open. See [`docs/hardware-validation.md`](docs/hardware-validation.md).

### M2 — operator tools

- Exercise status, inbox, and send operations from Telegram through OpenClaw
- Add repeater status, telemetry, authenticated login, and validated settings
- Return explicit delivery and verification outcomes

### M3 — native channel

- Map MeshCore direct peers to OpenClaw DMs
- Map configured MeshCore channels to OpenClaw group conversations
- Resume inbound delivery from durable event cursors after restarts
- Apply LoRa-aware chunking and reply routing

## Verification requirements

- Unit tests use a fake transport and deterministic protocol events.
- Replay tests use captured frames with all secrets removed.
- Package validation uses `openclaw plugins validate` against built JavaScript.
- Hardware acceptance uses a second MeshCore node and a known repeater.
- Repeater configuration tests perform read/change/read-back verification.
