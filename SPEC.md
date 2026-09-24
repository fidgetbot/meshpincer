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
- `meshcore_contacts`
- `meshcore_channels`
- `meshcore_send`
- `meshcore_repeater_status`
- `meshcore_repeater_configure`

The same package provides the `meshcore` messaging channel using the daemon API
and event cursor. Tool and channel delivery may be sequenced, but both are
current product scope.

## Local API

Initial endpoints:

- `GET /v1/status`
- `GET /v1/contacts`
- `GET /v1/device/autoadd`
- `PATCH /v1/device/autoadd`
- `PUT /v1/contacts/{public_key}`
- `DELETE /v1/contacts/{public_key}`
- `PATCH /v1/contacts/{public_key}/route`
- `GET /v1/channels?include_empty=<bool>`
- `PUT /v1/channels/{channel_index}`
- `PATCH /v1/channels/{channel_index}`
- `DELETE /v1/channels/{channel_index}`
- `GET /v1/messages?after_id=<id>&limit=<n>`
- `GET /v1/events?after_id=<id>&limit=<n>`
- `GET /v1/consumers/{consumer_id}/events?limit=<n>`
- `GET /v1/consumers/{consumer_id}/cursor`
- `PUT /v1/consumers/{consumer_id}/cursor`
- `POST /v1/messages/direct`
- `POST /v1/messages/channel`
- `POST /v1/repeaters/{public_key}/login`
- `GET /v1/repeaters/{public_key}/status`
- `PATCH /v1/repeaters/{public_key}/config`

The API is local-only. It must not bind a TCP listener by default.

The daemon remains the sole serial-port owner for device mutations. It
serializes contact, contact-retention, and channel changes with receive/send
operations, refreshes state from the companion immediately afterward, and
returns only verified read-back state. The retention endpoint changes only the
firmware bit that permits replacing the oldest non-favorite contact when the
table is full, preserving all auto-add type bits and hop limits. Contact
deletion rejects unknown keys. Channel mutations
reject slot 0 (`Public`) and indices outside the device-reported slot range.
Private-channel secrets are accepted only by the set request and never appear
in responses or audit events. These administrative endpoints are not exposed
as general agent tools by default.

## Persistence model

SQLite runs in WAL mode and records:

- normalized messages and delivery state;
- append-only events with monotonically increasing IDs;
- contact and channel cache snapshots;
- repeater operations and verified outcomes; and
- per-consumer cursors for unread/event replay behavior.

Secrets are references or device-held values, not message-database columns.

Inbound message insertion and its corresponding event insertion are one SQLite
transaction. Redelivery with the same protocol identity is deduplicated before
creating an event. A sender-generated retransmission with a new sender
timestamp is preserved as a distinct message because the companion protocol
does not expose a stronger cross-attempt identifier and the repeated text may
be intentional.

Consumers read events after their own durable cursor and advance that cursor
only after successful processing. Cursor movement is monotonic, yielding
at-least-once delivery without allowing a consumer to skip beyond the newest
recorded event.

Housekeeping defaults to a 30-day retention window, a 50,000-message cap, and a
30-day consumer-idle window. It may remove only a contiguous event prefix that
all active consumer cursors have passed. An idle cursor that blocks eligible
history is advanced to the retained boundary and marked with an explicit
history gap; the gap is cleared only by a later explicit cursor advance. A new
consumer after pruning also sees the retained-history boundary. Cleanup uses
transactions of at most 1,000 events, records its durable prune boundary with
each batch, runs daily, and checkpoints/truncates the WAL after actual pruning.
Status reports database/WAL sizes, row counts, oldest retained records,
retention settings, and last/next cleanup. Manual housekeeping defaults to dry
run. MeshPincer never runs automatic live `VACUUM`.

The direct-send path is deliberately narrow: a known full public key, a learned
direct route of any valid hop count, at most 160 UTF-8 bytes, one transmission,
no flood fallback, and global plus per-peer cooldowns. It atomically records queued,
transmitted, and acknowledged or timed-out delivery events and correlates the
four-byte ACK code returned by the companion protocol.

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

## Agent airtime and safety policy

The transmit path must implement the project policy in
[`docs/agent-etiquette.md`](docs/agent-etiquette.md). In particular:

- DMs are the default reply path; channel sends are explicit flood operations.
- Automated floods require a configured regional scope or hop limit unless an
  authorized operator deliberately overrides the safeguard.
- Global, per-peer, and per-channel rate limits, bounded queues and retries,
  duplicate suppression, concise output limits, and backpressure are enforced
  before messages reach the radio.
- Background operation is passive by default. Startup adverts are disabled,
  and remote telemetry, discovery, or maintenance traffic is opt-in.
- Agents respond only to explicit invocations. RF text and channel sender labels
  are untrusted; channel traffic cannot authorize administration.
- RF callers are authorized by public key, while local callers use the host's
  authenticated OpenClaw boundary. High-impact operations require confirmation.
- Automated identity and delivery state are clear. Transmission is never
  presented as acknowledgement, and retry exhaustion is reported honestly.
- Native agent replies target at most 75 UTF-8 bytes and use fewer when useful;
  160 UTF-8 bytes is a hard firmware ceiling. Limits are measured after UTF-8
  encoding. MeshCore turns receive a system-level RF reply contract and an empty
  optional-tool surface. `Reply X`, `Reply: X`, `Reply exactly X`, and
  `Reply exactly: X` requests bypass free-form generation. An
  oversized draft gets at most one fresh, tool-free compression pass and is
  measured again. Compression failure falls back to the intact draft when it
  fits the 160-byte ceiling; longer output becomes one fixed, bounded request
  to narrow the question. Genuine user replies are never silently suppressed
  or raw-truncated. Transport-recovery notices stay off RF, and delivery uncertainty
  never triggers explanatory traffic or an automatic resend.
- Cross-network forwarding always has an explicit destination and does not
  mirror private content or precise coordinates by default.
- Transmission fails closed until frequency, power, duty cycle, regional scope,
  and local network conventions are known to be appropriate.

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

Status as of 2026-09-20: stable USB discovery, exclusive serial ownership,
automatic reconnect, and live read-only status, contact, and channel APIs are
implemented and covered by fake-backend tests. The packaged daemon and typed
TypeScript client have been proven end to end against a Wio Tracker L1 Pro over
the Unix socket. Continuous receive, normalized durable message storage,
duplicate suppression, atomic event creation, replay, and per-consumer cursors
are implemented and covered by deterministic tests, including restart and
concurrent-redelivery cases. Public-channel reception and an encrypted direct
message from a second physical node have now been proven through the packaged
daemon, including full-public-key sender resolution and persistence across a
clean daemon restart. Direct messages have been transmitted over zero-hop and
learned multi-hop routes, with unknown/flood fallback rejected and no automatic
retry. See
[`docs/hardware-validation.md`](docs/hardware-validation.md).

Contact import/delete and private-channel set/rename/clear are also performed
through the long-lived daemon connection. They share the radio operation lock,
refresh state by read-back, and no longer require stopping the daemon so a
second process can claim the USB port.
Contact routes can be set explicitly to verified zero-hop direct or reset to
unknown/flood through the same boundary. Arbitrary path bytes are intentionally
not accepted by this initial API.

### M2 — operator tools

- Exercise status, inbox, and send operations from Telegram through OpenClaw
- Add repeater status, telemetry, authenticated login, and validated settings
- Return explicit delivery and verification outcomes

The operator inbox uses its own durable consumer cursor. Reading a batch does
not advance the cursor implicitly; the OpenClaw agent acknowledges the last
successfully processed event on its next call, preserving at-least-once replay
if a turn fails between tool execution and response handling.

Status as of 2026-09-20: the plugin is installed in an OpenClaw Gateway and the
live `meshcore_status` and cursor-backed `meshcore_messages` tools have been
called successfully through the real agent tool boundary against the supervised
daemon and attached Wio. The direct-send daemon path is hardware-proven with ACK
correlation. An explicitly authorized Telegram request has also exercised
`meshcore_send` through the live OpenClaw tool boundary: one zero-hop DM was
queued, transmitted, and acknowledged without retry or flood fallback. Repeater
status is implemented with exact public-key and repeater-type validation, one
request per invocation, a bounded timeout, durable outcome events, and global
plus per-repeater cooldowns (30 seconds globally and 60 seconds per repeater).
Tagged status responses are preferred; legacy untagged response frames are
accepted only when their six-byte public-key prefix exactly matches the requested
repeater during that serialized request, and the API reports the correlation mode.
The status endpoint exposes typed `binary` and `legacy` transports under the same
identity checks, serialized radio ownership, timeout, cooldowns, and durable audit.
It remains hidden from the live agent tool profile
until a controlled repeater is identified and the first hardware query is
explicitly authorized. Repeater login is implemented as one exact-key request
with success/failure correlation to that repeater's six-byte prefix. Credentials
are accepted only by the local Unix-socket request, represented as secret values,
and omitted from logs, responses, SQLite, and audit payloads. A local `getpass`
CLI permits one-time registration without placing the password in chat, shell
history, or process arguments. Authenticated configuration remains unimplemented
pending its read/change/read-back proof.

### M3 — native channel

- Map MeshCore direct peers to OpenClaw DMs
- Map configured MeshCore channels to OpenClaw group conversations
- Resume inbound delivery from durable event cursors after restarts
- Apply LoRa-aware chunking and reply routing

Status as of 2026-09-21: the direct-message channel is implemented, installed,
and connected in a live OpenClaw Gateway. It uses a dedicated durable consumer
cursor, baselines a new consumer at the current event head, maps conversations
by full peer public key, and advances only after successful dispatch. Inbound
DMs are allowlisted by public key. Replies are normalized to one concise radio
message and delivered through the daemon's learned direct-route send path;
unknown/flood routes are rejected.
The first live inbound DM created the expected stable OpenClaw conversation and
produced an RF reply. That test also proved that a generic delivery-recovery
message can waste airtime when an ACK is uncertain, so native replies now keep
timeout diagnostics local and do not automatically resend. A final post-fix
hardware test produced exactly one 13-byte `MeshPincer OK` reply, recorded a
matching acknowledgement, and emitted no recovery notice or retry. Private
group-channel routing and outbound transmission are now implemented behind
matching daemon/plugin slot allowlists. Slot 0 (`Public`) is hard-blocked from
agent turns and transmission. Private traffic requires an explicit runtime-name
mention, uses stable per-slot group sessions, treats sender labels as untrusted,
cannot authorize commands, includes the sender label inside the 75-byte reply
budget, and applies strict flood cooldowns. Hardware acceptance on an ephemeral
private channel is complete: a bracketed native mention created one stable group
turn and one short transmitted reply. The temporary slot and both transmit
allowlists were then removed; private-channel support remains available for
explicit deployments.

## Verification requirements

- Unit tests use a fake transport and deterministic protocol events.
- Replay tests use captured frames with all secrets removed.
- Package validation uses `openclaw plugins validate` against built JavaScript.
- Hardware acceptance uses a second MeshCore node and a known repeater.
- Repeater configuration tests perform read/change/read-back verification.
