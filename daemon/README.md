# MeshPincer daemon

`meshpincerd` is the local MeshCore radio owner and durable message service.

```bash
uv sync --dev
uv run meshpincerd
```

Configuration is supplied with environment variables:

- `MESHPINCER_STATE_DIR` — state and SQLite directory
- `MESHPINCER_SOCKET` — Unix-domain socket path
- `MESHPINCER_SERIAL_PORT` — optional explicit serial port override
- `MESHPINCER_USB_VID` and `MESHPINCER_USB_PID` — USB identity, accepting
  decimal or `0x` notation (defaults target the Wio Tracker L1)
- `MESHPINCER_USB_SERIAL` — optional hardware serial used to disambiguate
  multiple matching radios; it is not exposed by the API
- `MESHPINCER_QUERY_TIMEOUT` — companion command timeout in seconds
- `MESHPINCER_REFRESH_INTERVAL` — read-only snapshot interval in seconds
- `MESHPINCER_RECONNECT_INITIAL` and `MESHPINCER_RECONNECT_MAX` — reconnect
  backoff bounds in seconds
- `MESHPINCER_DIRECT_GLOBAL_COOLDOWN` and
  `MESHPINCER_DIRECT_PEER_COOLDOWN` — minimum seconds between direct sends
  globally and to the same peer (defaults: 5 and 30)
- `MESHPINCER_CHANNEL_SEND_ALLOWLIST` — comma-separated private channel slots
  permitted for transmission; empty by default, and slot 0 (`Public`) is always
  blocked
- `MESHPINCER_CHANNEL_GLOBAL_COOLDOWN` and
  `MESHPINCER_CHANNEL_PER_CHANNEL_COOLDOWN` — minimum seconds between channel
  floods globally and on the same slot (defaults: 30 and 300)
- `MESHPINCER_REPEATER_LOGIN_TIMEOUT` — bounded login response timeout
  (default: 15 seconds)
- `MESHPINCER_REPEATER_LOGIN_GLOBAL_COOLDOWN` and
  `MESHPINCER_REPEATER_LOGIN_PEER_COOLDOWN` — minimum seconds between login
  attempts globally and to the same repeater (defaults: 30 and 60)
- `MESHPINCER_RETENTION_DAYS` — full-history retention window (default: 30)
- `MESHPINCER_MAX_MESSAGES` — bounded message-row target (default: 50000)
- `MESHPINCER_CURSOR_MAX_IDLE_DAYS` — idle interval before a consumer cursor
  may be expired with an explicit history gap (default: 30)
- `MESHPINCER_HOUSEKEEPING_INTERVAL` — automatic cleanup interval in seconds
  (default: 86400)

When no serial override is set, discovery matches the configured USB identity
and fails closed if more than one radio matches without a configured hardware
serial.

Implemented endpoints:

- `GET /v1/status`
- `GET /v1/contacts`
- `GET /v1/channels` (configured channels only)
- `GET /v1/channels?include_empty=true` (all device slots)
- `GET /v1/messages?after_id=<id>&limit=<n>`
- `GET /v1/events?after_id=<id>&limit=<n>`
- `GET /v1/consumers/{consumer_id}/events?limit=<n>`
- `GET /v1/consumers/{consumer_id}/cursor`
- `PUT /v1/consumers/{consumer_id}/cursor`
- `POST /v1/database/housekeeping` (dry-run by default)
- `POST /v1/messages/direct`
- `POST /v1/messages/channel`
- `POST /v1/advertisements/local`
- `POST /v1/repeaters/{public_key}/login`
- `GET /v1/repeaters/{public_key}/status`

The daemon auto-fetches direct and channel messages while connected. It writes
each normalized inbound message and its `message.received` event atomically,
deduplicates protocol redelivery, and maintains independent monotonic cursors
for consumers such as operator tools and the native MeshCore channel. Consumers
advance their cursor after processing, so unacknowledged events replay after a
restart.

Housekeeping runs once at daemon startup and then daily. It prunes only data
already consumed by every active cursor, uses 1,000-event transactions, and
reports a durable history gap when an abandoned cursor is moved to the retained
boundary. `GET /v1/status` includes database and WAL sizes, row counts, oldest
retained timestamps, policy values, and last/next cleanup times. Cleanup
checkpoints/truncates the WAL after pruning and never runs `VACUUM` automatically.

Direct sends require a known contact with a learned route, accept at most 160
UTF-8 bytes, and perform one transmission with no retry or flood fallback. The
daemon persists `queued`, `transmitted`, and `acknowledged` or `timed_out`
delivery events, correlates the companion's expected ACK code, serializes
radio operations, and enforces global and per-peer cooldowns before
transmission.

The local contact-management API can set a known contact to a verified zero-hop
direct route or reset it to unknown/flood with
`PATCH /v1/contacts/{public_key}/route`. Route changes share the daemon's
serialized radio boundary and are immediately read back; arbitrary path bytes
are not accepted.

Channel sends require a configured private slot in the explicit daemon
allowlist. Slot 0 (`Public`) is read-only. A successful companion submission is
recorded as `transmitted`; MeshPincer never labels a channel broadcast
`acknowledged` because the protocol provides no end-to-end channel ACK.

Authenticate this companion to a known repeater without exposing the password
in shell history or process arguments:

```bash
uv run meshpincer-repeater-login <64-hex-repeater-public-key>
```

The CLI prompts with hidden input and submits the password only to the local
Unix-socket API. The daemon sends one login request, accepts success or rejection
only from the requested repeater prefix, applies 30-second global and 60-second
per-repeater cooldowns, and records only credential-free audit metadata.

For initial repeater registration, dual local advertisements, ACL permissions,
and bounded status verification, follow
[`docs/repeater-pairing.md`](../docs/repeater-pairing.md).

The API never returns passwords, channel secrets, USB hardware serials, or stored contact
coordinates.
