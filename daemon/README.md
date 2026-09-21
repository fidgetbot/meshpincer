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
- `POST /v1/messages/direct`
- `POST /v1/messages/channel`

The daemon auto-fetches direct and channel messages while connected. It writes
each normalized inbound message and its `message.received` event atomically,
deduplicates protocol redelivery, and maintains independent monotonic cursors
for consumers such as operator tools and the native MeshCore channel. Consumers
advance their cursor after processing, so unacknowledged events replay after a
restart.

Direct sends require a known contact with a zero-hop route, accept at most 160
characters, and perform one transmission with no retry or flood fallback. The
daemon persists `queued`, `transmitted`, and `acknowledged` or `timed_out`
delivery events, correlates the companion's expected ACK code, serializes
radio operations, and enforces global and per-peer cooldowns before
transmission.

Channel sends require a configured private slot in the explicit daemon
allowlist. Slot 0 (`Public`) is read-only. A successful companion submission is
recorded as `transmitted`; MeshPincer never labels a channel broadcast
`acknowledged` because the protocol provides no end-to-end channel ACK.

The API never returns channel secrets, USB hardware serials, or stored contact
coordinates.
