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

When no serial override is set, discovery matches the configured USB identity
and fails closed if more than one radio matches without a configured hardware
serial.

Implemented read-only and replay endpoints:

- `GET /v1/status`
- `GET /v1/contacts`
- `GET /v1/channels` (configured channels only)
- `GET /v1/channels?include_empty=true` (all device slots)
- `GET /v1/messages?after_id=<id>&limit=<n>`
- `GET /v1/events?after_id=<id>&limit=<n>`
- `GET /v1/consumers/{consumer_id}/events?limit=<n>`
- `GET /v1/consumers/{consumer_id}/cursor`
- `PUT /v1/consumers/{consumer_id}/cursor`

The daemon auto-fetches direct and channel messages while connected. It writes
each normalized inbound message and its `message.received` event atomically,
deduplicates protocol redelivery, and maintains independent monotonic cursors
for consumers such as operator tools and the native MeshCore channel. Consumers
advance their cursor after processing, so unacknowledged events replay after a
restart.

The API never returns channel secrets, USB hardware serials, or stored contact
coordinates.
