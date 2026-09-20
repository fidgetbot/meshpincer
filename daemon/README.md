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

When no serial override is set, hardware discovery will select and verify the
configured MeshCore companion node. Hardware integration is the next milestone.
