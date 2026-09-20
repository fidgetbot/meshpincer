# Local API

`meshpincerd` exposes its versioned HTTP API over a Unix-domain socket. The
FastAPI application is the source of truth for the OpenAPI document; clients
should treat `/v1` as the compatibility boundary.

During development, an OpenAPI document can be inspected without opening a
network listener:

```bash
cd daemon
uv run python -c 'import json; from meshpincer.app import app; print(json.dumps(app.openapi(), indent=2))'
```

The socket defaults to the platform state directory documented in
`daemon/README.md`. No TCP listener is enabled by default.
