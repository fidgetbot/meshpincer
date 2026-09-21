# MeshPincer OpenClaw plugin

OpenClaw operator tools for the local MeshPincer MeshCore radio service.

The plugin connects to `meshpincerd` over a Unix-domain socket. Configure a
non-default path under `plugins.entries.meshpincer.config.socketPath`.

The typed client supports durable event replay and cursor advancement over the
same socket. Each OpenClaw surface uses its own consumer ID, processes events in
order, then advances its monotonic cursor for at-least-once delivery across
Gateway or plugin restarts.

## Build

```bash
npm install
npm run plugin:build
npm run plugin:validate
npm test
```
