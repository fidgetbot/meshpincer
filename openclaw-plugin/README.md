# MeshPincer OpenClaw plugin

OpenClaw operator tools for the local MeshPincer MeshCore radio service.

The plugin connects to `meshpincerd` over a Unix-domain socket. Configure a
non-default path under `plugins.entries.meshpincer.config.socketPath`.

## Build

```bash
npm install
npm run plugin:build
npm run plugin:validate
npm test
```
