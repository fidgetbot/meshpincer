# MeshPincer OpenClaw plugin

OpenClaw operator tools for the local MeshPincer MeshCore radio service.

The plugin connects to `meshpincerd` over a Unix-domain socket. Configure a
non-default path under `plugins.entries.meshpincer.config.socketPath`.

When OpenClaw uses a restrictive tool profile such as `coding`, explicitly add
the implemented operator tools to `tools.alsoAllow`:

```json5
{
  tools: {
    alsoAllow: [
      "meshcore_status",
      "meshcore_messages",
      "meshcore_contacts",
      "meshcore_channels",
      "meshcore_send",
    ],
  },
}
```

Do not allow the repeater tools until their daemon endpoints and verification
workflow are configured. Plugin activation alone does not bypass OpenClaw's
normal tool policy.

The typed client supports durable event replay and cursor advancement over the
same socket. Each OpenClaw surface uses its own consumer ID, processes events in
order, then advances its monotonic cursor for at-least-once delivery across
Gateway or plugin restarts.

`meshcore_messages` defaults to an unread, cursor-backed view. It never marks a
batch read implicitly: after an agent successfully processes a response, it
passes the last event ID back as `acknowledgeThroughEventId` on its next call.
Set `mode` to `history` for an explicit message-ID-based history query.

## Build

```bash
npm install
npm run plugin:build
npm run plugin:validate
npm test
```
