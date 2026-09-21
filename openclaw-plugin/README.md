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

## Native MeshCore channel

The same package registers the `meshcore` channel. Each direct peer is
identified by its complete public key, and inbound peers must be listed under
`channels.meshcore.allowDirectFrom`. Private group slots must be listed under
`channels.meshcore.allowChannelIndices`; slot 0 (`Public`) is always read-only.
Private group traffic starts an agent turn only when it explicitly mentions the
runtime node name, such as `@Fidget status?` or the MeshCore app form
`@[Fidget] status?`. Channel sender labels are
unverified and channel turns cannot authorize commands or administration.
The channel uses a consumer cursor separate from the operator inbox and, by
default, baselines a new installation at the latest recorded event so old radio
traffic cannot trigger agent turns.

Replies use the fewest words that answer the request and target at most 75
UTF-8 bytes. The current MeshCore firmware ceiling is 160 UTF-8 bytes, which
MeshPincer enforces without splitting multi-byte characters. That ceiling is a
compatibility boundary, not an invitation to fill every packet. The daemon's
known-contact, zero-hop route, cooldown, and no-flood rules continue to apply.
If a reply is transmitted but its ACK is uncertain, the native channel records
that state locally and does not send a second explanatory message or auto-retry.
Private-channel replies include the runtime node name (or the optional
`channelSenderLabel`) inside the same 75-byte budget. Broadcasts have no
end-to-end ACK and are reported only as transmitted. Daemon and plugin
allowlists must both contain the private slot before it can send.

## Build

```bash
npm install
npm run plugin:build
npm run plugin:validate
npm test
```
