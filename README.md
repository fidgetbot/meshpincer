# MeshPincer

**MeshCore for OpenClaw**

**Built by an OpenClaw agent, for OpenClaw agents.**

MeshPincer is an agent-built bridge between OpenClaw and an always-on MeshCore
companion radio. It gives OpenClaw agents two related capabilities:

1. operator tools that let an authorized OpenClaw conversation inspect and act
   on a MeshCore network; and
2. a native MeshCore channel that lets radio peers converse with an OpenClaw
   agent.

## Architecture

```text
MeshCore RF <-> companion radio <-> meshpincerd <-> OpenClaw plugin
                                      |               |-- operator tools
                                      |               `-- MeshCore channel
                                      `-- SQLite event and message store
```

The Python daemon exclusively owns the radio connection. The TypeScript plugin
talks to it over a local Unix-domain socket, so OpenClaw restarts do not prevent
the daemon from collecting inbound messages.

Device mutations use that same daemon-owned connection. Contact imports and
deletions, plus private-channel set, rename, and clear operations, are
serialized with message traffic and verified by immediate radio read-back.
They do not require handing the serial port to a helper process or restarting
the daemon. Public-channel configuration remains blocked.

The project is intentionally reusable: device names, agent names, network
profiles, contacts, and channels are discovered or configured at runtime rather
than compiled into MeshPincer.

## Be a quiet mesh neighbor

Agents can generate traffic much faster than LoRa can carry it. MeshPincer
therefore treats airtime as a shared, scarce resource:

- prefer DMs and learned direct routes over channel floods;
- scope automated channel traffic to the smallest useful region or hop budget;
- rate-limit, deduplicate, batch, and keep replies short;
- observe passively and avoid routine probes or startup adverts;
- accept commands only from explicit, authorized interactions; and
- identify automation and report delivery state honestly.

The complete [agent etiquette](docs/agent-etiquette.md) is a product requirement
for the transmit path, not optional deployment advice.

## Repository layout

- `daemon/` — Python 3.12 asyncio service built on `meshcore_py`
- `openclaw-plugin/` — TypeScript OpenClaw tool plugin
- `docs/agent-etiquette.md` — airtime and safety policy for agents
- `SPEC.md` — product scope, architecture, and delivery milestones

## Development

Prerequisites:

- Python 3.12 managed with `uv`
- Node.js 24.16 or newer
- OpenClaw 2026.9.3 or a compatible release

```bash
# Daemon
cd daemon
uv sync --dev
uv run pytest

# OpenClaw plugin
cd ../openclaw-plugin
npm install
npm run plugin:validate
npm test
```

The first hardware-backed daemon slice is operational. It discovers a companion
by stable USB identity, owns and reconnects the serial session, and serves live
status, health, telemetry, contacts, and channel data over the Unix socket. The
OpenClaw plugin exposes that data through typed tools without publishing device
serial numbers, channel secrets, or contact coordinates.

Durable receive is also implemented. The daemon continuously drains pending
direct and channel messages into SQLite, suppresses duplicate protocol events,
and atomically appends monotonically numbered events. Independent OpenClaw
consumers can replay those events and advance durable cursors only after they
have processed them, providing at-least-once delivery across restarts.

SQLite housekeeping is bounded and cursor-aware. By default MeshPincer retains
30 days and at most 50,000 messages, but removes only a contiguous event prefix
already passed by every active consumer. Consumers idle for 30 days may expire;
their next cursor read reports an explicit history gap and resumes at the
retained boundary. Cleanup runs daily in batches of at most 1,000 events,
checkpoints and truncates the WAL only after pruning, exposes database metrics
through `meshcore_status`, supports a dry-run endpoint, and never performs an
automatic live `VACUUM`.

The receive path has been proven over RF with a second physical MeshCore node:
both channel traffic and an encrypted direct message survived a clean daemon
restart with stable message and event IDs. Outbound DMs have been transmitted
over both zero-hop and learned multi-hop direct routes without flood fallback.
The daemon
durably records each delivery-state transition and enforces direct-message
length and cooldown limits before transmission.

The operator plugin is also live-proven through an OpenClaw Gateway: an agent
has called the real `meshcore_status` and cursor-backed `meshcore_messages`
tools against a supervised `meshpincerd` instance and attached Wio. Restrictive
OpenClaw tool profiles must explicitly allow the implemented MeshPincer tools;
see the [plugin README](openclaw-plugin/README.md).

The native `meshcore` channel is installed and connected in the same Gateway.
It consumes direct-message events through an independent durable cursor, maps
peers by full public key, starts new installations at the current event head,
and permits inbound turns only from explicitly configured public keys. Native
channel replies use the daemon's acknowledged direct-send path and remain
subject to its route, length, and cooldown safeguards.

Native replies aim for at most 75 UTF-8 bytes and use fewer whenever possible.
The firmware's 160-byte text maximum is enforced as a hard encoded-byte limit,
not treated as a normal response length. MeshCore turns receive a system-level
RF writing contract and no optional tools. `Reply X`, `Reply: X`,
`Reply exactly X`, and `Reply exactly: X` requests are handled
deterministically. An oversized draft gets at most one
fresh, tool-free local compression pass and is measured again. If compression
is unavailable or misses the 75-byte target, a legal draft up to 160 bytes is
sent intact; a longer draft becomes one fixed, bounded request to narrow the
question. Genuine user requests never disappear silently. Uncertain
acknowledgements and OpenClaw restart-recovery notices remain local and never
generate explanatory radio traffic or retries.

Allowlisted private MeshCore slots map to native OpenClaw group conversations.
They require an explicit `@<runtime node name>` invocation; sender labels remain
untrusted and cannot authorize commands or administration. Public is always
read-only. Private-channel replies count their sender label inside the 75-byte
budget, use strict global/per-channel cooldowns, and are recorded as
`transmitted` rather than falsely claiming an ACK.

Read-only repeater status is implemented as one bounded request to an exact,
known repeater public key. The daemon validates the contact type, records the
outcome durably, and enforces a 30-second global cooldown plus a 60-second
per-repeater cooldown. It never polls automatically or retries a timed-out
request. One-time repeater authentication is available through the local-only
login endpoint and hidden-input CLI. The login sends exactly once, correlates
success or rejection to the requested repeater prefix, and never stores or logs
the credential. Admin-only remote ACL inspection is hardware-proven through one
bounded binary request and returns only six-byte client key prefixes plus
permission bytes. Typed repeater mutations are implemented behind the same
daemon-owned connection: `meshcore_repeater_acl_set` changes another
companion's role and verifies the resulting ACL snapshot, while
`meshcore_repeater_configure` initially supports only the reversible local
zero-hop advert interval. Configuration performs a pre-read, one change, and a
post-change read-back. Both mutation tools reject unknown/non-repeater targets,
use independent cooldowns, and never expose arbitrary remote CLI. The daemon
also refuses to change its own ACL entry, avoiding accidental loss of the
administrative path. Live hardware mutation proof is tracked separately from
local and CI validation.
The separate `meshcore_repeater_config_get` tool reads that setting without
changing it.

Pairing a new repeater-management relationship requires both route/identity
visibility and ACL registration. Follow the complete, hardware-proven
[`docs/repeater-pairing.md`](docs/repeater-pairing.md) procedure; do not treat an
advertisement, `setperm` acknowledgement, or login transmission by itself as
proof of pairing.

The contact and private-channel management endpoints are intentionally local
administrative APIs on the Unix socket, not general-purpose OpenClaw agent
tools. Channel secrets are accepted only for a set operation and are never
returned, logged, or written to the SQLite event payloads.
Contact-route management is likewise typed and local-only: it can set a known
contact to zero-hop direct or reset it to unknown/flood, then immediately reads
the contact back from the radio. It does not accept arbitrary route bytes.

The same local API can read and toggle the companion firmware's
"overwrite oldest non-favorite when full" contact-retention bit. MeshPincer
preserves the device's existing auto-add type mask and hop limit, verifies the
new setting by immediate read-back, and leaves favorited contacts ineligible
for automatic eviction.

## Project status

Early development. The first hardware-backed, agent-operated MVP is being built
and verified in public.
