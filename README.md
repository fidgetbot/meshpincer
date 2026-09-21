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

The receive path has been proven over RF with a second physical MeshCore node:
both channel traffic and an encrypted direct message survived a clean daemon
restart with stable message and event IDs. A single zero-hop outbound DM was
also transmitted and acknowledged without retry or flood fallback. The daemon
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
not treated as a normal response length. Uncertain acknowledgements are logged
locally and never generate automatic explanatory radio traffic or retries.

## Project status

Early development. The first hardware-backed, agent-operated MVP is being built
and verified in public.
