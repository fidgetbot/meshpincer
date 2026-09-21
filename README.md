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

## Repository layout

- `daemon/` — Python 3.12 asyncio service built on `meshcore_py`
- `openclaw-plugin/` — TypeScript OpenClaw tool plugin
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

The current scaffold proves the service boundary and plugin contracts. Hardware
validation has proven USB discovery, companion-protocol access, radio
configuration, GNSS telemetry, and persistent device naming. The next coding
step is moving that working protocol path into the persistent daemon.

## Project status

Early development. The first hardware-backed, agent-operated MVP is being built
and verified in public.
