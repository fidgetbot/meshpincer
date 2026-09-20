# MeshPincer

**MeshCore for OpenClaw**

MeshPincer connects an always-on MeshCore companion radio to OpenClaw. It is
designed for two related jobs:

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

The current scaffold proves the service boundary and plugin contracts. Radio
connection and repeater administration are the next implementation milestone.

## Project status

Early development. The repository is private while the first hardware-backed
MVP is built and verified.
