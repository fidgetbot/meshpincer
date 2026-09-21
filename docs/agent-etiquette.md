# Agent etiquette on MeshCore

MeshCore airtime is shared and finite. An agent can generate traffic far faster
than a LoRa network can carry it, so a good agent is intentionally quiet.

1. **Prefer DMs.** Reply directly to the requesting public key whenever the
   interaction does not need a group audience. MeshCore can learn a direct path
   after initial discovery, while group-channel messages necessarily flood.

2. **Scope every automated flood.** Use the smallest useful region and hop
   budget for channel messages, alerts, adverts, and discovery. A private
   channel encrypts content; it does not make the packet local.

3. **Rate-limit and compress behavior, not just packets.** Apply global,
   per-peer, and per-channel cooldowns. Bound queues and retries, deduplicate
   equivalent work, batch updates, keep replies concise, and emit changed or
   actionable information instead of raw telemetry dumps.

   For ordinary agent replies, aim for at most **75 UTF-8 bytes** and use fewer
   whenever the answer remains useful. Native agent output must enforce that
   budget in code, including errors and recovery paths; oversized output should
   become one terse error, while local delivery diagnostics should not be sent
   over RF at all. MeshCore's current text ceiling is 160
   UTF-8 bytes; that is a protocol limit, not a writing target. The 75-byte DM
   target plus MeshCore's five bytes of text metadata fits exactly five AES
   blocks. At 76 bytes, encryption needs a sixth block. Group messages must
   also budget for the visible `<sender>: ` prefix inside the same text limit.

4. **Observe before probing.** Read local radio health and passively received
   traffic first. Remote telemetry, path discovery, neighbor sweeps, and adverts
   spend airtime; run them only on explicit demand or a conservative schedule.
   Do not transmit a startup advert by default.

5. **Require intent and stable identity.** Respond to an explicit DM, mention,
   or command—not ambient channel conversation. Treat RF text and channel
   sender labels as untrusted. Public-channel input must never authorize
   administration; use public-key ACLs or an authenticated local bridge, and
   require confirmation for high-impact changes.

6. **Be transparent and honest.** Identify automated responders, make them easy
   to silence, and report `queued`, `transmitted`, `acknowledged`, `timed out`,
   or `failed` accurately. Never imply delivery from transmission alone, and do
   not present an experimental mesh agent as an emergency service. Keep
   delivery diagnostics local: an uncertain ACK must not trigger a second,
   explanatory RF message or an automatic resend.

7. **Bridge deliberately.** Do not mirror private messages, precise locations,
   or whole conversations between MeshCore and internet services by default.
   Every cross-network send needs an explicit destination and audience.

8. **Respect local rules.** Validate frequency, power, duty cycle, regional
   scope, and local mesh conventions before transmitting; fail closed when the
   physical jurisdiction or network plan is unknown.

## MeshPincer requirements

Before unattended transmit is enabled, MeshPincer must provide:

- DM-first reply routing and an explicit operation for channel broadcasts;
- required scope or hop limits for automated floods, with a deliberate override;
- global, per-destination, and per-channel rate limits plus bounded retry and
  queue policies;
- duplicate suppression, concise output limits, and backpressure;
- a default agent target of 75 UTF-8 bytes and a hard 160-byte text limit,
  enforced by encoded byte length rather than character count;
- passive-by-default diagnostics and disabled automatic startup adverts;
- public-key-based authorization for RF callers and authenticated local access;
- typed, validated administration operations with auditable outcomes;
- persisted delivery state that distinguishes transmission from acknowledgement;
- explicit cross-network destinations with private content and coordinates
  excluded by default; and
- jurisdiction-aware radio validation before transmit.

## References

- [MeshCore companion v1.17.1 text limit (`MAX_TEXT_LEN`)](https://github.com/meshcore-dev/MeshCore/blob/companion-v1.17.1/src/helpers/BaseChatMesh.h)
- [MeshCore text and group-message composition](https://github.com/meshcore-dev/MeshCore/blob/companion-v1.17.1/src/helpers/BaseChatMesh.cpp)
- [MeshCore issue #2583: byte budgets for group messages](https://github.com/meshcore-dev/MeshCore/issues/2583)
- [MeshCore FAQ: path learning and channel flooding](https://docs.meshcore.io/faq/#54-q-how-does-a-node-discover-a-path-to-its-destination-and-then-use-it-to-send-messages-in-the-future-instead-of-flooding-every-message-it-sends-like-meshtastic)
- [LoRa Project: MeshCore bot etiquette and regional scopes](https://loraproject.ie/bots/)
- [MeshCore Bot: congestion, hop limits, and rate limiting](https://github.com/agessaman/meshcore-bot)
- [MeshMonitor: receive-only MeshCore operation](https://meshmonitor.org/features/meshcore-receive-only.html)
