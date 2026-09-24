# Hardware validation

## Repeater pairing and status — 2026-09-23

The companion was registered in a known repeater's ACL through an authenticated
admin companion. Remote `setperm` returned `OK`, but repeated bounded status
requests and one hidden-input login request initially timed out despite normal
direct messaging and live Public-channel reception.

MeshPincer then sent one local zero-hop self advertisement, while the operator
also sent one advertisement from the repeater. The next single binary status
request succeeded and was correlated by its request tag. The repeater returned
a 4.179 V battery reading, an empty transmit queue, and 11.5 dB last SNR. No
automatic retry, flood advertisement, clock mutation, or additional ACL change
was performed.

This validates the combined dual-advertisement plus ACL-registration pairing
procedure. It does not isolate which advertisement was necessary because both
occurred before the successful request. The reproducible operational procedure
is recorded in [`repeater-pairing.md`](repeater-pairing.md).

## Wio Tracker L1 Pro — 2026-09-20

The first read-only hardware inventory was performed against a Wio Tracker L1
Pro running the official USB companion firmware.

Observed results:

- macOS enumerated the device as `Seeed Wio Tracker L1` using USB VID:PID
  `2886:1667` and exposed a USB CDC serial endpoint;
- `meshcore_py` connected successfully over serial and returned the companion
  node's self and device information;
- firmware reported `v1.17.1-d929643`, build date `14-Aug-2026`, protocol
  version 13, and repeater mode disabled;
- battery, storage, core, radio, and packet statistics were readable, with no
  device errors reported;
- the contact list was empty;
- channel slot 0 was named `Public`, and the remaining 39 slots were empty; and
- the configured radio parameters were 869.618 MHz, 62.5 kHz bandwidth, SF8,
  CR5, and 22 dBm transmit power.

No radio transmission or configuration mutation was performed during the
read-only inventory. The observed 869.618 MHz configuration was outside the US
902–928 MHz ISM band, so RF testing was paused.

The device location was subsequently confirmed as Seattle, Washington, and the
target network profile was supplied as 910.525 MHz, 62.5 kHz bandwidth, SF7,
and CR5. Those four parameters were applied through the companion protocol and
read back successfully after a fresh serial connection. Transmit power remains
unchanged at 22 dBm.

The companion's self-telemetry also returned a GNSS location consistent with
Seattle's Capitol Hill neighborhood. No advert or mesh message was transmitted
during configuration or GNSS validation.

Changing the companion's device/advert name and reading it back after a fresh
serial connection was also proven. The chosen name is device configuration;
MeshPincer remains name-agnostic and discovers it through self-info at runtime.

The first packaged `meshpincerd` hardware slice was then exercised over its
Unix-domain socket. Stable USB matching selected the attached radio, the daemon
completed its read-only snapshot, and `GET /v1/status`, `GET /v1/contacts`, and
both configured-only and all-slot `GET /v1/channels` queries returned expected
data. The TypeScript client used by the OpenClaw tools read the same live status
and channel data. Graceful shutdown released the serial connection.

This validation did not send an advert or mesh message. The live API output was
also checked to ensure it contains no channel secret or USB hardware serial.

### Two-node receive acceptance

A second MeshCore companion was exchanged with the Wio through out-of-band
contact cards containing only node name, public key, and node type. Neither
node needed to advertise over RF. Both contact records were verified by public
key before testing.

After the radio profile change, the Wio initially reported the stored Seattle
profile correctly but accumulated no receive airtime or packets while the
second node was receiving active traffic on the same profile. A companion
device reboot restored reception. This demonstrates that configuration
read-back alone is not sufficient validation after a profile change: live
receive airtime or packet counters must also advance during known traffic.

With the receiver active, the packaged daemon accepted and durably stored both
Public-channel traffic and an encrypted direct message from the second node.
The direct message resolved from its six-byte wire prefix to the contact's full
public key. A clean daemon restart preserved the original message and event
IDs, replayed them from an unadvanced consumer cursor, and did not create
additional records.

The sender later retransmitted the same direct-message text with a new sender
timestamp while changing from a zero-hop path to flood routing. MeshPincer
preserved that as a separate protocol message. Exact redelivery of the same
protocol identity is deduplicated; distinct sender timestamps are retained so
an intentional repeated message is not silently discarded.

### Two-node send and acknowledgement acceptance

After explicit operator authorization, MeshPincer sent one short direct
message to the second companion over its verified zero-hop route. The daemon
performed exactly one transmission: automatic retry, flood fallback, channel
send, and advert were all disabled for the test. It durably recorded queued,
transmitted, and acknowledged events, and the acknowledgement matched the
four-byte code returned when the companion accepted the send command.

This completes the direct-message RF send/acknowledgement case. The production
path also fails closed for unknown contacts and non-zero-hop routes, limits
message length, serializes radio operations, and enforces global and per-peer
cooldowns before transmission.

The same send path was subsequently exercised through the installed OpenClaw
operator tool from a Telegram conversation. One explicit request sent
`MeshPincer operator OK` to the known zero-hop companion. MeshPincer created one
outbound record and durably recorded queued, transmitted, and acknowledged
events with a matching ACK code in 0.83 seconds. It created no retry, flood,
channel message, or advert. The pager operator confirmed receipt of the single
message copy.

### Native OpenClaw channel acceptance

The installed `meshcore` channel was baselined at the current durable event
head, then received a new direct message from the allowlisted second companion.
It created a stable OpenClaw conversation keyed by the peer's full public key,
ran an agent turn, submitted a direct reply through `meshpincerd`, and advanced
its channel cursor without activating on ambient Public-channel traffic.

The first reply was transmitted but did not receive an ACK within the
firmware-suggested window. OpenClaw's generic delivery recovery then generated
a second explanatory message; that message reached the pager but also lacked a
recorded ACK. This proved the complete inbound-agent-outbound RF path, while
exposing behavior that is inappropriate for scarce LoRa airtime. MeshPincer's
native channel now treats a timed-out ACK as a locally recorded uncertain
outcome: it logs the state and neither emits explanatory RF traffic nor retries
automatically. Operator-initiated sends continue to report timed-out delivery
explicitly.

The same acceptance test prompted a payload-policy correction. Companion
firmware v1.17.1 limits text with `MAX_TEXT_LEN` to 160 UTF-8 bytes, not 160
Unicode characters. MeshPincer now enforces that encoded-byte ceiling and
guides agents toward at most 75 UTF-8 bytes for ordinary replies. With the
five-byte direct-message text header, 75 bytes fills exactly five AES blocks;
76 bytes requires a sixth block.

A final post-fix acceptance test sent `Reply exactly: MeshPincer OK` from the
allowlisted companion. OpenClaw generated and MeshPincer transmitted exactly
`MeshPincer OK` (13 UTF-8 bytes). The daemon recorded one queued event, one
transmitted event, and a matching acknowledgement less than one second after
transmission. No recovery notice, oversized fallback, duplicate, or automatic
retry followed. This closes the concise native direct-message reply and
restart-recovery suppression cases.

### Private group-channel acceptance

A randomly keyed temporary channel in slot 1 was imported out of band into the
two companions and explicitly allowlisted at both the daemon and OpenClaw
boundaries. Ambient group traffic remained passive. A message using the
MeshCore app's bracketed runtime-name mention created one stable per-slot group
conversation and exactly one short channel reply. The reply was durably
recorded as transmitted; no end-to-end acknowledgement was claimed because
MeshCore channel broadcasts do not provide one.

After the proof, the temporary channel was removed from the Wio, slot 1 was
read back as empty, and both local transmit allowlists were cleared. No RF
packet was sent by the USB configuration cleanup. Public remained receive-only
throughout.

### Restart-free device-management acceptance

The supervised daemon was upgraded once to load its contact and private-channel
management API. After it reconnected, an existing companion contact was
idempotently updated and private slot 1 was renamed to its existing label
through the daemon's Unix socket. Both operations used the same long-lived
daemon PID, shared its serialized radio-operation lock, and returned verified
read-back state without disconnecting the USB session.

The contact retained its flood/unknown route, while the private channel retained
its channel hash. A fresh periodic hardware-statistics snapshot still reported
14 transmitted packets—the same count as before both changes—confirming that
neither operation emitted RF traffic. Audit events recorded the operation and
sanitized outcome without storing the private channel secret.

The companion's contact-retention setting was later brought under the same
daemon-owned mutation path. MeshPincer reads the complete auto-add bitmask,
changes only the `overwrite oldest non-favorite when full` bit, and verifies
the full read-back so other auto-add type and hop-limit settings cannot be
silently reset.

The live Wio initially reported auto-add mask `0`, hop limit `0`, and overwrite
disabled. MeshPincer enabled only bit 0 and read back mask `1` with the hop
limit unchanged. The two controlled companion contacts were marked favorite;
their routes survived the update. The radio's transmitted-packet counter was
unchanged across the setting and contact updates, confirming that the USB-only
management operations emitted no RF traffic.

Device serial numbers, precise coordinates, channel secrets, BLE credentials,
and full public keys are intentionally excluded from this public record.
