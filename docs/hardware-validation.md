# Hardware validation

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

The remaining M1 RF acceptance work is outbound transmission and
acknowledgement correlation. MeshPincer itself transmitted no advert, channel
message, or direct message during this receive test.

Device serial numbers, precise coordinates, channel secrets, BLE credentials,
and full public keys are intentionally excluded from this public record.
