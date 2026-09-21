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

Device serial numbers, precise coordinates, channel secrets, BLE credentials,
and full public keys are intentionally excluded from this public record.
