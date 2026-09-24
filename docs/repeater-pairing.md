# Repeater pairing

Use this procedure when a MeshPincer companion must query or administer a
known MeshCore repeater. Pairing has two independent parts: advertisements make
the nodes and return route visible, while ACL registration grants management
permission. A successful advertisement does not grant permission, and an ACL
entry alone does not prove that replies can return.

## Prerequisites

- Both nodes use the same frequency, bandwidth, spreading factor, and coding
  rate, appropriate for the physical jurisdiction.
- MeshPincer knows the repeater's full 32-byte public key and identifies its
  contact type as repeater.
- `meshpincerd` exclusively owns the companion's serial connection.
- An already authenticated admin companion can manage the repeater, or the
  repeater guest/admin password is available locally.

Never place a repeater password in chat, shell arguments, logs, or SQLite.

## Pairing sequence

1. Send one local/zero-hop advertisement from the repeater using its companion
   app. Do not use a flood advertisement.
2. Ask MeshPincer to send one local/zero-hop advertisement:

   ```bash
   curl --unix-socket ~/.openclaw/state/meshpincer/meshpincer.sock \
     -X POST http://localhost/v1/advertisements/local
   ```

   The endpoint is serialized with other radio work, has a 60-second cooldown,
   cannot flood, and does not retry. `accepted` confirms only that the companion
   accepted the command; it is not RF delivery confirmation.
3. Register the MeshPincer companion's full public key in the repeater ACL.
   From an already authenticated admin companion's repeater CLI, run:

   ```text
   setperm <64-hex-companion-public-key> 1
   ```

   Permission `1` is read-only and is sufficient for repeater status. Use a
   higher role only when a separately authorized operation requires it. An `OK`
   response means the command was accepted; it does not prove end-to-end
   management traffic yet.

   Alternatively, register using the password from a hidden local prompt:

   ```bash
   cd daemon
   uv run meshpincer-repeater-login <64-hex-repeater-public-key>
   ```

   This sends exactly one login request and does not persist the credential.
4. After the status cooldown permits, issue exactly one bounded status request
   through the OpenClaw `meshcore_repeater_status` tool or:

   ```bash
   curl --unix-socket ~/.openclaw/state/meshpincer/meshpincer.sock \
     http://localhost/v1/repeaters/<64-hex-repeater-public-key>/status
   ```

5. Treat pairing as verified only when the response is correlated to the
   requested repeater and contains its status fields. Do not retry
   automatically after a timeout.

## ACL inspection

The text command `get acl` is serial-only and returns `??: acl` through a
remote repeater CLI. Newer repeater firmware also supports an authenticated
binary ACL request for remote admin clients. These are different interfaces;
failure of the text command does not imply that remote binary ACL inspection is
unsupported.

## Proven hardware behavior

The first successful Lighthouse status response occurred after both the
MeshPincer companion and repeater had advertised locally and the companion had
already been registered in the repeater ACL. Earlier status and login requests
timed out. Because both advertisements happened before the successful request,
the test proves the combined pairing sequence, not that either individual
advertisement was independently necessary.

