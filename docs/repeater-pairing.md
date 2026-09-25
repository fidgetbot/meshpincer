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

   Permission roles used by the repeater firmware are:

   - `1` — read-only; sufficient for status
   - `2` — read-write; does not permit ACL inspection
   - `3` — admin; required for remote binary ACL inspection

   For a companion that will perform ongoing ACL audits, register it directly
   as admin:

   ```text
   setperm <64-hex-companion-public-key> 3
   ```

   Retain role `3` only while that administrative capability is intended.

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

After admin registration, request exactly one ACL snapshot through the OpenClaw
`meshcore_repeater_acl` tool or the local daemon API:

```bash
curl --unix-socket ~/.openclaw/state/meshpincer/meshpincer.sock \
  http://localhost/v1/repeaters/<64-hex-repeater-public-key>/acl
```

The response contains the repeater public key, route metadata, and ACL entries
as six-byte public-key prefixes plus permission bytes. It does not expose full
client public keys or credentials. MeshPincer serializes the request, applies
independent 30-second global and 60-second per-repeater cooldowns, records a
credential-free audit trail, and never retries automatically.

Confirm registration by locating the first 12 hexadecimal characters of the
companion's full public key and checking that its permission byte matches the
intended role. To reduce privileges later, rerun `setperm` from an already
authenticated repeater admin with role `1` or `2`.

## Agent-side ACL and configuration maintenance

After the mutation tools have been deployed and explicitly allowed, an
authorized operator can use `meshcore_repeater_acl_set` with the repeater's full
public key, the target companion's full public key, and permission `0` through
`3`. The daemon pre-reads the ACL, sends exactly one typed `setperm` command,
and verifies the result with a second binary ACL snapshot. It records only the
target's six-byte public-key prefix in audit events and refuses to change its
own ACL entry. Use an independently authenticated admin client for deliberate
self-demotion or recovery.

`meshcore_repeater_configure` is similarly narrow. Its initial supported
setting is `local_advert_interval_minutes`, accepting `0` (disabled) or even
values from `60` through `240`. It reads the old value, changes it once, and
requires a matching read-back. Restore the returned `previous_value` to roll
the operation back. Neither tool accepts raw CLI text.

Remote CLI replies are unacknowledged and can be lost even when the command
arrives. MeshPincer therefore permits at most three attempts per idempotent
typed step. Each timeout retry uses a fresh timestamp and response tag; reusing
the timestamp would trigger firmware replay handling and suppress command
execution. Results report the attempt count. This policy does not apply to
arbitrary messages or commands.

## Proven hardware behavior

The first successful hardware repeater status response occurred after both the
MeshPincer companion and repeater had advertised locally and the companion had
already been registered in the repeater ACL. Earlier status and login requests
timed out. Because both advertisements happened before the successful request,
the test proves the combined pairing sequence, not that either individual
advertisement was independently necessary.

A subsequent admin-only binary ACL request succeeded over the paired zero-hop
route and returned the registered companion's six-byte key prefix with
permission `3`. This proves remote ACL inspection independently of the
serial-only `get acl` text command.
