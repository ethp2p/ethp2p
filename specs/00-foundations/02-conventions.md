---
status: implemented
requires: []
---

# Conventions

Every spec in this tree uses the notation below and links here instead of restating it.

## Requirements language

MUST, MUST NOT, SHOULD, SHOULD NOT and MAY are to be interpreted as described in [RFC 2119] and [RFC 8174] when, and only when, they appear in capitals.

## Notation

| Notation | Meaning |
| -------- | ------- |
| `0x2f` | A byte in hexadecimal; `0x2f6d` is a byte string. Example blocks write bytes as bare hex pairs, one annotation per line. |
| `x \|\| y` | Concatenation. |
| `len(x)` | The length of `x` in bytes. |
| `uvarint(n)` | `n` as an unsigned LEB128 varint, the Protocol Buffers varint: seven bits per byte, least significant group first, the high bit set on every byte but the last, at most 64 bits. Senders use the minimal encoding; a spec says where a receiver rejects any other. |
| `prefixed(x)` | `x` preceded by its length in bytes: `uvarint(len(x)) \|\| x`, so a reader knows how many bytes to take. Example: `prefixed(aa bb cc)` is `03 aa bb cc`. |
| `HELLO` | A [frame](../01-transport/02-streams.md#frames) of the type with that name, `uvarint(type) \|\| prefixed(payload)`. Unless its spec says otherwise, the payload is the message of the same name: a `HELLO` frame carries a `Hello`. |
| `[x]` | `x` or nothing. |
| `*x` | Zero or more `x`. |
| `Hello` | A protobuf message. Schemas are proto3. |
| `CLASSIFY_TIMEOUT` | A constant, defined in the constants table of the spec that uses it. |
| `name = ...` | A byte layout; `#` starts a comment. |

[RFC 2119]: https://www.rfc-editor.org/rfc/rfc2119
[RFC 8174]: https://www.rfc-editor.org/rfc/rfc8174
