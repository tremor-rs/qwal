# Repository Template&emsp; [![Build Status]][tests.yaml] [![Quality Checks]][checks.yaml] [![License Checks]][licenses.yaml] [![Security Checks]][security.yaml] [![Code Coverage]][coveralls.io]

[Build Status]: https://github.com/tremor-rs/qwal/workflows/Tests/badge.svg
[tests.yaml]: https://github.com/tremor-rs/qwal/actions/workflows/tests.yaml
[Quality Checks]: https://github.com/tremor-rs/qwal/workflows/Checks/badge.svg
[checks.yaml]: https://github.com/tremor-rs/qwal/actions/workflows/checks.yaml
[License Checks]: https://github.com/tremor-rs/qwal/workflows/License%20audit/badge.svg
[licenses.yaml]: https://github.com/tremor-rs/qwal/actions/workflows/licenses.yaml
[Security Checks]: https://github.com/tremor-rs/qwal/workflows/Security%20audit/badge.svg
[security.yaml]: https://github.com/tremor-rs/qwal/actions/workflows/security.yaml
[code coverage]: https://coveralls.io/repos/github/tremor-rs/qwal/badge.svg?branch=main
[coveralls.io]: https://coveralls.io/github/tremor-rs/qwal?branch=main

**Queue like disk backed WAL**

---

Pronouced `Quál` - from the german word for `agony` - because it is.

## features

### tokio
uses the tokio types

### async-std
uses async-std types



## Operations

The basic concept is simple, `qwal` supports exactly 4 operations:

### `push` 

Appends a new entry to the end of the queue, returning its index.

### `pop`

Reads the next index from the queue, returning the previous entry and its index if
one exists, or none if the queue is empty.

### `ack`

Acknowledges processing of an entry, confirming it is enqueued.

### `revert`

Reverts back to the last acknowledged entry in the queue - will clear/drain any entry since that point.


## Performance Characteristics

### `push`

Writes the data to disk and performs a fsync. Also a `seek` might be executed if a `pop` was 
performed  since the last `push`.

### `pop`

Reads the data from disk. Also a `seek` is performed if a `push` since the alst `reads`.

### `ack`

No disk operations are performed, `ack`'s are persisted either during a `push` operation or
as part of the shutdown sequence.

## Operations

`qwal` provides all limits as soft limits, meaning they are considered reached after an operation
has exceeded them, and not before.

### `chunk_size`

To allow easier reclamation of space the WAL is stored in multiple chunks. The chunk size
defines the limit. A chunk is considered full once a `push` makes it **exceed** the `chunk_size`

### `max_chunks`

The soft limit of of chunks that can be active and open at the same time. The WAL is considered full when
`max_chunks` + 1 would need to be created.

