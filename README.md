# Repository Template&emsp; [![Build Status]][tests.yml] [![Quality Checks]][checks.yml] [![License Checks]][licenses.yml] [![Security Checks]][security.yaml] [![Code Coverage]][coveralls.io]

[Build Status]: https://github.com/tremor-rs/qwal/workflows/Tests/badge.svg
[tests.yml]: https://github.com/tremor-rs/qwal/actions/workflows/tests.yml
[Quality Checks]: https://github.com/tremor-rs/qwal/workflows/Checks/badge.svg
[checks.yml]: https://github.com/tremor-rs/qwal/actions/workflows/checks.yml
[License Checks]: https://github.com/tremor-rs/qwal/workflows/License%20audit/badge.svg
[licenses.yml]: https://github.com/tremor-rs/qwal/actions/workflows/licenses.yaml
[Security Checks]: https://github.com/tremor-rs/qwal/workflows/Security%20audit/badge.svg
[security.yaml]: https://github.com/tremor-rs/qwal/actions/workflows/security.yaml
[code coverage]: https://coveralls.io/repos/github/tremor-rs/qwal/badge.svg?branch=main
[coveralls.io]: https://coveralls.io/github/tremor-rs/qwal?branch=main

**Queue like disk backed WAL**

---

Pronouced `Quál` - from the german wordrd for `agony` - because it is.



## Operations

The basic concept is simple, qual supports exactly 4 opperations:

### `push` 

Appends a new entry to the end of the queue, returns it's index.

### `pop`

Reads the next index from the queue, returns the entry and it's index or none if the queue
is fully consumed.

### `ack`

Acknowledges the processing of a entry, this means that in the entry can be removed.

### `revert`

Reverts back to the last acknowledged entry - will anything since then.


## Performance Characteristics

### `push`

Writes the data to disk and performs a fsync. Also a `seek` might be executed if a `pop` was 
performed  since the last `push`.

### `pop`

Reads the data from disk. Also a `seek` is performed if a `push` since the alst `reads`.

### `ack`

No disk operations are performed, `ack`'s are persisted either during a `push` operation or
as part of the shutdown.

## Operations

`qwal` provides all limits as soft limits, meaning they are considered reached once an operation
exceeded them not before.

### `chunk_size`

To allow easier reclemation of space the WAL is stored in multiple chunks. The chunk size
defines the limit. A chunk is considered full once a `push` made it **exceed** the `chunk_size`

### `max_chunks`

The soft limit of of chunks that can be open at the same time. The WAL is considered full when
`max_chunks` + 1 would need to be created

