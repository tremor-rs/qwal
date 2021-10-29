# Repository Template&emsp; [![Discord]][discord-invite] ![Build Status] ![Quality Checks] ![License Checks] ![Security Checks] [![Code Coverage]][codecov.io]

[build status]: https://github.com/tremor-rs/qwal/workflows/Tests/badge.svg
[quality checks]: https://github.com/tremor-rs/qwal/workflows/Checks/badge.svg
[license checks]: https://github.com/tremor-rs/qwal/workflows/License%20audit/badge.svg
[security checks]: https://github.com/tremor-rs/qwal/workflows/Security%20audit/badge.svg
[code coverage]: https://codecov.io/gh/tremor-rs/qwal/branch/main/graph/badge.svg
[codecov.io]: https://codecov.io/gh/tremor-rs/qwal
[discord]: https://img.shields.io/discord/752801695066488843.svg?label=&logo=discord&logoColor=ffffff&color=7389D8&labelColor=6A7EC2
[discord-invite]: https://bit.ly/tremor-discord

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

