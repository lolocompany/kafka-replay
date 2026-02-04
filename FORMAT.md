# Binary File Format Specification

This document describes the binary file format used by the Kafka Replay transcoder to store recorded Kafka messages.

## Overview

The file format consists of:
1. A fixed-size file header containing protocol metadata
2. A series of message entries, each containing a timestamp, size, and message data

## File Structure

```
[File Header (20 bytes)]
[Message Entry 1]
[Message Entry 2]
...
[Message Entry N]
```

## File Header

The file header is 20 bytes total and appears at the beginning of every file:

| Offset | Size | Type | Description |
|--------|------|------|-------------|
| 0 | 4 | int32 (big-endian) | Protocol version (currently 1) |
| 4 | 16 | bytes | Reserved space for future use (all zeros) |

### Protocol Version

The protocol version field is a 32-bit signed integer stored in big-endian byte order. Currently, the only supported version is `1`. Files with unsupported protocol versions will be rejected when reading.

### Reserved Space

The 16 bytes following the protocol version are reserved for future protocol extensions. Currently, these bytes are always set to zero.

## Message Entry Format

Each message entry follows this structure:

| Offset | Size | Type | Description |
|--------|------|------|-------------|
| 0 | 8 | int64 (big-endian) | Unix timestamp (seconds since epoch, UTC) |
| 8 | 8 | int64 (big-endian) | Message data size in bytes |
| 16 | variable | bytes | Message data (raw bytes) |

### Timestamp

The timestamp is stored as a Unix timestamp (seconds since January 1, 1970 UTC) as a 64-bit signed integer in big-endian byte order. This represents when the message was recorded.

**Example:** A timestamp value of `1706872530` represents `2024-02-02T10:15:30Z`.

### Message Size

The message size field indicates the length of the message data in bytes. It is stored as a 64-bit signed integer in big-endian byte order. The maximum supported message size is 100 MB (104,857,600 bytes). Messages larger than this will cause an error when reading.

### Message Data

The message data follows immediately after the size field. It contains the raw bytes of the Kafka message value. The length of this field is determined by the message size field.

## Byte Order

All multi-byte integers (int32, int64) are stored in **big-endian** (network byte order) format. This ensures compatibility across different architectures.

## Example

For a message with:
- Timestamp: `2024-02-02T10:15:30Z` (Unix timestamp: `1706872530`)
- Data: `"Hello, World!"` (13 bytes)

The binary representation would be:

```
[File Header - 20 bytes]
[0x00 0x00 0x00 0x01]  # Protocol version 1
[0x00 ... 0x00]        # 16 reserved bytes

[Message Entry - 29 bytes]
[0x00 0x00 0x00 0x00 0x65 0x9C 0x5C 0x92]  # Timestamp: 1706872530
[0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x0D]  # Size: 13
[0x48 0x65 0x6C 0x6C 0x6F 0x2C 0x20 0x57 0x6F 0x72 0x6C 0x64 0x21]  # "Hello, World!"
```

## Reading Files

When reading files:

1. **Read the header** (20 bytes) and validate the protocol version
2. **For each message entry:**
   - Read 8 bytes for the timestamp
   - Read 8 bytes for the message size
   - Read N bytes (where N is the message size) for the message data
   - Parse the timestamp from Unix seconds to a time.Time value

## Writing Files

When writing files:

1. **Write the header** (20 bytes) with protocol version 1 and zero-filled reserved bytes
2. **For each message:**
   - Convert the timestamp to Unix seconds (int64)
   - Write 8 bytes (big-endian) for the timestamp
   - Write 8 bytes (big-endian) for the message size
   - Write the message data bytes

## Constants

The format uses the following constants (defined in `pkg/transcoder/constants.go`):

- `ProtocolVersion = 1`
- `HeaderVersionSize = 4` bytes
- `HeaderReservedSize = 16` bytes
- `HeaderSize = 20` bytes (HeaderVersionSize + HeaderReservedSize)
- `TimestampSize = 8` bytes
- `SizeFieldSize = 8` bytes
- Maximum message size: `100 * 1024 * 1024` bytes (100 MB)

## Implementation

The format is implemented in the `pkg/transcoder` package:

- **`EncodeWriter`**: Writes messages in this format
- **`DecodeReader`**: Reads messages from this format

Both types work with Go's standard `io.Writer` and `io.ReadSeeker` interfaces, making them flexible and testable.
