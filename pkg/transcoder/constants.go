package transcoder

const (
	// ProtocolVersion is the current version of the binary protocol
	ProtocolVersion = 2
	// ProtocolVersion1 is the legacy version 1 (without message keys)
	ProtocolVersion1 = 1
	// HeaderVersionSize is the size of the version field in the header (int32 = 4 bytes)
	HeaderVersionSize = 4
	// HeaderReservedSize is the size of reserved space in the header for future use
	HeaderReservedSize = 16
	// HeaderSize is the total size of the file header
	HeaderSize = HeaderVersionSize + HeaderReservedSize // 20 bytes total
	// TimestampSize is the size of the timestamp field (int64 Unix timestamp = 8 bytes)
	TimestampSize = 8
	// SizeFieldSize is the size of the message size field (int64 = 8 bytes)
	SizeFieldSize = 8
	// KeySizeFieldSize is the size of the key size field (int64 = 8 bytes)
	KeySizeFieldSize = 8
)
