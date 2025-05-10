package gtfs

// Byte sizes for encoding/decoding various data types
const (
	lenBytes     = 4
	timeBytes    = 8
	boolBytes    = 1
	float64Bytes = 8
	uint8Bytes   = 1
	uint32Bytes  = 4
)

// Current version of the GTFS database
const CurrentVersion = 2

// Number of seconds in a day
const secondsInDay = 24 * 60 * 60
