package gtfs

import (
	"encoding/binary"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

type LocationType uint8
type ModeFlag uint8

const (
	StopLocationType LocationType = iota
	StationLocationType
	EntranceExitLocationType
	GenericNodeLocationType
	BoardingAreaLocationType
	UnknownLocationType
)

const (
	BusModeFlag ModeFlag = 1 << iota
	SchoolBusModeFlag
	RailModeFlag
	FerryModeFlag
	UnknownModeFlag = 0
)

// Represents a stop, platform, or station in a transit system
type Stop struct {
	ID             Key
	Code           string
	Name           string
	ParentID       Key
	Location       Coordinate
	LocationType   LocationType
	SupportedModes ModeFlag
}
type StopMap map[Key]*Stop
type StopArray []*Stop

// Encode serializes the Stop struct (excluding ID) into a byte slice.
// Format:
// - Code: 4-byte length + UTF-8 string
// - Name: 4-byte length + UTF-8 string
// - ParentID: 4-byte length + UTF-8 string
// - Location: 2 * float64 (fixed size)
// - LocationType: 1 byte (LocationType enum)
// - SupportedModes: 1 byte (bitmask for each mode)
func (s Stop) Encode() []byte {
	codeStr := s.Code
	nameStr := s.Name
	parentIDStr := string(s.ParentID)
	locationBytes := s.Location.Encode() // Coordinate.Encode() returns a fixed-size slice

	// Calculate total length
	totalLen := lenBytes + len(codeStr) + // Code
		lenBytes + len(nameStr) + // Name
		lenBytes + len(parentIDStr) + // ParentID
		len(locationBytes) + // Location (fixed size: 2 * float64Bytes)
		uint8Bytes + // LocationType
		uint8Bytes // SupportedModes

	data := make([]byte, totalLen)
	offset := 0

	// Marshal Code
	binary.BigEndian.PutUint32(data[offset:], uint32(len(codeStr)))
	offset += lenBytes
	copy(data[offset:], codeStr)
	offset += len(codeStr)

	// Marshal Name
	binary.BigEndian.PutUint32(data[offset:], uint32(len(nameStr)))
	offset += lenBytes
	copy(data[offset:], nameStr)
	offset += len(nameStr)

	// Marshal ParentID
	binary.BigEndian.PutUint32(data[offset:], uint32(len(parentIDStr)))
	offset += lenBytes
	copy(data[offset:], parentIDStr)
	offset += len(parentIDStr)

	// Marshal Location
	copy(data[offset:], locationBytes)
	offset += len(locationBytes)

	// Marshal LocationType
	data[offset] = byte(s.LocationType)
	offset += uint8Bytes

	// Marshal SupportedModes
	data[offset] = byte(s.SupportedModes)

	return data
}

// Decode deserializes the byte slice into the Stop struct.
func (s *Stop) Decode(id Key, data []byte) error {
	if s == nil {
		return errors.New("cannot decode into a nil Stop")
	}
	offset := 0

	// Set ID from parameter
	s.ID = id

	// Unmarshal Code
	if offset+lenBytes > len(data) {
		return errors.New("stop buffer too small for Code length")
	}
	codeLen := binary.BigEndian.Uint32(data[offset:])
	offset += lenBytes
	if offset+int(codeLen) > len(data) {
		return errors.New("stop buffer too small for Code content")
	}
	s.Code = string(data[offset : offset+int(codeLen)])
	offset += int(codeLen)

	// Unmarshal Name
	if offset+lenBytes > len(data) {
		return errors.New("stop buffer too small for Name length")
	}
	nameLen := binary.BigEndian.Uint32(data[offset:])
	offset += lenBytes
	if offset+int(nameLen) > len(data) {
		return errors.New("stop buffer too small for Name content")
	}
	s.Name = string(data[offset : offset+int(nameLen)])
	offset += int(nameLen)

	// Unmarshal ParentID
	if offset+lenBytes > len(data) {
		return errors.New("stop buffer too small for ParentID length")
	}
	parentIDLen := binary.BigEndian.Uint32(data[offset:])
	offset += lenBytes
	if offset+int(parentIDLen) > len(data) {
		return errors.New("stop buffer too small for ParentID content")
	}
	s.ParentID = Key(data[offset : offset+int(parentIDLen)])
	offset += int(parentIDLen)

	// Unmarshal Location
	coordinateSize := float64Bytes * 2
	if offset+coordinateSize > len(data) {
		return errors.New("stop buffer too small for Location data")
	}
	err := s.Location.Decode(data[offset : offset+coordinateSize])
	if err != nil {
		return fmt.Errorf("failed to decode Location: %w", err)
	}
	offset += coordinateSize

	// Unmarshal LocationType
	if offset+uint8Bytes > len(data) {
		return errors.New("stop buffer too small for LocationType")
	}
	s.LocationType = LocationType(data[offset])
	offset += uint8Bytes

	// Unmarshal SupportedModes
	if offset+uint8Bytes > len(data) {
		return errors.New("stop buffer too small for SupportedModes")
	}
	s.SupportedModes = ModeFlag(data[offset])
	offset += uint8Bytes

	// Check if all data was consumed
	if offset != len(data) {
		return errors.New("stop buffer not fully consumed, trailing data exists")
	}

	return nil
}

// Parse a string into a ModeFlag
func parseModeFlag(mode string) ModeFlag {
	switch mode {
	case "Bus":
		return BusModeFlag
	case "School Bus":
		return SchoolBusModeFlag
	case "Rail":
		return RailModeFlag
	case "Ferry":
		return FerryModeFlag
	default:
		return UnknownModeFlag
	}
}

// Load and parse stops from the GTFS stops.txt file
func ParseStops(file io.Reader) (StopMap, error) {
	// Read file using CSV reader
	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	stops := make(StopMap)
	for i, record := range records {
		if i == 0 {
			continue // skip header
		}

		// Parse record into Stop struct
		id := Key(record[2])
		code := record[3]
		name := record[4]
		parentID := Key(record[1])

		lat, err := strconv.ParseFloat(record[6], 64)
		if err != nil {
			return nil, err
		}
		lon, err := strconv.ParseFloat(record[7], 64)
		if err != nil {
			return nil, err
		}
		location := Coordinate{
			Latitude:  lat,
			Longitude: lon,
		}

		typeInt, err := strconv.Atoi(record[0])
		if err != nil {
			typeInt = int(StopLocationType)
		}
		locationType := LocationType(typeInt)

		modes := ModeFlag(0)
		modeStrs := strings.SplitSeq(record[9], ",")
		for modeStr := range modeStrs {
			modes |= parseModeFlag(strings.TrimSpace(modeStr))
		}

		stops[id] = &Stop{
			ID:             id,
			Code:           code,
			Name:           name,
			ParentID:       parentID,
			Location:       location,
			LocationType:   locationType,
			SupportedModes: modes,
		}
	}

	return stops, nil
}
