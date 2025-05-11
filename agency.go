package gtfs

import (
	"encoding/binary"
	"encoding/csv"
	"errors"
	"io"
)

// Represents an agency that provides transit services
type Agency struct {
	ID       Key
	Name     string
	URL      string
	Timezone string
}
type AgencyMap map[Key]*Agency

// Encode serializes the Agency struct (excluding ID) into a byte slice.
// Format:
// - Name: 4-byte length + UTF-8 string
// - URL: 4-byte length + UTF-8 string
// - Timezone: 4-byte length + UTF-8 string
func (a Agency) Encode() []byte {
	// This assumes ID is handled separately or not part of this particular encoding
	nameStr := a.Name
	urlStr := a.URL
	timezoneStr := a.Timezone

	totalLen := lenBytes + len(nameStr) +
		lenBytes + len(urlStr) +
		lenBytes + len(timezoneStr)

	data := make([]byte, totalLen)
	offset := 0

	// Marshal Name
	binary.BigEndian.PutUint32(data[offset:], uint32(len(nameStr)))
	offset += lenBytes
	copy(data[offset:], nameStr)
	offset += len(nameStr)

	// Marshal URL
	binary.BigEndian.PutUint32(data[offset:], uint32(len(urlStr)))
	offset += lenBytes
	copy(data[offset:], urlStr)
	offset += len(urlStr)

	// Marshal Timezone
	binary.BigEndian.PutUint32(data[offset:], uint32(len(timezoneStr)))
	offset += lenBytes
	copy(data[offset:], timezoneStr)
	// offset += len(timezoneStr) // Not strictly needed for the last field

	return data
}

// Decode deserializes the byte slice into the Agency struct.
func (a *Agency) Decode(id Key, data []byte) error {
	if a == nil {
		return errors.New("cannot decode into a nil Agency")
	}
	offset := 0

	a.ID = id // Set ID from parameter

	// Unmarshal Name
	if offset+lenBytes > len(data) {
		return errors.New("buffer too small for Agency Name length")
	}
	nameLen := binary.BigEndian.Uint32(data[offset:])
	offset += lenBytes
	if offset+int(nameLen) > len(data) {
		return errors.New("buffer too small for Agency Name content")
	}
	a.Name = string(data[offset : offset+int(nameLen)])
	offset += int(nameLen)

	// Unmarshal URL
	if offset+lenBytes > len(data) {
		return errors.New("buffer too small for Agency URL length")
	}
	urlLen := binary.BigEndian.Uint32(data[offset:])
	offset += lenBytes
	if offset+int(urlLen) > len(data) {
		return errors.New("buffer too small for Agency URL content")
	}
	a.URL = string(data[offset : offset+int(urlLen)])
	offset += int(urlLen)

	// Unmarshal Timezone
	if offset+lenBytes > len(data) {
		return errors.New("buffer too small for Agency Timezone length")
	}
	timezoneLen := binary.BigEndian.Uint32(data[offset:])
	offset += lenBytes
	if offset+int(timezoneLen) > len(data) {
		return errors.New("buffer too small for Agency Timezone content")
	}
	a.Timezone = string(data[offset : offset+int(timezoneLen)])
	offset += int(timezoneLen)

	if offset != len(data) {
		return errors.New("agency buffer not fully consumed, trailing data exists")
	}
	return nil
}

// Load and parse agencies from the GTFS agency.txt file
func ParseAgencies(file io.Reader) (AgencyMap, error) {
	// Read file using CSV reader
	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	agencies := make(AgencyMap)
	for i, record := range records {
		if i == 0 {
			continue // skip header
		}

		// Parse record into Agency struct
		id := Key(record[0])
		name := record[1]
		url := record[2]
		timezone := record[3]

		agencies[id] = &Agency{
			ID:       id,
			Name:     name,
			URL:      url,
			Timezone: timezone,
		}
	}

	return agencies, nil
}
