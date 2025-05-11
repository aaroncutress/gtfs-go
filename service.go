package gtfs

import (
	"encoding/binary"
	"encoding/csv"
	"errors"
	"io"
	"strconv"
	"time"
)

// Flag for each day of the week
type WeekdayFlag uint8

const (
	MondayWeekdayFlag WeekdayFlag = 1 << iota
	TuesdayWeekdayFlag
	WednesdayWeekdayFlag
	ThursdayWeekdayFlag
	FridayWeekdayFlag
	SaturdayWeekdayFlag
	SundayWeekdayFlag
)

// Represents the days of the week a service is active
type Service struct {
	ID        Key
	Weekdays  WeekdayFlag
	StartDate time.Time
	EndDate   time.Time
}
type ServiceMap map[Key]*Service

// Encode serializes the Service struct (excluding ID) into a byte slice.
// Format:
// - Weekdays: 1 byte (bitmask for each day of the week)
// - StartDate: 8 bytes (Unix timestamp)
// - EndDate: 8 bytes (Unix timestamp)
func (s Service) Encode() []byte {
	// Calculate total length
	// 1 byte for Weekdays + 8 bytes for StartDate + 8 bytes for EndDate
	totalLen := uint8Bytes + timeBytes + timeBytes
	data := make([]byte, totalLen)
	offset := 0

	// Marshal Weekdays
	data[offset] = byte(s.Weekdays)
	offset += 1

	// Marshal StartDate as Unix timestamp (int64)
	binary.BigEndian.PutUint64(data[offset:], uint64(s.StartDate.Unix()))
	offset += timeBytes

	// Marshal EndDate as Unix timestamp (int64)
	binary.BigEndian.PutUint64(data[offset:], uint64(s.EndDate.Unix()))
	// offset += timeBytes // Not strictly needed for the last field

	return data
}

// Decode deserializes the byte slice into the Service struct.
func (s *Service) Decode(id Key, data []byte) error {
	if s == nil {
		return errors.New("cannot decode into a nil Service")
	}
	offset := 0

	// Set ID from parameter
	s.ID = id

	// Unmarshal Weekdays
	if offset+1 > len(data) {
		return errors.New("service buffer too small for Weekdays")
	}
	s.Weekdays = WeekdayFlag(data[offset])
	offset += 1

	// Unmarshal StartDate
	if offset+timeBytes > len(data) {
		return errors.New("service buffer too small for StartDate")
	}
	startDateUnix := int64(binary.BigEndian.Uint64(data[offset:]))
	s.StartDate = time.Unix(startDateUnix, 0).UTC() // Store as UTC, or choose a specific location
	offset += timeBytes

	// Unmarshal EndDate
	if offset+timeBytes > len(data) {
		return errors.New("service buffer too small for EndDate")
	}
	endDateUnix := int64(binary.BigEndian.Uint64(data[offset:]))
	s.EndDate = time.Unix(endDateUnix, 0).UTC() // Store as UTC, or choose a specific location
	offset += timeBytes

	// Check if all data was consumed
	if offset != len(data) {
		return errors.New("service buffer not fully consumed, trailing data exists")
	}

	return nil
}

// Parses a weekday flag from the GTFS calendar.txt file
func parseWeekdayFlag(day string, flag WeekdayFlag) WeekdayFlag {
	dayInt, err := strconv.Atoi(day)
	if err == nil && dayInt == 1 {
		return flag
	}
	return 0
}

// Load and parse services from the GTFS calendar.txt file
func ParseServices(file io.Reader) (ServiceMap, error) {
	// Read file using CSV reader
	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	services := make(ServiceMap)
	for i, record := range records {
		if i == 0 {
			continue // skip header
		}

		// Parse record into Service struct
		id := Key(record[0])
		startDate, err := time.ParseInLocation("20060102", record[8], time.UTC)
		if err != nil {
			return nil, err
		}
		endDate, err := time.ParseInLocation("20060102", record[9], time.UTC)
		if err != nil {
			return nil, err
		}
		weekdays := parseWeekdayFlag(record[1], MondayWeekdayFlag) |
			parseWeekdayFlag(record[2], TuesdayWeekdayFlag) |
			parseWeekdayFlag(record[3], WednesdayWeekdayFlag) |
			parseWeekdayFlag(record[4], ThursdayWeekdayFlag) |
			parseWeekdayFlag(record[5], FridayWeekdayFlag) |
			parseWeekdayFlag(record[6], SaturdayWeekdayFlag) |
			parseWeekdayFlag(record[7], SundayWeekdayFlag)

		services[id] = &Service{
			ID:        id,
			Weekdays:  weekdays,
			StartDate: startDate,
			EndDate:   endDate,
		}
	}

	return services, nil
}
