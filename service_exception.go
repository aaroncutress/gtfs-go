package gtfs

import (
	"encoding/binary"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"time"
)

// Enum for the types of service exception
type ExceptionType bool

const (
	AddedExceptionType   ExceptionType = false
	RemovedExceptionType ExceptionType = true
)

// Represents an exception for a service on a specific date
type ServiceException struct {
	ServiceID Key
	Date      time.Time
	Type      ExceptionType
}
type ServiceExceptionKey struct {
	ServiceID Key
	Date      time.Time
}
type ServiceExceptionMap map[ServiceExceptionKey]*ServiceException

// Encode serializes the ServiceException struct into a byte slice.
// Format:
// - ServiceID: 4-byte length + UTF-8 string
// - Date: 8 bytes (Unix timestamp)
// - Type: 1 byte (bool as uint8)
func (se ServiceException) Encode() []byte {
	serviceIDStr := string(se.ServiceID)

	// Calculate total length
	totalLen := lenBytes + len(serviceIDStr) + // ServiceID
		timeBytes + // Date
		boolBytes // Type

	data := make([]byte, totalLen)
	offset := 0

	// Marshal ServiceID
	binary.BigEndian.PutUint32(data[offset:], uint32(len(serviceIDStr)))
	offset += lenBytes
	copy(data[offset:], serviceIDStr)
	offset += len(serviceIDStr)

	// Marshal Date as Unix timestamp (int64)
	binary.BigEndian.PutUint64(data[offset:], uint64(se.Date.Unix()))
	offset += timeBytes

	// Marshal Type (bool as uint8)
	if se.Type {
		data[offset] = 1
	} else {
		data[offset] = 0
	}
	// offset += boolBytes // Not strictly needed for the last field

	return data
}

// Decode deserializes the byte slice into the ServiceException struct.
func (se *ServiceException) Decode(data []byte) error {
	if se == nil {
		return errors.New("cannot decode into a nil ServiceException")
	}
	offset := 0

	// Unmarshal ServiceID
	if offset+lenBytes > len(data) {
		return errors.New("buffer too small for ServiceID length")
	}
	serviceIDLen := binary.BigEndian.Uint32(data[offset:])
	offset += lenBytes
	if offset+int(serviceIDLen) > len(data) {
		return errors.New("buffer too small for ServiceID content")
	}
	se.ServiceID = Key(data[offset : offset+int(serviceIDLen)])
	offset += int(serviceIDLen)

	// Unmarshal Date
	if offset+timeBytes > len(data) {
		return errors.New("buffer too small for Date")
	}
	dateUnix := int64(binary.BigEndian.Uint64(data[offset:]))
	se.Date = time.Unix(dateUnix, 0).UTC() // Store as UTC, or choose a specific location
	offset += timeBytes

	// Unmarshal Type
	if offset+boolBytes > len(data) {
		return errors.New("buffer too small for Type")
	}
	if data[offset] == 1 {
		se.Type = true
	} else if data[offset] == 0 {
		se.Type = false
	} else {
		return fmt.Errorf("invalid byte value for bool (Type): got %d, want 0 or 1", data[offset])
	}
	offset += boolBytes

	// Check if all data was consumed
	if offset != len(data) {
		return errors.New("buffer not fully consumed, trailing data exists")
	}

	return nil
}

// Load and parse service exceptions from the GTFS calendar_dates.txt file
func ParseServiceExceptions(file io.Reader) (ServiceExceptionMap, error) {
	// Read file using CSV reader
	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	exceptions := make(ServiceExceptionMap)
	for i, record := range records {
		if i == 0 {
			continue // skip header
		}

		// Parse record into ServiceException struct
		serviceID := Key(record[0])
		date, err := time.ParseInLocation("20060102", record[1], time.UTC)
		if err != nil {
			return nil, err
		}
		var exceptionType ExceptionType
		switch record[2] {
		case "1":
			exceptionType = AddedExceptionType
		case "2":
			exceptionType = RemovedExceptionType
		default:
			return nil, errors.New("invalid exception type")
		}

		key := ServiceExceptionKey{
			ServiceID: serviceID,
			Date:      date,
		}

		exceptions[key] = &ServiceException{
			ServiceID: serviceID,
			Date:      date,
			Type:      exceptionType,
		}
	}

	return exceptions, nil
}
