package models

import (
	"database/sql"
	"encoding/csv"
	"errors"
	"io"
	"time"
)

// Enum for the types of service exception
type ExceptionType uint8

const (
	AddedExceptionType ExceptionType = iota + 1
	RemovedExceptionType
)

// Represents an exception for a service on a specific date
type ServiceException struct {
	ServiceID Key
	Date      time.Time
	Type      ExceptionType
}
type ServiceExceptionMap map[Key]*ServiceException

// Encode the ServiceException struct into a record
func (se *ServiceException) Encode() []any {
	return []any{
		string(se.ServiceID),
		se.Date.Format("20060102"),
		int(se.Type),
	}
}

// Decode records into ServiceException structs
func DecodeServiceExceptions(record *sql.Rows) ([]*ServiceException, error) {
	serviceExceptions := make([]*ServiceException, 0)

	for record.Next() {
		var serviceID, dateStr string
		var exceptionTypeInt int

		err := record.Scan(&serviceID, &dateStr, &exceptionTypeInt)
		if err != nil {
			return nil, err
		}

		date, err := time.Parse("20060102", dateStr)
		if err != nil {
			return nil, err
		}

		serviceExceptions = append(serviceExceptions, &ServiceException{
			ServiceID: Key(serviceID),
			Date:      date,
			Type:      ExceptionType(exceptionTypeInt),
		})
	}

	if err := record.Err(); err != nil {
		return nil, err
	}

	return serviceExceptions, nil
}

// Load service exceptions from the GTFS calendar_dates.txt file
func LoadServiceExceptions(file io.Reader) (ServiceExceptionMap, error) {
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
		date, err := time.Parse("20060102", record[1])
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

		exceptions[serviceID] = &ServiceException{
			ServiceID: serviceID,
			Date:      date,
			Type:      exceptionType,
		}
	}

	return exceptions, nil
}
