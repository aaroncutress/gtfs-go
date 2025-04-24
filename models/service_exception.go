package models

import (
	"encoding/csv"
	"errors"
	"io"
	"time"

	"github.com/kelindar/column"
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

// Saves the service exception to the database
func (se ServiceException) Save(row column.Row) error {
	row.SetString("service_id", string(se.ServiceID))
	row.SetString("date", se.Date.Format("20060102"))
	row.SetUint("type", uint(se.Type))
	return nil
}

// Loads the service exception from the database
func (se *ServiceException) Load(row column.Row) error {
	key, keyOk := row.Key()
	date, dateOk := row.String("date")
	typeInt, typeIntOk := row.Uint("type")

	if !keyOk || !dateOk || !typeIntOk {
		return errors.New("missing required fields")
	}

	se.ServiceID = Key(key)
	se.Date, _ = time.Parse("20060102", date)
	se.Type = ExceptionType(typeInt)
	return nil
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
