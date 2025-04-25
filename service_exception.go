package gtfs

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
type ServiceExceptionArray []*ServiceException

// Saves a service exception to the database
func (se ServiceException) Save(row column.Row) error {
	row.SetUint("type", uint(se.Type))
	return nil
}

// Loads a service exception from the database
func (se *ServiceException) Load(row column.Row) error {
	key, keyOk := row.Key()
	typeInt, typeIntOk := row.Uint("type")

	if !keyOk || !typeIntOk {
		return errors.New("missing required fields")
	}

	service_id := key[:len(key)-8]
	dateStr := key[len(key)-8:]
	date, err := time.ParseInLocation("20060102", dateStr, time.UTC)
	if err != nil {
		return err
	}

	*se = ServiceException{
		ServiceID: Key(service_id),
		Date:      date,
		Type:      ExceptionType(typeInt),
	}
	return nil
}

// Loads all service exceptions from the database transaction
func (sea *ServiceExceptionArray) Load(txn *column.Txn) error {
	keyCol := txn.Key()
	typeCol := txn.Uint("type")

	count := txn.Count()
	if count == 0 {
		return nil
	}
	*sea = make(ServiceExceptionArray, count)

	var e error
	i := 0
	err := txn.Range(func(idx uint32) {
		key, keyOk := keyCol.Get()
		typeInt, typeIntOk := typeCol.Get()

		if !keyOk || !typeIntOk {
			e = errors.New("missing required fields")
			return
		}

		serviceID := key[:len(key)-8]
		date := key[len(key)-8:]
		exceptionDate, err := time.ParseInLocation("20060102", date, time.UTC)
		if err != nil {
			e = err
			return
		}

		(*sea)[i] = &ServiceException{
			ServiceID: Key(serviceID),
			Date:      exceptionDate,
			Type:      ExceptionType(typeInt),
		}
		i++
	})
	if err != nil {
		return err
	}
	if e != nil {
		return e
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

		exceptions[serviceID] = &ServiceException{
			ServiceID: serviceID,
			Date:      date,
			Type:      exceptionType,
		}
	}

	return exceptions, nil
}
