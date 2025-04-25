package models

import (
	"encoding/csv"
	"errors"
	"io"
	"strconv"
	"time"

	"github.com/kelindar/column"
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
type ServiceArray []*Service

// Saves a service to the database
func (s Service) Save(row column.Row) error {
	row.SetUint("weekdays", uint(s.Weekdays))
	row.SetString("start_date", s.StartDate.Format("20060102"))
	row.SetString("end_date", s.EndDate.Format("20060102"))
	return nil
}

// Loads a service from the database
func (s *Service) Load(row column.Row) error {
	key, keyOk := row.Key()
	weekdays, weekdaysOk := row.Uint("weekdays")
	startDateStr, startDateOk := row.String("start_date")
	endDateStr, endDateOk := row.String("end_date")

	if !keyOk || !weekdaysOk || !startDateOk || !endDateOk {
		return errors.New("missing required fields")
	}

	startDate, err := time.ParseInLocation("20060102", startDateStr, time.UTC)
	if err != nil {
		return err
	}
	endDate, err := time.ParseInLocation("20060102", endDateStr, time.UTC)
	if err != nil {
		return err
	}

	*s = Service{
		ID:        Key(key),
		Weekdays:  WeekdayFlag(weekdays),
		StartDate: startDate.UTC(),
		EndDate:   endDate.UTC(),
	}
	return nil
}

// Loads all services from the database transaction
func (sa *ServiceArray) Load(txn *column.Txn) error {
	idCol := txn.Key()
	weekdaysCol := txn.Uint("weekdays")
	startDateStrCol := txn.String("start_date")
	endDateStrCol := txn.String("end_date")

	count := txn.Count()
	if count == 0 {
		return nil
	}
	*sa = make(ServiceArray, count)

	var e error
	i := 0
	err := txn.Range(func(idx uint32) {
		id, idOk := idCol.Get()
		weekdays, weekdaysOk := weekdaysCol.Get()
		startDateStr, startDateOk := startDateStrCol.Get()
		endDateStr, endDateOk := endDateStrCol.Get()

		if !idOk || !weekdaysOk || !startDateOk || !endDateOk {
			e = errors.New("missing required fields")
			return
		}

		startDate, err := time.ParseInLocation("20060102", startDateStr, time.UTC)
		if err != nil {
			e = err
			return
		}
		endDate, err := time.ParseInLocation("20060102", endDateStr, time.UTC)
		if err != nil {
			e = err
			return
		}

		(*sa)[i] = &Service{
			ID:        Key(id),
			Weekdays:  WeekdayFlag(weekdays),
			StartDate: startDate,
			EndDate:   endDate,
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
