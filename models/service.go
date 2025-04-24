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

// Saves the service to the database
func (s Service) Save(row column.Row) error {
	row.SetUint("weekdays", uint(s.Weekdays))
	row.SetString("start_date", s.StartDate.Format("20060102"))
	row.SetString("end_date", s.EndDate.Format("20060102"))
	return nil
}

// Loads the service from the database
func (s *Service) Load(row column.Row) error {
	key, keyOk := row.Key()
	weekdays, weekdaysOk := row.Uint("weekdays")
	startDate, startDateOk := row.String("start_date")
	endDate, endDateOk := row.String("end_date")

	if !keyOk || !weekdaysOk || !startDateOk || !endDateOk {
		return errors.New("missing required fields")
	}

	s.ID = Key(key)
	s.Weekdays = WeekdayFlag(weekdays)
	s.StartDate, _ = time.Parse("20060102", startDate)
	s.EndDate, _ = time.Parse("20060102", endDate)
	return nil
}

func parseWeekdayFlag(day string, flag WeekdayFlag) WeekdayFlag {
	dayInt, err := strconv.Atoi(day)
	if err == nil && dayInt == 1 {
		return flag
	}
	return 0
}

// Load services from the GTFS calendar.txt file
func LoadServices(file io.Reader) (ServiceMap, error) {
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
		startDate, err := time.Parse("20060102", record[8])
		if err != nil {
			return nil, err
		}
		endDate, err := time.Parse("20060102", record[9])
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
