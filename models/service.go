package models

import (
	"database/sql"
	"encoding/csv"
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

// Encodes the Service struct into a record
func (s *Service) Encode() []any {
	return []any{
		string(s.ID),
		int(s.Weekdays),
		s.StartDate.Format("20060102"),
		s.EndDate.Format("20060102"),
	}
}

// Decodes a record into a Service struct
func DecodeService(record *sql.Row) (*Service, error) {
	var id, startDateStr, endDateStr string
	var weekdaysInt int
	err := record.Scan(&id, &weekdaysInt, &startDateStr, &endDateStr)
	if err != nil {
		return nil, err
	}

	startDate, err := time.Parse("20060102", startDateStr)
	if err != nil {
		return nil, err
	}

	endDate, err := time.Parse("20060102", endDateStr)
	if err != nil {
		return nil, err
	}

	return &Service{
		ID:        Key(id),
		Weekdays:  WeekdayFlag(weekdaysInt),
		StartDate: startDate,
		EndDate:   endDate,
	}, nil
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
