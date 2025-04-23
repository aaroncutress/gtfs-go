package models

import (
	"database/sql"
	"encoding/csv"
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

// Encode the Agency struct into a record
func (a *Agency) Encode() []any {
	return []any{
		string(a.ID),
		a.Name,
		a.URL,
		a.Timezone,
	}
}

// Decode a record into an Agency struct
func DecodeAgency(record *sql.Row) (*Agency, error) {
	var id, name, url, timezone string
	err := record.Scan(&id, &name, &url, &timezone)
	if err != nil {
		return nil, err
	}

	return &Agency{
		ID:       Key(id),
		Name:     name,
		URL:      url,
		Timezone: timezone,
	}, nil
}

// Load agencies from the GTFS agency.txt file
func LoadAgencies(file io.Reader) (AgencyMap, error) {
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
