package models

import (
	"encoding/csv"
	"errors"
	"io"

	"github.com/kelindar/column"
)

// Represents an agency that provides transit services
type Agency struct {
	ID       Key
	Name     string
	URL      string
	Timezone string
}
type AgencyMap map[Key]*Agency

// Saves the agency to the database
func (a Agency) Save(r column.Row) error {
	r.SetString("name", a.Name)
	r.SetString("url", a.URL)
	r.SetString("timezone", a.Timezone)
	return nil
}

// Loads the agency from the database
func (a *Agency) Load(r column.Row) error {
	key, keyOk := r.Key()
	name, nameOk := r.String("name")
	url, urlOk := r.String("url")
	timezone, timezoneOk := r.String("timezone")

	if !keyOk || !nameOk || !urlOk || !timezoneOk {
		return errors.New("missing required fields")
	}

	a.ID = Key(key)
	a.Name = name
	a.URL = url
	a.Timezone = timezone
	return nil
}

// LoadAllAgencies loads all agencies from the database
func LoadAllAgencies(txn *column.Txn) ([]*Agency, error) {
	var agencies []*Agency

	idCol := txn.Key()
	nameCol := txn.String("name")
	urlCol := txn.String("url")
	timezoneCol := txn.String("timezone")

	var e error
	err := txn.Range(func(i uint32) {
		id, idOk := idCol.Get()
		name, nameOk := nameCol.Get()
		url, urlOk := urlCol.Get()
		timezone, timezoneOk := timezoneCol.Get()

		if !idOk || !nameOk || !urlOk || !timezoneOk {
			e = errors.New("missing required fields")
			return
		}

		agencies = append(agencies, &Agency{
			ID:       Key(id),
			Name:     name,
			URL:      url,
			Timezone: timezone,
		})
	})

	if err != nil {
		return nil, err
	}
	if e != nil {
		return nil, e
	}

	return agencies, nil
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
