package gtfs

import (
	"encoding/csv"
	"errors"
	"io"

	"github.com/kelindar/column"
)

// Represents an agency that provides transit services
type Agency struct {
	ID   Key
	Name string
	URL  string
}
type AgencyArray []*Agency
type AgencyMap map[Key]*Agency

// Saves an agency to the database
func (a Agency) Save(r column.Row) error {
	r.SetString("name", a.Name)
	r.SetString("url", a.URL)
	return nil
}

// Loads an agency from the database
func (a *Agency) Load(r column.Row) error {
	key, keyOk := r.Key()
	name, nameOk := r.String("name")
	url, urlOk := r.String("url")

	if !keyOk || !nameOk || !urlOk {
		return errors.New("missing required fields")
	}

	*a = Agency{
		ID:   Key(key),
		Name: name,
		URL:  url,
	}
	return nil
}

// Loads all agencies from the database transaction
func (aa *AgencyArray) Load(txn *column.Txn) error {
	idCol := txn.Key()
	nameCol := txn.String("name")
	urlCol := txn.String("url")

	count := txn.Count()
	if count == 0 {
		return nil
	}
	*aa = make(AgencyArray, count)

	var e error
	i := 0
	err := txn.Range(func(idx uint32) {
		id, idOk := idCol.Get()
		name, nameOk := nameCol.Get()
		url, urlOk := urlCol.Get()

		if !idOk || !nameOk || !urlOk {
			e = errors.New("missing required fields")
			return
		}

		(*aa)[i] = &Agency{
			ID:   Key(id),
			Name: name,
			URL:  url,
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

// Load and parse agencies from the GTFS agency.txt file
func ParseAgencies(file io.Reader) (AgencyMap, error) {
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

		agencies[id] = &Agency{
			ID:   id,
			Name: name,
			URL:  url,
		}
	}

	return agencies, nil
}
