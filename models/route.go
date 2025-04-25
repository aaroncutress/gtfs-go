package models

import (
	"encoding/csv"
	"errors"
	"io"
	"strconv"

	"github.com/kelindar/column"
)

type RouteType uint8

const (
	TramRouteType RouteType = iota
	SubwayRouteType
	RailRouteType
	BusRouteType
	FerryRouteType
	CableCarRouteType
	GondolaRouteType
	FunicularRouteType
	TrolleybusRouteType = iota + 3
	MonorailRouteType
)

// Represents a route in a transit system
type Route struct {
	ID       Key
	AgencyID Key
	Name     string
	Type     RouteType
	Colour   string
	ShapeID  Key
	Stops    KeyArray
}
type RouteMap map[Key]*Route
type RouteArray []*Route

// Saves a route to the database
func (r Route) Save(row column.Row) error {
	row.SetString("agency_id", string(r.AgencyID))
	row.SetString("name", r.Name)
	row.SetUint("type", uint(r.Type))
	row.SetString("colour", r.Colour)
	row.SetString("shape_id", string(r.ShapeID))
	row.SetRecord("stops", r.Stops)

	return nil
}

// Loads a route from the database
func (r *Route) Load(row column.Row) error {
	key, keyOk := row.Key()
	agencyID, agencyIDOk := row.String("agency_id")
	name, nameOk := row.String("name")
	typeInt, typeIntOk := row.Uint("type")
	colour, colourOk := row.String("colour")
	shapeID, shapeIDOk := row.String("shape_id")
	stopsAny, stopsOk := row.Record("stops")

	if !keyOk || !agencyIDOk || !nameOk || !typeIntOk || !colourOk || !shapeIDOk || !stopsOk {
		return errors.New("missing required fields")
	}

	stops, ok := stopsAny.(*KeyArray)
	if !ok {
		return errors.New("invalid stops format")
	}

	*r = Route{
		ID:       Key(key),
		AgencyID: Key(agencyID),
		Name:     name,
		Type:     RouteType(typeInt),
		Colour:   colour,
		ShapeID:  Key(shapeID),
		Stops:    *stops,
	}

	return nil
}

// Loads all routes from the database transaction
func (ra *RouteArray) Load(txn *column.Txn) error {
	idCol := txn.Key()
	agencyIDCol := txn.String("agency_id")
	nameCol := txn.String("name")
	typeCol := txn.Uint("type")
	colourCol := txn.String("colour")
	shapeIDCol := txn.String("shape_id")
	stopsCol := txn.Record("stops")

	count := txn.Count()
	if count == 0 {
		return nil
	}
	*ra = make(RouteArray, count)

	var e error
	i := 0
	err := txn.Range(func(idx uint32) {
		id, idOk := idCol.Get()
		agencyID, agencyIDOk := agencyIDCol.Get()
		name, nameOk := nameCol.Get()
		typeInt, typeIntOk := typeCol.Get()
		colour, colourOk := colourCol.Get()
		shapeID, shapeIDOk := shapeIDCol.Get()
		stopsAny, stopsOk := stopsCol.Get()

		if !idOk || !agencyIDOk || !nameOk || !typeIntOk || !colourOk || !shapeIDOk || !stopsOk {
			e = errors.New("missing required fields")
			return
		}

		stops, ok := stopsAny.(*KeyArray)
		if !ok {
			e = errors.New("invalid stops format")
			return
		}

		(*ra)[i] = &Route{
			ID:       Key(id),
			AgencyID: Key(agencyID),
			Name:     name,
			Type:     RouteType(typeInt),
			Colour:   colour,
			ShapeID:  Key(shapeID),
			Stops:    *stops,
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

// Load and parse routes from the GTFS routes.txt file
func ParseRoutes(file io.Reader) (RouteMap, error) {
	// Read file using CSV reader
	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	routes := make(RouteMap)
	for i, record := range records {
		if i == 0 {
			continue // skip header
		}

		// Parse record into Route struct
		id := Key(record[0])
		agencyID := Key(record[1])
		name := record[2]
		if name == "" {
			name = record[3]
		}

		typeInt, err := strconv.Atoi(record[5])
		if err != nil {
			return nil, err
		}
		typeRoute := RouteType(typeInt)
		colour := record[7]

		routes[id] = &Route{
			ID:       id,
			AgencyID: agencyID,
			Name:     name,
			Type:     typeRoute,
			Colour:   colour,
		}
	}

	return routes, nil
}
