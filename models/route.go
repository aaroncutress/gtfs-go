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

// Saves the route to the database
func (r Route) Save(row column.Row) error {
	row.SetString("agency_id", string(r.AgencyID))
	row.SetString("name", r.Name)
	row.SetUint("type", uint(r.Type))
	row.SetString("colour", r.Colour)
	row.SetString("shape_id", string(r.ShapeID))
	row.SetRecord("stops", r.Stops)

	return nil
}

// Loads the route from the database
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

	r.ID = Key(key)
	r.AgencyID = Key(agencyID)
	r.Name = name
	r.Type = RouteType(typeInt)
	r.Colour = colour
	r.ShapeID = Key(shapeID)
	r.Stops = *stops

	return nil
}

// Load routes from the GTFS routes.txt file
func LoadRoutes(file io.Reader) (RouteMap, error) {
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
