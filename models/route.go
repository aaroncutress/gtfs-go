package models

import (
	"database/sql"
	"encoding/csv"
	"io"
	"strconv"
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
	Stops    []Key
}
type RouteMap map[Key]*Route

// Encode the Route struct into a record
func (r *Route) Encode() []any {
	return []any{
		string(r.ID),
		string(r.AgencyID),
		r.Name,
		int(r.Type),
		r.Colour,
		string(r.ShapeID),
	}
}

// Encode the Route stops into a list of records
func (r *Route) EncodeStops() [][]any {
	records := make([][]any, len(r.Stops))
	for i, stopID := range r.Stops {
		records[i] = []any{
			string(r.ID),
			string(stopID),
		}
	}
	return records
}

// Decode a record into a Route struct
func DecodeRoute(record *sql.Row, routeStopRecords *sql.Rows) (*Route, error) {
	// Decode the record into a Route struct
	var id, agencyID, name, colour, shapeID string
	var typeInt int
	err := record.Scan(&id, &agencyID, &name, &typeInt, &colour, &shapeID)
	if err != nil {
		return nil, err
	}
	typeRoute := RouteType(typeInt)

	route := &Route{
		ID:       Key(id),
		AgencyID: Key(agencyID),
		Name:     name,
		Type:     typeRoute,
		Colour:   colour,
		ShapeID:  Key(shapeID),
	}

	// Parse route stops
	route.Stops = make([]Key, 0)
	for routeStopRecords.Next() {
		var routeID, stopID string
		err := routeStopRecords.Scan(&routeID, &stopID)
		if err != nil {
			return nil, err
		}
		route.Stops = append(route.Stops, Key(stopID))
	}

	return route, nil
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
