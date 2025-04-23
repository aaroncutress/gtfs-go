package models

import (
	"database/sql"
	"encoding/csv"
	"io"
	"sort"
	"strconv"
)

// Represents the shape of a transit route
type Shape struct {
	ID          Key
	Coordinates []Coordinate
}
type ShapeMap map[Key]*Shape

// Encode the Shape struct into a slice of records
func (s *Shape) Encode() [][]any {
	records := make([][]any, len(s.Coordinates))
	for i, coord := range s.Coordinates {
		records[i] = []any{
			string(s.ID),
			i,
			coord.String(),
		}
	}
	return records
}

// Decode a slice of records into a Shape struct
func DecodeShape(records *sql.Rows) (*Shape, error) {
	// if len(records) == 0 {
	// 	return nil, nil
	// }

	// id := Key(records[0][0].(string))
	// coordinates := make([]Coordinate, len(records))
	// for _, record := range records {
	// 	if len(record) < 3 {
	// 		return nil, nil
	// 	}

	// 	coordStr := record[2].(string)
	// 	coord, err := NewCoordinateFromString(coordStr)
	// 	if err != nil {
	// 		return nil, err
	// 	}

	// 	i, err := strconv.Atoi(record[1].(string))
	// 	if err != nil {
	// 		return nil, err
	// 	}

	// 	coordinates[i] = coord
	// }

	var id, coordStr string
	var seq int
	coordinates := make([]Coordinate, 0)
	sequences := make([]int, 0)

	for records.Next() {
		err := records.Scan(&id, &seq, &coordStr)
		if err != nil {
			return nil, err
		}
		coord, err := NewCoordinateFromString(coordStr)
		if err != nil {
			return nil, err
		}
		coordinates = append(coordinates, coord)
		sequences = append(sequences, seq)
	}

	// Sort coordinates by sequence
	sort.Slice(coordinates, func(i, j int) bool {
		return sequences[i] < sequences[j]
	})

	return &Shape{
		ID:          Key(id),
		Coordinates: coordinates,
	}, nil
}

// Load shapes from the GTFS shapes.txt file
func LoadShapes(file io.Reader) (ShapeMap, error) {
	// Read file using CSV reader
	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	var currentID Key
	var currentCoordinates []Coordinate

	shapes := make(ShapeMap)
	for i, record := range records {
		if i == 0 {
			continue // skip header
		}

		// Parse record into Shape struct
		id := Key(record[0])
		lat, err := strconv.ParseFloat(record[1], 64)
		if err != nil {
			return nil, err
		}
		lon, err := strconv.ParseFloat(record[2], 64)
		if err != nil {
			return nil, err
		}

		if id != currentID {
			if currentID != "" {
				shapes[currentID] = &Shape{
					ID:          currentID,
					Coordinates: currentCoordinates,
				}
			}
			currentID = id
			currentCoordinates = []Coordinate{}
		}
		coordinate := Coordinate{
			Latitude:  lat,
			Longitude: lon,
		}
		currentCoordinates = append(currentCoordinates, coordinate)
	}

	// Add the last shape
	if currentID != "" {
		shapes[currentID] = &Shape{
			ID:          currentID,
			Coordinates: currentCoordinates,
		}
	}

	return shapes, nil
}
