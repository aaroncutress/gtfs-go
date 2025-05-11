package gtfs

import (
	"encoding/csv"
	"io"
	"strconv"
)

// Represents the shape of a transit route
type Shape struct {
	ID          Key
	Coordinates CoordinateArray
}
type ShapeMap map[Key]*Shape

// Encode serializes the Shape struct (excluding ID) into a byte slice.
// Format:
// - Coordinates: CoordinateArray (encoded as a byte slice)
func (s Shape) Encode() []byte {
	return s.Coordinates.Encode()
}

// Decode deserializes the byte slice into the Shape struct.
func (s *Shape) Decode(id Key, data []byte) error {
	// Decode the data into the Shape struct
	s.ID = id
	return s.Coordinates.Decode(data)
}

// Load and parse shapes from the GTFS shapes.txt file
func ParseShapes(file io.Reader) (ShapeMap, int, error) {
	// Read file using CSV reader
	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, 0, err
	}

	var currentID Key
	var currentCoordinates CoordinateArray

	shapes := make(ShapeMap)
	maxShapeLength := 0

	for i, record := range records {
		if i == 0 {
			continue // skip header
		}

		// Parse record into Shape struct
		id := Key(record[0])
		lat, err := strconv.ParseFloat(record[1], 64)
		if err != nil {
			return nil, 0, err
		}
		lon, err := strconv.ParseFloat(record[2], 64)
		if err != nil {
			return nil, 0, err
		}

		if id != currentID {
			if currentID != "" {
				shapes[currentID] = &Shape{
					ID:          currentID,
					Coordinates: currentCoordinates,
				}
				if len(currentCoordinates) > maxShapeLength {
					maxShapeLength = len(currentCoordinates)
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
		if len(currentCoordinates) > maxShapeLength {
			maxShapeLength = len(currentCoordinates)
		}
	}

	return shapes, maxShapeLength, nil
}
