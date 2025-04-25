package models

import (
	"encoding/csv"
	"errors"
	"io"
	"strconv"

	"github.com/kelindar/column"
)

// Represents the shape of a transit route
type Shape struct {
	ID          Key
	Coordinates CoordinateArray
}
type ShapeMap map[Key]*Shape
type ShapeArray []*Shape

// Saves a shape to the database
func (s Shape) Save(row column.Row, coordsPerRow int) error {
	i := 0
	for currentRow := 0; currentRow < len(s.Coordinates); currentRow += coordsPerRow {
		// Get the next set of coordinates
		endRow := min(currentRow+coordsPerRow, len(s.Coordinates))
		coords := s.Coordinates[currentRow:endRow]

		// Set the coordinates in the row
		row.SetRecord("coordinates"+strconv.Itoa(i), coords)
		i++
	}

	return nil
}

// Loads a shape from the database
func (s *Shape) Load(row column.Row, numRows int) error {
	key, keyOk := row.Key()

	var coordinatesAnyAll []any
	for i := range numRows {
		coordinatesAny, coordinatesOk := row.Record("coordinates" + strconv.Itoa(i))
		if !coordinatesOk {
			if i == 0 {
				return errors.New("missing required fields")
			}
			break
		}
		coordinatesAnyAll = append(coordinatesAnyAll, coordinatesAny)
	}

	if !keyOk {
		return errors.New("missing required fields")
	}

	coordinates := make([]Coordinate, 0)
	for _, coordinatesAny := range coordinatesAnyAll {
		coords, ok := coordinatesAny.(*CoordinateArray)
		if !ok {
			return errors.New("invalid coordinates format")
		}
		coordinates = append(coordinates, *coords...)
	}

	*s = Shape{
		ID:          Key(key),
		Coordinates: coordinates,
	}
	return nil
}

// Loads all shapes from the database transaction
func (sa *ShapeArray) Load(txn *column.Txn, numRows int) error {
	idCol := txn.Key()

	count := txn.Count()
	if count == 0 {
		return nil
	}
	*sa = make(ShapeArray, count)

	var e error
	i := 0
	err := txn.Range(func(idx uint32) {
		id, idOk := idCol.Get()
		var coordinatesAnyAll []any
		for i := range numRows {
			coordinatesAny, coordinatesOk := txn.Record("coordinates" + strconv.Itoa(i)).Get()
			if !coordinatesOk {
				if i == 0 {
					e = errors.New("missing required fields")
					return
				}
				break
			}
			coordinatesAnyAll = append(coordinatesAnyAll, coordinatesAny)
		}

		if !idOk {
			e = errors.New("missing required fields")
			return
		}

		coordinates := make(CoordinateArray, 0)
		for _, coordinatesAny := range coordinatesAnyAll {
			coords, ok := coordinatesAny.(*CoordinateArray)
			if !ok {
				e = errors.New("invalid coordinates format")
				return
			}
			coordinates = append(coordinates, *coords...)
		}

		(*sa)[i] = &Shape{
			ID:          Key(id),
			Coordinates: coordinates,
		}
		i++
	})

	if e != nil {
		return e
	}
	return err
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
