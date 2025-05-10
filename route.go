package gtfs

import (
	"encoding/binary"
	"encoding/csv"
	"errors"
	"fmt"
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
	Stops    KeyArray
}
type RouteMap map[Key]*Route
type RouteArray []*Route

// Encode the Route struct into a byte slice
// Format:
// - AgencyID: 4-byte length + UTF-8 string
// - Name: 4-byte length + UTF-8 string
// - Type: 1-byte enum (RouteType)
// - Colour: 4-byte length + UTF-8 string
// - ShapeID: 4-byte length + UTF-8 string
// - Stops: KeyArray (encoded as a byte slice)
func (r Route) Encode() []byte {
	agencyIDStr := string(r.AgencyID)
	nameStr := r.Name
	colourStr := r.Colour
	shapeIDStr := string(r.ShapeID)

	// Encode Stops field first to get its byte representation and length
	stopsBytes := r.Stops.Encode()

	// Calculate total length for fixed fields + length of encoded stops
	totalLen := lenBytes + len(agencyIDStr) + // AgencyID
		lenBytes + len(nameStr) + // Name
		uint8Bytes + // Type (uint8)
		lenBytes + len(colourStr) + // Colour
		lenBytes + len(shapeIDStr) + // ShapeID
		len(stopsBytes) // Length of encoded Stops data

	data := make([]byte, totalLen)
	offset := 0

	// Marshal AgencyID
	binary.BigEndian.PutUint32(data[offset:], uint32(len(agencyIDStr)))
	offset += lenBytes
	copy(data[offset:], agencyIDStr)
	offset += len(agencyIDStr)

	// Marshal Name
	binary.BigEndian.PutUint32(data[offset:], uint32(len(nameStr)))
	offset += lenBytes
	copy(data[offset:], nameStr)
	offset += len(nameStr)

	// Marshal Type
	data[offset] = byte(r.Type)
	offset += 1

	// Marshal Colour
	binary.BigEndian.PutUint32(data[offset:], uint32(len(colourStr)))
	offset += lenBytes
	copy(data[offset:], colourStr)
	offset += len(colourStr)

	// Marshal ShapeID
	binary.BigEndian.PutUint32(data[offset:], uint32(len(shapeIDStr)))
	offset += lenBytes
	copy(data[offset:], shapeIDStr)
	offset += len(shapeIDStr)

	// Append encoded Stops data
	copy(data[offset:], stopsBytes)

	return data
}

// Decode the byte slice into the Route struct
func (r *Route) Decode(id Key, data []byte) error {
	if r == nil {
		return errors.New("cannot decode into a nil Route")
	}
	offset := 0

	// Set ID from parameter
	r.ID = id

	// Unmarshal AgencyID
	if offset+lenBytes > len(data) {
		return errors.New("buffer too small for AgencyID length")
	}
	agencyIDLen := binary.BigEndian.Uint32(data[offset:])
	offset += lenBytes
	if offset+int(agencyIDLen) > len(data) {
		return errors.New("buffer too small for AgencyID content")
	}
	r.AgencyID = Key(data[offset : offset+int(agencyIDLen)])
	offset += int(agencyIDLen)

	// Unmarshal Name
	if offset+lenBytes > len(data) {
		return errors.New("buffer too small for Name length")
	}
	nameLen := binary.BigEndian.Uint32(data[offset:])
	offset += lenBytes
	if offset+int(nameLen) > len(data) {
		return errors.New("buffer too small for Name content")
	}
	r.Name = string(data[offset : offset+int(nameLen)])
	offset += int(nameLen)

	// Unmarshal Type
	if offset+1 > len(data) {
		return errors.New("buffer too small for Type")
	}
	r.Type = RouteType(data[offset])
	offset += 1

	// Unmarshal Colour
	if offset+lenBytes > len(data) {
		return errors.New("buffer too small for Colour length")
	}
	colourLen := binary.BigEndian.Uint32(data[offset:])
	offset += lenBytes
	if offset+int(colourLen) > len(data) {
		return errors.New("buffer too small for Colour content")
	}
	r.Colour = string(data[offset : offset+int(colourLen)])
	offset += int(colourLen)

	// Unmarshal ShapeID
	if offset+lenBytes > len(data) {
		return errors.New("buffer too small for ShapeID length")
	}
	shapeIDLen := binary.BigEndian.Uint32(data[offset:])
	offset += lenBytes
	if offset+int(shapeIDLen) > len(data) {
		return errors.New("buffer too small for ShapeID content")
	}
	r.ShapeID = Key(data[offset : offset+int(shapeIDLen)])
	offset += int(shapeIDLen)

	// The rest of the data belongs to Stops
	if offset > len(data) {
		return errors.New("offset beyond data length before decoding Stops")
	}
	stopsData := data[offset:]
	err := r.Stops.Decode(stopsData)
	if err != nil {
		return fmt.Errorf("failed to decode Stops: %w", err)
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
