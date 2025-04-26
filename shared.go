package gtfs

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/umahmood/haversine"
)

type Key string
type KeyArray []Key

func (ka *KeyArray) Append(key Key) {
	*ka = append(*ka, key)
}

func (ka KeyArray) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer
	// Write the number of keys
	if err := binary.Write(&buf, binary.LittleEndian, uint64(len(ka))); err != nil {
		return nil, err
	}
	for _, key := range ka {
		keyBytes := []byte(key)
		// Write the length of the key
		if err := binary.Write(&buf, binary.LittleEndian, uint64(len(keyBytes))); err != nil {
			return nil, err
		}
		// Write the key bytes
		if _, err := buf.Write(keyBytes); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func (ka *KeyArray) UnmarshalBinary(data []byte) error {
	buf := bytes.NewReader(data)
	var arrayLen uint64
	// Read the number of keys
	if err := binary.Read(buf, binary.LittleEndian, &arrayLen); err != nil {
		return err
	}
	keys := make([]Key, 0, arrayLen)
	for i := uint64(0); i < arrayLen; i++ {
		var keyLen uint64
		// Read the length of the key
		if err := binary.Read(buf, binary.LittleEndian, &keyLen); err != nil {
			return err
		}
		keyBytes := make([]byte, keyLen)
		// Read the key bytes
		if _, err := buf.Read(keyBytes); err != nil {
			return err
		}
		keys = append(keys, Key(keyBytes))
	}
	*ka = keys
	return nil
}

// --- Coordinate ---

// Represents a geographical coordinate with latitude and longitude.
type Coordinate struct {
	Latitude  float64
	Longitude float64
}

// Create a new Coordinate instance with the given latitude and longitude.
func NewCoordinate(lat, lon float64) Coordinate {
	return Coordinate{
		Latitude:  lat,
		Longitude: lon,
	}
}

// Create a new Coordinate instance from a string in the format "lat,lon".
func NewCoordinateFromString(coord string) (Coordinate, error) {
	var lat, lon float64
	_, err := fmt.Sscanf(coord, "%f,%f", &lat, &lon)
	if err != nil {
		return Coordinate{}, err
	}
	return NewCoordinate(lat, lon), nil
}

// Return a string representation of the coordinate in the format "lat,lon".
func (c Coordinate) String() string {
	return fmt.Sprintf("%f,%f", c.Latitude, c.Longitude)
}

// Check if the coordinate is zero (0, 0).
func (c Coordinate) IsZero() bool {
	return c.Latitude == 0 && c.Longitude == 0
}

// Check if the coordinate is valid (latitude between -90 and 90, longitude between -180 and 180).
func (c Coordinate) IsValid() bool {
	return c.Latitude >= -90 && c.Latitude <= 90 && c.Longitude >= -180 && c.Longitude <= 180
}

// Calculate the distance to another coordinate using the Haversine formula.
func (c Coordinate) DistanceTo(other Coordinate) float64 {
	_, km := haversine.Distance(
		haversine.Coord{Lat: c.Latitude, Lon: c.Longitude},
		haversine.Coord{Lat: other.Latitude, Lon: other.Longitude},
	)
	return km
}

type CoordinateArray []Coordinate

func (ca CoordinateArray) MarshalBinary() ([]byte, error) {
	buf := new(bytes.Buffer)
	count := uint32(len(ca))

	if err := binary.Write(buf, binary.LittleEndian, count); err != nil {
		return nil, err
	}

	for _, coord := range ca {
		if err := binary.Write(buf, binary.LittleEndian, coord.Latitude); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, coord.Longitude); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

func (ca *CoordinateArray) UnmarshalBinary(data []byte) error {
	reader := bytes.NewReader(data)

	var count uint32
	if err := binary.Read(reader, binary.LittleEndian, &count); err != nil {
		return err
	}

	*ca = make(CoordinateArray, count)
	for i := uint32(0); i < count; i++ {
		if err := binary.Read(reader, binary.LittleEndian, &(*ca)[i].Latitude); err != nil {
			return err
		}
		if err := binary.Read(reader, binary.LittleEndian, &(*ca)[i].Longitude); err != nil {
			return err
		}
	}

	if reader.Len() > 0 {
		return errors.New("extra data after unmarshalling")
	}

	return nil
}
