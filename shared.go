package gtfs

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"github.com/umahmood/haversine"
)

type Key string
type KeyArray []Key

func (ka *KeyArray) Append(key Key) {
	*ka = append(*ka, key)
}

// Encodes the KeyArray into a byte slice
// Format:
// - Count: 4 bytes (number of keys)
// - Each key: 4 bytes (length of the key) + UTF-8 string
func (ka KeyArray) Encode() []byte {
	// Calculate total length correctly
	totalLen := lenBytes // For the count of keys
	for _, k := range ka {
		totalLen += lenBytes + len(string(k)) // len(string(k)) for the key content
	}

	data := make([]byte, totalLen)
	offset := 0

	// Marshal count
	binary.BigEndian.PutUint32(data[offset:], uint32(len(ka)))
	offset += lenBytes

	// Marshal keys
	for _, k := range ka {
		keyStr := string(k)
		binary.BigEndian.PutUint32(data[offset:], uint32(len(keyStr)))
		offset += lenBytes
		copy(data[offset:], keyStr)
		offset += len(keyStr)
	}
	return data
}

// Decodes the byte slice into the KeyArray
func (ka *KeyArray) Decode(data []byte) error {
	if ka == nil {
		return errors.New("cannot decode into a nil KeyArray")
	}
	offset := 0

	// Unmarshal count
	if offset+lenBytes > len(data) {
		return errors.New("keyarray buffer too small for count")
	}
	count := binary.BigEndian.Uint32(data[offset:])
	offset += lenBytes

	// Unmarshal keys
	// Use a temporary slice to build, then assign to *ka to handle if *ka was non-nil
	tempKa := make(KeyArray, count)
	for i := uint32(0); i < count; i++ {
		if offset+lenBytes > len(data) {
			return fmt.Errorf("keyarray buffer too small for key %d length", i)
		}
		keyLen := binary.BigEndian.Uint32(data[offset:])
		offset += lenBytes

		if offset+int(keyLen) > len(data) {
			return fmt.Errorf("keyarray buffer too small for key %d content", i)
		}
		tempKa[i] = Key(data[offset : offset+int(keyLen)])
		offset += int(keyLen)
	}
	*ka = tempKa // Assign the newly decoded slice

	// Check if all data was consumed
	if offset != len(data) {
		return errors.New("keyarray buffer not fully consumed, trailing data exists")
	}
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

// Encode the Coordinate into a byte slice
// Format:
// - Latitude: 8 bytes (float64)
// - Longitude: 8 bytes (float64)
func (c Coordinate) Encode() []byte {
	data := make([]byte, float64Bytes*2) // 8 bytes for lat + 8 bytes for lon
	offset := 0

	binary.BigEndian.PutUint64(data[offset:], math.Float64bits(c.Latitude))
	offset += float64Bytes
	binary.BigEndian.PutUint64(data[offset:], math.Float64bits(c.Longitude))
	return data
}

// Decode the byte slice into a Coordinate
func (c *Coordinate) Decode(data []byte) error {
	if c == nil {
		return errors.New("cannot decode into a nil Coordinate")
	}
	if len(data) < float64Bytes*2 {
		return errors.New("coordinate buffer too small")
	}
	offset := 0

	c.Latitude = math.Float64frombits(binary.BigEndian.Uint64(data[offset:]))
	offset += float64Bytes
	c.Longitude = math.Float64frombits(binary.BigEndian.Uint64(data[offset:]))
	offset += float64Bytes

	// Check if all data was consumed (optional for fixed-size struct if called with exact slice)
	if offset != len(data) {
		return errors.New("coordinate buffer not fully consumed, trailing data exists")
	}
	return nil
}

type CoordinateArray []Coordinate

// Encode the CoordinateArray into a byte slice
// Format:
// - Count: 4 bytes (number of coordinates)
// - Each coordinate: 8 bytes (float64 for latitude) + 8 bytes (float64 for longitude)
func (ca CoordinateArray) Encode() []byte {
	// Calculate total length: 4 bytes for count + (count * size_of_coordinate_encoding)
	// Size of each coordinate encoding is float64Bytes * 2
	coordSize := float64Bytes * 2
	totalLen := lenBytes + (len(ca) * coordSize)

	data := make([]byte, totalLen)
	offset := 0

	// Marshal count
	binary.BigEndian.PutUint32(data[offset:], uint32(len(ca)))
	offset += lenBytes

	// Marshal each coordinate
	for _, coord := range ca {
		coordBytes := coord.Encode() // This already creates a slice of coordSize
		copy(data[offset:], coordBytes)
		offset += coordSize
	}
	return data
}

// Decode the byte slice into the CoordinateArray
func (ca *CoordinateArray) Decode(data []byte) error {
	if ca == nil {
		return errors.New("cannot decode into a nil CoordinateArray")
	}
	offset := 0

	// Unmarshal count
	if offset+lenBytes > len(data) {
		return errors.New("coordinatearray buffer too small for count")
	}
	count := binary.BigEndian.Uint32(data[offset:])
	offset += lenBytes

	// Unmarshal coordinates
	coordSize := float64Bytes * 2
	tempCa := make(CoordinateArray, count)
	for i := uint32(0); i < count; i++ {
		if offset+coordSize > len(data) {
			return fmt.Errorf("coordinatearray buffer too small for coordinate %d", i)
		}
		var coord Coordinate
		// Pass the exact slice for the current coordinate to its Decode method
		err := coord.Decode(data[offset : offset+coordSize])
		if err != nil {
			return fmt.Errorf("failed to decode coordinate %d: %w", i, err)
		}
		tempCa[i] = coord
		offset += coordSize
	}
	*ca = tempCa

	// Check if all data was consumed
	if offset != len(data) {
		return errors.New("coordinatearray buffer not fully consumed, trailing data exists")
	}
	return nil
}
