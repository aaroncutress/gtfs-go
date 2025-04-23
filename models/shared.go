package models

import (
	"fmt"

	"github.com/umahmood/haversine"
)

type Key string

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
