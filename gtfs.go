package gtfs

import (
	"database/sql"

	"github.com/aaroncutress/gtfs-go/models"
	_ "modernc.org/sqlite"
)

var requiredFiles = []string{
	"agency.txt",
	"calendar.txt",
	"stops.txt",
	"routes.txt",
	"trips.txt",
	"stop_times.txt",
}

// Represents a GTFS database connection
type GTFS struct {
	db *sql.DB
}

// Close the GTFS database connection
func (g *GTFS) Close() error {
	if g.db != nil {
		return g.db.Close()
	}
	return nil
}

// Returns the route with the given ID
func (g *GTFS) GetRouteByID(routeID models.Key) (*models.Route, error) {
	record := g.db.QueryRow("SELECT * FROM routes WHERE id = ?", routeID)
	stopRecords, err := g.db.Query("SELECT * FROM routes_stops WHERE route_id = ?", routeID)
	if err != nil {
		return nil, err
	}
	defer stopRecords.Close()

	return models.DecodeRoute(record, stopRecords)
}

// Returns the route with the given name
func (g *GTFS) GetRouteByName(routeName string) (*models.Route, error) {
	record := g.db.QueryRow("SELECT * FROM routes WHERE name = ?", routeName)
	stopRecords, err := g.db.Query("SELECT * FROM routes_stops WHERE route_id = ?", routeName)
	if err != nil {
		return nil, err
	}
	defer stopRecords.Close()
	return models.DecodeRoute(record, stopRecords)
}

// Returns the agency with the given ID
func (g *GTFS) GetStopByID(stopID models.Key) (*models.Stop, error) {
	record := g.db.QueryRow("SELECT * FROM stops WHERE id = ?", stopID)
	return models.DecodeStop(record)
}

func (g *GTFS) GetTripByID(tripID models.Key) (*models.Trip, error) {
	record := g.db.QueryRow("SELECT * FROM trips WHERE id = ?", tripID)
	stopRecords, err := g.db.Query("SELECT * FROM trip_stops WHERE trip_id = ?", tripID)
	if err != nil {
		return nil, err
	}
	defer stopRecords.Close()
	return models.DecodeTrip(record, stopRecords)
}

// Returns all trips for a given route ID
func (g *GTFS) GetTripsByRouteID(routeID models.Key) ([]*models.Trip, error) {
	records, err := g.db.Query("SELECT id FROM trips WHERE route_id = ?", routeID)
	if err != nil {
		return nil, err
	}
	defer records.Close()

	var trips []*models.Trip
	for records.Next() {
		var tripID string
		err := records.Scan(&tripID)
		if err != nil {
			return nil, err
		}
		trip, err := g.GetTripByID(models.Key(tripID))
		if err != nil {
			return nil, err
		}
		trips = append(trips, trip)
	}
	return trips, nil
}

// Returns the service with the given ID
func (g *GTFS) GetServiceByID(serviceID models.Key) (*models.Service, error) {
	record := g.db.QueryRow("SELECT * FROM services WHERE id = ?", serviceID)
	return models.DecodeService(record)
}

// Returns all services exceptions for a given service ID
func (g *GTFS) GetServiceExceptionsByServiceID(serviceID models.Key) ([]*models.ServiceException, error) {
	records, err := g.db.Query("SELECT * FROM service_exceptions WHERE service_id = ?", serviceID)
	if err != nil {
		return nil, err
	}
	defer records.Close()

	return models.DecodeServiceExceptions(records)
}
