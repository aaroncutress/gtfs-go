package gtfs

import (
	"errors"
	"time"

	"github.com/aaroncutress/gtfs-go/internal"
	"github.com/aaroncutress/gtfs-go/models"
	"github.com/kelindar/column"
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
	Version int

	filePath string
	db       *internal.GTFSDB
}

// Save the GTFS database to the file
func (g *GTFS) Save() error {
	return g.db.Save(g.filePath, g.Version)
}

// Returns the agency with the given ID
func (g *GTFS) GetAgencyByID(agencyID models.Key) (*models.Agency, error) {
	agency := &models.Agency{}

	// Query the database for the agency with the given ID
	err := g.db.Agencies.QueryKey(string(agencyID), agency.Load)

	if err != nil {
		return nil, err
	}
	return agency, nil
}

// Returns the agency for a given route ID
func (g *GTFS) GetAgencyByRouteID(routeID models.Key) (*models.Agency, error) {
	var agencyID string

	// Query the database for the agency ID associated with the route
	err := g.db.Routes.QueryKey(string(routeID), func(row column.Row) error {
		var ok bool
		agencyID, ok = row.String("agency_id")
		if !ok {
			return errors.New("missing agency_id")
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	// Query the database for the agency with the given ID
	return g.GetAgencyByID(models.Key(agencyID))
}

// Returns the route with the given ID
func (g *GTFS) GetRouteByID(routeID models.Key) (*models.Route, error) {
	route := &models.Route{}

	// Query the database for the route with the given ID
	err := g.db.Routes.QueryKey(string(routeID), route.Load)

	if err != nil {
		return nil, err
	}
	return route, nil
}

// Returns the agency with the given ID
func (g *GTFS) GetStopByID(stopID models.Key) (*models.Stop, error) {
	stop := &models.Stop{}

	// Query the database for the stop with the given ID
	err := g.db.Stops.QueryKey(string(stopID), stop.Load)

	if err != nil {
		return nil, err
	}
	return stop, nil
}

func (g *GTFS) GetTripByID(tripID models.Key) (*models.Trip, error) {
	trip := &models.Trip{}

	// Query the database for the trip with the given ID
	err := g.db.Trips.QueryKey(string(tripID), trip.Load)

	if err != nil {
		return nil, err
	}
	return trip, nil
}

// Returns all trips for a given route ID
func (g *GTFS) GetTripsByRouteID(routeID models.Key) (models.TripArray, error) {
	trips := models.TripArray{}

	// Query the database for all trips associated with the route ID
	err := g.db.Trips.Query(func(txn *column.Txn) error {
		txnFilter := txn.WithValue("route_id", func(v any) bool {
			return v == string(routeID)
		})
		return trips.Load(txnFilter)
	})

	if err != nil {
		return nil, err
	}
	return trips, nil
}

// Returns the service with the given ID
func (g *GTFS) GetServiceByID(serviceID models.Key) (*models.Service, error) {
	service := &models.Service{}

	// Query the database for the service with the given ID
	err := g.db.Services.QueryKey(string(serviceID), service.Load)

	if err != nil {
		return nil, err
	}
	return service, nil
}

// Returns all services exceptions for a given service ID and date
func (g *GTFS) GetServiceException(serviceID models.Key, date time.Time) (*models.ServiceException, error) {
	exception := &models.ServiceException{}

	// Query the database for the service exception with the given service ID and date
	key := string(serviceID) + date.Format("20060102")
	err := g.db.ServiceExceptions.QueryKey(key, exception.Load)

	if err != nil {
		return nil, err
	}
	return exception, nil
}

// Returns all agencies in the GTFS database
func (g *GTFS) GetAllAgencies() (models.AgencyArray, error) {
	agencies := models.AgencyArray{}
	err := g.db.Agencies.Query(agencies.Load)

	if err != nil {
		return nil, err
	}
	return agencies, nil
}

// Returns all routes in the GTFS database
func (g *GTFS) GetAllRoutes() (models.RouteArray, error) {
	routes := models.RouteArray{}
	err := g.db.Routes.Query(routes.Load)

	if err != nil {
		return nil, err
	}
	return routes, nil
}
