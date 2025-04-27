package gtfs

import (
	"errors"
	"time"

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
	Created time.Time

	filePath string
	db       *gtfsdb
}

// Save the GTFS database to the file
func (g *GTFS) Save() error {
	return g.db.save(g.filePath, g.Version)
}

// --- Individual Query Functions ---

// Returns the agency with the given ID
func (g *GTFS) GetAgencyByID(agencyID Key) (*Agency, error) {
	agency := &Agency{}

	// Query the database for the agency with the given ID
	err := g.db.agencies.QueryKey(string(agencyID), agency.Load)

	if err != nil {
		return nil, err
	}
	return agency, nil
}

// Returns the route with the given ID
func (g *GTFS) GetRouteByID(routeID Key) (*Route, error) {
	route := &Route{}

	// Query the database for the route with the given ID
	err := g.db.routes.QueryKey(string(routeID), route.Load)

	if err != nil {
		return nil, err
	}
	return route, nil
}

// Returns the route with the given name
func (g *GTFS) GetRouteByName(routeName string) (*Route, error) {
	var routeID Key

	// Query the database for the route with the given name
	err := g.db.routesByNameIndex.QueryKey(routeName, func(row column.Row) error {
		routeIDsAny, ok := row.Record("ids")
		if !ok {
			return errors.New("missing route ID")
		}

		routeIDs, ok := routeIDsAny.(*KeyArray)
		if !ok {
			return errors.New("invalid route ID format")
		}

		if len(*routeIDs) == 0 {
			return errors.New("no route IDs found")
		}
		routeID = (*routeIDs)[0]
		return nil
	})
	if err != nil {
		return nil, err
	}

	return g.GetRouteByID(routeID)
}

// Returns the stop with the given ID
func (g *GTFS) GetStopByID(stopID Key) (*Stop, error) {
	stop := &Stop{}

	// Query the database for the stop with the given ID
	err := g.db.stops.QueryKey(string(stopID), stop.Load)

	if err != nil {
		return nil, err
	}
	return stop, nil
}

// Returns the stop with the given name
func (g *GTFS) GetStopByName(stopName string) (*Stop, error) {
	var stopID Key

	// Query the database for the stop with the given name
	err := g.db.stopsByNameIndex.QueryKey(stopName, func(row column.Row) error {
		stopIDsAny, ok := row.Record("ids")
		if !ok {
			return errors.New("missing stop ID")
		}
		stopIDs, ok := stopIDsAny.(*KeyArray)
		if !ok {
			return errors.New("invalid stop ID format")
		}
		if len(*stopIDs) == 0 {
			return errors.New("no stop IDs found")
		}
		stopID = (*stopIDs)[0]
		return nil
	})
	if err != nil {
		return nil, err
	}

	return g.GetStopByID(stopID)
}

// Returns the trip with the given ID
func (g *GTFS) GetTripByID(tripID Key) (*Trip, error) {
	trip := &Trip{}

	// Query the database for the trip with the given ID
	err := g.db.trips.QueryKey(string(tripID), trip.Load)

	if err != nil {
		return nil, err
	}
	return trip, nil
}

// Returns all trips for a given route ID
func (g *GTFS) GetTripsByRouteID(routeID Key) (TripArray, error) {
	var tripIDs *KeyArray
	trips := TripArray{}

	// Query the database for all trips associated with the route ID
	err := g.db.tripsByRouteIndex.QueryKey(string(routeID), func(row column.Row) error {
		tripIDsAny, ok := row.Record("ids")
		if !ok {
			return errors.New("missing trip ID")
		}
		tripIDs, ok = tripIDsAny.(*KeyArray)
		if !ok {
			return errors.New("invalid trip ID format")
		}
		if len(*tripIDs) == 0 {
			return errors.New("no trip IDs found")
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	// Query the database for each trip ID and load the trip data
	err = g.db.trips.Query(func(txn *column.Txn) error {
		for _, tripID := range *tripIDs {
			trip := &Trip{}
			err := txn.QueryKey(string(tripID), trip.Load)
			if err != nil {
				return err
			}
			trips = append(trips, trip)
		}
		return nil
	})

	return trips, nil
}

// Returns the service with the given ID
func (g *GTFS) GetServiceByID(serviceID Key) (*Service, error) {
	service := &Service{}

	// Query the database for the service with the given ID
	err := g.db.services.QueryKey(string(serviceID), service.Load)

	if err != nil {
		return nil, err
	}
	return service, nil
}

// Returns all services exceptions for a given service ID and date
func (g *GTFS) GetServiceException(serviceID Key, date time.Time) (*ServiceException, error) {
	exception := &ServiceException{}

	// Query the database for the service exception with the given service ID and date
	key := string(serviceID) + date.Format("20060102")
	err := g.db.serviceExceptions.QueryKey(key, exception.Load)

	if err != nil {
		return nil, err
	}
	return exception, nil
}

// --- Bulk Query Functions ---

// Returns all agencies in the GTFS database
func (g *GTFS) GetAllAgencies() (AgencyArray, error) {
	agencies := AgencyArray{}
	err := g.db.agencies.Query(agencies.Load)

	if err != nil {
		return nil, err
	}
	return agencies, nil
}

// Returns all routes in the GTFS database
func (g *GTFS) GetAllRoutes() (RouteArray, error) {
	routes := RouteArray{}
	err := g.db.routes.Query(routes.Load)

	if err != nil {
		return nil, err
	}
	return routes, nil
}

// Returns all stops in the GTFS database
func (g *GTFS) GetAllStops() (StopArray, error) {
	stops := StopArray{}
	err := g.db.stops.Query(stops.Load)

	if err != nil {
		return nil, err
	}
	return stops, nil
}

// Returns all trips in the GTFS database
func (g *GTFS) GetAllTrips() (TripArray, error) {
	trips := TripArray{}
	err := g.db.trips.Query(trips.Load)

	if err != nil {
		return nil, err
	}
	return trips, nil
}

// Returns all services in the GTFS database
func (g *GTFS) GetAllServices() (ServiceArray, error) {
	services := ServiceArray{}
	err := g.db.services.Query(services.Load)

	if err != nil {
		return nil, err
	}
	return services, nil
}

// Returns all service exceptions in the GTFS database
func (g *GTFS) GetAllServiceExceptions() (ServiceExceptionArray, error) {
	exceptions := ServiceExceptionArray{}
	err := g.db.serviceExceptions.Query(exceptions.Load)

	if err != nil {
		return nil, err
	}
	return exceptions, nil
}
