package gtfs

import (
	"errors"
	"time"

	bolt "go.etcd.io/bbolt"
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
	Created int64

	filePath string
	db       *bolt.DB
}

// Closes the GTFS database connection and saves metadata
func (g *GTFS) Close() error {
	if g.db == nil {
		return nil
	}

	return g.db.Close()
}

// --- Individual Query Functions ---

// Returns the agency with the given ID
func (g *GTFS) GetAgencyByID(agencyID Key) (*Agency, error) {
	agency := &Agency{}

	// Query the database for the agency with the given ID
	err := g.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("agencies"))
		if b == nil {
			return errors.New("bucket not found")
		}
		data := b.Get([]byte(agencyID))
		if data == nil {
			return errors.New("agency not found")
		}
		return agency.Decode(agencyID, data)
	})

	if err != nil {
		return nil, err
	}
	return agency, nil
}

// Returns the route with the given ID
func (g *GTFS) GetRouteByID(routeID Key) (*Route, error) {
	route := &Route{}

	// Query the database for the route with the given ID
	err := g.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("routes"))
		if b == nil {
			return errors.New("bucket not found")
		}
		data := b.Get([]byte(routeID))
		if data == nil {
			return errors.New("route not found")
		}
		return route.Decode(routeID, data)
	})

	if err != nil {
		return nil, err
	}
	return route, nil
}

// Returns the route with the given name
func (g *GTFS) GetRouteByName(routeName string) (*Route, error) {
	var routeID Key

	// Query the database for the route with the given name
	err := g.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("routesByNameIndex"))
		if b == nil {
			return errors.New("bucket not found")
		}
		data := b.Get([]byte(routeName))
		if data == nil {
			return errors.New("route not found")
		}
		routeID = Key(data)
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
	err := g.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("stops"))
		if b == nil {
			return errors.New("bucket not found")
		}
		data := b.Get([]byte(stopID))
		if data == nil {
			return errors.New("stop not found")
		}
		return stop.Decode(stopID, data)
	})

	if err != nil {
		return nil, err
	}
	return stop, nil
}

// Returns the stop with the given name
func (g *GTFS) GetStopByName(stopName string) (*Stop, error) {
	var stopID Key

	// Query the database for the stop with the given name
	err := g.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("stopsByNameIndex"))
		if b == nil {
			return errors.New("bucket not found")
		}
		data := b.Get([]byte(stopName))
		if data == nil {
			return errors.New("stop not found")
		}
		stopID = Key(data)
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
	err := g.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("trips"))
		if b == nil {
			return errors.New("bucket not found")
		}
		data := b.Get([]byte(tripID))
		if data == nil {
			return errors.New("trip not found")
		}
		return trip.Decode(tripID, data)
	})

	if err != nil {
		return nil, err
	}
	return trip, nil
}

// Returns all trips for a given route ID
func (g *GTFS) GetTripsByRouteID(routeID Key) (TripArray, error) {
	var tripIDs *KeyArray

	// Query the database for all trips associated with the route ID
	err := g.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("tripsByRouteIndex"))
		if b == nil {
			return errors.New("bucket not found")
		}
		data := b.Get([]byte(routeID))
		if data == nil {
			return errors.New("no trips found for route")
		}
		tripIDs = &KeyArray{}
		err := tripIDs.Decode(data)
		if err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	trips := make(TripArray, len(*tripIDs))

	// Query the database for each trip ID and load the trip data
	err = g.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("trips"))
		if b == nil {
			return errors.New("bucket not found")
		}
		for i, tripID := range *tripIDs {
			data := b.Get([]byte(tripID))
			if data == nil {
				return errors.New("trip not found")
			}
			trip := &Trip{}
			err := trip.Decode(tripID, data)
			if err != nil {
				return err
			}
			trips[i] = trip
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return trips, nil
}

// Returns the service with the given ID
func (g *GTFS) GetServiceByID(serviceID Key) (*Service, error) {
	service := &Service{}

	// Query the database for the service with the given ID
	err := g.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("services"))
		if b == nil {
			return errors.New("bucket not found")
		}
		data := b.Get([]byte(serviceID))
		if data == nil {
			return errors.New("service not found")
		}
		return service.Decode(serviceID, data)
	})

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
	err := g.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("serviceExceptions"))
		if b == nil {
			return errors.New("bucket not found")
		}
		data := b.Get([]byte(key))
		if data == nil {
			return errors.New("service exception not found")
		}
		return exception.Decode(data)
	})

	if err != nil {
		return nil, err
	}
	return exception, nil
}

// Returns the shape with the given ID
func (g *GTFS) GetShapeByID(shapeID Key) (*Shape, error) {
	shape := &Shape{}

	// Query the database for the shape with the given ID
	err := g.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("shapes"))
		if b == nil {
			return errors.New("bucket not found")
		}
		data := b.Get([]byte(shapeID))
		if data == nil {
			return errors.New("shape not found")
		}
		return shape.Decode(shapeID, data)
	})

	if err != nil {
		return nil, err
	}
	return shape, nil
}

// --- Bulk Query Functions ---

// Returns all agencies in the GTFS database
func (g *GTFS) GetAllAgencies() (AgencyArray, error) {
	var agencies AgencyArray

	err := g.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("agencies"))
		if b == nil {
			return errors.New("bucket not found")
		}

		agencies = make(AgencyArray, 0, b.Stats().KeyN)

		return b.ForEach(func(k, v []byte) error {
			agency := &Agency{}
			err := agency.Decode(Key(k), v)
			if err != nil {
				return err
			}
			agencies = append(agencies, agency)
			return nil
		})
	})

	if err != nil {
		return nil, err
	}
	return agencies, nil
}

// Returns all routes in the GTFS database
func (g *GTFS) GetAllRoutes() (RouteArray, error) {
	var routes RouteArray

	err := g.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("routes"))
		if b == nil {
			return errors.New("bucket not found")
		}

		routes = make(RouteArray, 0, b.Stats().KeyN)

		return b.ForEach(func(k, v []byte) error {
			route := &Route{}
			err := route.Decode(Key(k), v)
			if err != nil {
				return err
			}
			routes = append(routes, route)
			return nil
		})
	})

	if err != nil {
		return nil, err
	}
	return routes, nil
}

// Returns all stops in the GTFS database
func (g *GTFS) GetAllStops() (StopArray, error) {
	var stops StopArray

	err := g.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("stops"))
		if b == nil {
			return errors.New("bucket not found")
		}

		stops = make(StopArray, 0, b.Stats().KeyN)

		return b.ForEach(func(k, v []byte) error {
			stop := &Stop{}
			err := stop.Decode(Key(k), v)
			if err != nil {
				return err
			}
			stops = append(stops, stop)
			return nil
		})
	})

	if err != nil {
		return nil, err
	}
	return stops, nil
}

// Returns all trips in the GTFS database
func (g *GTFS) GetAllTrips() (TripArray, error) {
	var trips TripArray

	err := g.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("trips"))
		if b == nil {
			return errors.New("bucket not found")
		}

		trips = make(TripArray, 0, b.Stats().KeyN)

		return b.ForEach(func(k, v []byte) error {
			trip := &Trip{}
			err := trip.Decode(Key(k), v)
			if err != nil {
				return err
			}
			trips = append(trips, trip)
			return nil
		})
	})

	if err != nil {
		return nil, err
	}
	return trips, nil
}

// Returns all services in the GTFS database
func (g *GTFS) GetAllServices() (ServiceArray, error) {
	var services ServiceArray

	err := g.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("services"))
		if b == nil {
			return errors.New("bucket not found")
		}

		services = make(ServiceArray, 0, b.Stats().KeyN)

		return b.ForEach(func(k, v []byte) error {
			service := &Service{}
			err := service.Decode(Key(k), v)
			if err != nil {
				return err
			}
			services = append(services, service)
			return nil
		})
	})

	if err != nil {
		return nil, err
	}
	return services, nil
}

// Returns all service exceptions in the GTFS database
func (g *GTFS) GetAllServiceExceptions() (ServiceExceptionArray, error) {
	var exceptions ServiceExceptionArray

	err := g.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("serviceExceptions"))
		if b == nil {
			return errors.New("bucket not found")
		}

		exceptions = make(ServiceExceptionArray, 0, b.Stats().KeyN)

		return b.ForEach(func(k, v []byte) error {
			exception := &ServiceException{}
			err := exception.Decode(v)
			if err != nil {
				return err
			}
			exceptions = append(exceptions, exception)
			return nil
		})
	})

	if err != nil {
		return nil, err
	}
	return exceptions, nil
}
