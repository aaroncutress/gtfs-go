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
func (g *GTFS) GetTripsByRouteID(routeID Key) (TripMap, error) {
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

	trips := make(TripMap, len(*tripIDs))

	// Query the database for each trip ID and load the trip data
	err = g.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("trips"))
		if b == nil {
			return errors.New("bucket not found")
		}
		for _, tripID := range *tripIDs {
			data := b.Get([]byte(tripID))
			if data == nil {
				return errors.New("trip not found")
			}
			trip := &Trip{}
			err := trip.Decode(tripID, data)
			if err != nil {
				return err
			}
			trips[tripID] = trip
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return trips, nil
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

// --- Bulk Query Functions ---

// Returns the agencies with the given IDs
func (g *GTFS) GetAgenciesByIDs(agencyIDs []Key) (AgencyMap, error) {
	agencies := make(AgencyMap, len(agencyIDs))

	// Query the database for each agency ID and load the agency data
	err := g.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("agencies"))
		if b == nil {
			return errors.New("bucket not found")
		}
		for _, agencyID := range agencyIDs {
			data := b.Get([]byte(agencyID))
			if data == nil {
				continue
			}
			agency := &Agency{}
			err := agency.Decode(agencyID, data)
			if err != nil {
				return err
			}
			agencies[agencyID] = agency
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return agencies, nil
}

// Returns all agencies in the GTFS database
func (g *GTFS) GetAllAgencies() (AgencyMap, error) {
	var agencies AgencyMap

	err := g.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("agencies"))
		if b == nil {
			return errors.New("bucket not found")
		}

		agencies = make(AgencyMap, b.Stats().KeyN)

		return b.ForEach(func(k, v []byte) error {
			agency := &Agency{}
			key := Key(k)
			err := agency.Decode(key, v)
			if err != nil {
				return err
			}
			agencies[key] = agency
			return nil
		})
	})

	if err != nil {
		return nil, err
	}
	return agencies, nil
}

// Returns the routes with the given IDs
func (g *GTFS) GetRoutesByIDs(routeIDs []Key) (RouteMap, error) {
	routes := make(RouteMap, len(routeIDs))

	// Query the database for each route ID and load the route data
	err := g.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("routes"))
		if b == nil {
			return errors.New("bucket not found")
		}
		for _, routeID := range routeIDs {
			data := b.Get([]byte(routeID))
			if data == nil {
				continue
			}
			route := &Route{}
			err := route.Decode(routeID, data)
			if err != nil {
				return err
			}
			routes[routeID] = route
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return routes, nil
}

// Returns all routes in the GTFS database
func (g *GTFS) GetAllRoutes() (RouteMap, error) {
	var routes RouteMap

	err := g.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("routes"))
		if b == nil {
			return errors.New("bucket not found")
		}

		routes = make(RouteMap, b.Stats().KeyN)

		return b.ForEach(func(k, v []byte) error {
			route := &Route{}
			key := Key(k)
			err := route.Decode(key, v)
			if err != nil {
				return err
			}
			routes[key] = route
			return nil
		})
	})

	if err != nil {
		return nil, err
	}
	return routes, nil
}

// Returns the stops with the given IDs
func (g *GTFS) GetStopsByIDs(stopIDs []Key) (StopMap, error) {
	stops := make(StopMap, len(stopIDs))

	// Query the database for each stop ID and load the stop data
	err := g.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("stops"))
		if b == nil {
			return errors.New("bucket not found")
		}
		for _, stopID := range stopIDs {
			data := b.Get([]byte(stopID))
			if data == nil {
				continue
			}
			stop := &Stop{}
			err := stop.Decode(stopID, data)
			if err != nil {
				return err
			}
			stops[stopID] = stop
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return stops, nil
}

// Returns all stops in the GTFS database
func (g *GTFS) GetAllStops() (StopMap, error) {
	var stops StopMap

	err := g.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("stops"))
		if b == nil {
			return errors.New("bucket not found")
		}

		stops = make(StopMap, b.Stats().KeyN)

		return b.ForEach(func(k, v []byte) error {
			stop := &Stop{}
			key := Key(k)
			err := stop.Decode(key, v)
			if err != nil {
				return err
			}
			stops[key] = stop
			return nil
		})
	})

	if err != nil {
		return nil, err
	}
	return stops, nil
}

// Returns the shapes with the given IDs
func (g *GTFS) GetShapesByIDs(shapeIDs []Key) (ShapeMap, error) {
	shapes := make(ShapeMap, len(shapeIDs))

	// Query the database for each shape ID and load the shape data
	err := g.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("shapes"))
		if b == nil {
			return errors.New("bucket not found")
		}
		for _, shapeID := range shapeIDs {
			data := b.Get([]byte(shapeID))
			if data == nil {
				continue
			}
			shape := &Shape{}
			err := shape.Decode(shapeID, data)
			if err != nil {
				return err
			}
			shapes[shapeID] = shape
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return shapes, nil
}

// Returns all shapes in the GTFS database
func (g *GTFS) GetAllShapes() (ShapeMap, error) {
	var shapes ShapeMap

	err := g.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("shapes"))
		if b == nil {
			return errors.New("bucket not found")
		}

		shapes = make(ShapeMap, b.Stats().KeyN)

		return b.ForEach(func(k, v []byte) error {
			shape := &Shape{}
			key := Key(k)
			err := shape.Decode(key, v)
			if err != nil {
				return err
			}
			shapes[key] = shape
			return nil
		})
	})

	if err != nil {
		return nil, err
	}
	return shapes, nil
}

// Returns the trips with the given IDs
func (g *GTFS) GetTripsByIDs(tripIDs []Key) (TripMap, error) {
	trips := make(TripMap, len(tripIDs))

	// Query the database for each trip ID and load the trip data
	err := g.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("trips"))
		if b == nil {
			return errors.New("bucket not found")
		}
		for _, tripID := range tripIDs {
			data := b.Get([]byte(tripID))
			if data == nil {
				continue
			}
			trip := &Trip{}
			err := trip.Decode(tripID, data)
			if err != nil {
				return err
			}
			trips[tripID] = trip
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return trips, nil
}

// Returns all trips in the GTFS database
func (g *GTFS) GetAllTrips() (TripMap, error) {
	var trips TripMap

	err := g.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("trips"))
		if b == nil {
			return errors.New("bucket not found")
		}

		trips = make(TripMap, b.Stats().KeyN)

		return b.ForEach(func(k, v []byte) error {
			trip := &Trip{}
			key := Key(k)
			err := trip.Decode(key, v)
			if err != nil {
				return err
			}
			trips[key] = trip
			return nil
		})
	})

	if err != nil {
		return nil, err
	}
	return trips, nil
}

// Returns the services with the given IDs
func (g *GTFS) GetServicesByIDs(serviceIDs []Key) (ServiceMap, error) {
	services := make(ServiceMap, len(serviceIDs))

	// Query the database for each service ID and load the service data
	err := g.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("services"))
		if b == nil {
			return errors.New("bucket not found")
		}
		for _, serviceID := range serviceIDs {
			data := b.Get([]byte(serviceID))
			if data == nil {
				continue
			}
			service := &Service{}
			err := service.Decode(serviceID, data)
			if err != nil {
				return err
			}
			services[serviceID] = service
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return services, nil
}

// Returns all services in the GTFS database
func (g *GTFS) GetAllServices() (ServiceMap, error) {
	var services ServiceMap

	err := g.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("services"))
		if b == nil {
			return errors.New("bucket not found")
		}

		services = make(ServiceMap, b.Stats().KeyN)

		return b.ForEach(func(k, v []byte) error {
			service := &Service{}
			key := Key(k)
			err := service.Decode(key, v)
			if err != nil {
				return err
			}
			services[key] = service
			return nil
		})
	})

	if err != nil {
		return nil, err
	}
	return services, nil
}

// Returns all service exceptions in the GTFS database
func (g *GTFS) GetAllServiceExceptions() (ServiceExceptionMap, error) {
	var exceptions ServiceExceptionMap

	err := g.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("serviceExceptions"))
		if b == nil {
			return errors.New("bucket not found")
		}

		exceptions = make(ServiceExceptionMap, b.Stats().KeyN)

		return b.ForEach(func(k, v []byte) error {
			exception := &ServiceException{}
			err := exception.Decode(v)
			if err != nil {
				return err
			}
			key := ServiceExceptionKey{
				ServiceID: exception.ServiceID,
				Date:      exception.Date,
			}
			exceptions[key] = exception
			return nil
		})
	})

	if err != nil {
		return nil, err
	}
	return exceptions, nil
}
